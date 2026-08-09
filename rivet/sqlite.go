package rivet

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/ewhauser/rivet-go/internal/sqlitesocket"
	"github.com/ewhauser/rivet-go/internal/wire"
)

const (
	defaultSQLiteLeaseTimeout = 60 * time.Second
	defaultSQLiteCloseTimeout = 10 * time.Second
	maxSQLiteSQLBytes         = 1 << 20
	maxSQLiteArguments        = 1_024
	maxSQLiteValueBytes       = 1 << 20
)

// SQLiteTransport selects how database-declaring actors reach core SQLite.
type SQLiteTransport string

const (
	SQLiteTransportFFI      SQLiteTransport = "ffi"
	SQLiteTransportSocket   SQLiteTransport = "socket"
	SQLiteTransportDisabled SQLiteTransport = "disabled"
)

// Result is the mutation metadata returned by Exec.
type Result struct {
	RowsAffected int64
	LastInsertID int64
}

// Rows is a fully buffered SQLite query result. Every value is nil, int64,
// float64, string, or []byte. SQLite stores a bound NaN as NULL; infinities
// remain REAL values. Text returned by the pinned engine is valid UTF-8.
type Rows struct {
	Columns []string
	Values  [][]any
}

// SQLiteError is a structured SQLite or transaction error. SQLiteCode is the
// native extended SQLite code when the failure came from a SQL statement.
type SQLiteError struct {
	Code           string
	Message        string
	SQLiteCode     int32
	StatementIndex uint32
	cause          error
}

// Unwrap preserves context cancellation and transport causes for errors.Is.
func (e *SQLiteError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *SQLiteError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// Tx is an explicit actor SQLite transaction. The lease defaults to 60
// seconds. If Begin's context has an earlier deadline, that remaining duration
// becomes the lease timeout. Expiry rolls the transaction back; transactions
// also die with their actor generation or socket connection. One Tx may be
// open per DB; another Begin returns transaction_already_open.
type Tx interface {
	Exec(context.Context, string, ...any) (Result, error)
	Query(context.Context, string, ...any) (Rows, error)
	Commit(context.Context) error
	Rollback(context.Context) error
}

type sqliteBackend interface {
	exec(context.Context, string, []wire.SQLiteValue, *string) (Result, error)
	query(context.Context, string, []wire.SQLiteValue, *string) (Rows, error)
	begin(context.Context, string, time.Duration) error
	commit(context.Context, string) error
	rollback(context.Context, string) error
	close() error
}

// DB is one actor generation's SQLite database handle. Core serializes the
// native SQLite worker; the SDK permits concurrent non-transaction calls and
// core queues them. An active transaction exclusively gates other operations.
// Sleep and stop reject new calls, roll back that transaction, wait for
// admitted calls, and close this generation's transport.
type DB struct {
	backend sqliteBackend

	lifecycleMu  sync.Mutex
	lifecycle    *sync.Cond
	closing      bool
	active       int
	beginning    bool
	transactions map[*sqliteTx]struct{}
	closeOnce    sync.Once
	closeDone    chan struct{}
	closeErr     error
}

func makeDB(backend sqliteBackend) *DB {
	database := &DB{
		backend:      backend,
		transactions: make(map[*sqliteTx]struct{}),
		closeDone:    make(chan struct{}),
	}
	database.lifecycle = sync.NewCond(&database.lifecycleMu)
	return database
}

func newDB(session *pump.ActorSession, enabled bool) (*DB, error) {
	if session == nil || !enabled {
		return makeDB(disabledSQLiteBackend{}), nil
	}
	switch SQLiteTransport(session.SQLiteTransport()) {
	case SQLiteTransportFFI:
		return makeDB(ffiSQLiteBackend{session: session}), nil
	case SQLiteTransportSocket:
		if session.SQLiteSocketPath() == "" {
			return nil, &SQLiteError{Code: "sqlite_socket_unavailable", Message: "ActorStart did not include a socket endpoint"}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		client, err := sqlitesocket.Dial(ctx, session.SQLiteSocketPath())
		if err != nil {
			return nil, &SQLiteError{Code: "sqlite_socket_connect_failed", Message: err.Error()}
		}
		return makeDB(&socketSQLiteBackend{client: client}), nil
	default:
		return makeDB(disabledSQLiteBackend{}), nil
	}
}

func (d *DB) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	if d == nil || d.backend == nil {
		return Result{}, sqliteUnavailableError()
	}
	if err := validateSQLiteContext(ctx); err != nil {
		return Result{}, err
	}
	if err := validateSQLiteSQL(sql); err != nil {
		return Result{}, err
	}
	values, err := encodeSQLiteArgs(args)
	if err != nil {
		return Result{}, err
	}
	if err := d.beginOperation(); err != nil {
		return Result{}, err
	}
	defer d.finishOperation()
	return d.backend.exec(ctx, sql, values, nil)
}

func (d *DB) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if d == nil || d.backend == nil {
		return Rows{}, sqliteUnavailableError()
	}
	if err := validateSQLiteContext(ctx); err != nil {
		return Rows{}, err
	}
	if err := validateSQLiteSQL(sql); err != nil {
		return Rows{}, err
	}
	values, err := encodeSQLiteArgs(args)
	if err != nil {
		return Rows{}, err
	}
	if err := d.beginOperation(); err != nil {
		return Rows{}, err
	}
	defer d.finishOperation()
	return d.backend.query(ctx, sql, values, nil)
}

func (d *DB) Begin(ctx context.Context) (Tx, error) {
	if d == nil || d.backend == nil {
		return nil, sqliteUnavailableError()
	}
	if err := validateSQLiteContext(ctx); err != nil {
		return nil, err
	}
	timeout := defaultSQLiteLeaseTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	if timeout <= 0 {
		return nil, context.DeadlineExceeded
	}
	if err := d.beginOperation(); err != nil {
		return nil, err
	}
	defer d.finishOperation()
	d.lifecycleMu.Lock()
	if d.beginning || len(d.transactions) != 0 {
		d.lifecycleMu.Unlock()
		return nil, &SQLiteError{Code: "transaction_already_open", Message: "actor SQLite database already has an open transaction"}
	}
	d.beginning = true
	d.lifecycleMu.Unlock()
	defer func() {
		d.lifecycleMu.Lock()
		d.beginning = false
		d.lifecycleMu.Unlock()
	}()
	leaseKey := fmt.Sprintf("go-%016x", sqliteLeaseSequence.Add(1))
	if err := d.backend.begin(ctx, leaseKey, timeout); err != nil {
		return nil, err
	}
	transaction := &sqliteTx{owner: d, backend: d.backend, leaseKey: leaseKey}
	d.lifecycleMu.Lock()
	if d.closing {
		d.lifecycleMu.Unlock()
		rollbackCtx, cancel := context.WithTimeout(context.Background(), defaultSQLiteCloseTimeout)
		defer cancel()
		_ = d.backend.rollback(rollbackCtx, leaseKey)
		transaction.terminal.Store(true)
		return nil, sqliteClosedError()
	}
	d.transactions[transaction] = struct{}{}
	d.lifecycleMu.Unlock()
	return transaction, nil
}

func (d *DB) close() error {
	if d == nil || d.backend == nil {
		return nil
	}
	return d.closeTransport()
}

func (d *DB) closeForSleep() error {
	if d == nil || d.backend == nil {
		return nil
	}
	return d.closeTransport()
}

func (d *DB) closeForDestroy() error {
	if d == nil || d.backend == nil {
		return nil
	}
	return d.closeTransport()
}

func (d *DB) beginOperation() error {
	if d == nil || d.backend == nil {
		return sqliteUnavailableError()
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.closing {
		return sqliteClosedError()
	}
	d.active++
	return nil
}

func (d *DB) finishOperation() {
	d.lifecycleMu.Lock()
	d.active--
	if d.active == 0 {
		d.lifecycle.Broadcast()
	}
	d.lifecycleMu.Unlock()
}

func (d *DB) unregisterTransaction(transaction *sqliteTx) {
	if d == nil || transaction == nil {
		return
	}
	d.lifecycleMu.Lock()
	delete(d.transactions, transaction)
	d.lifecycleMu.Unlock()
}

func (d *DB) closeTransport() error {
	d.closeOnce.Do(func() {
		d.lifecycleMu.Lock()
		d.closing = true
		transactions := make([]*sqliteTx, 0, len(d.transactions))
		for transaction := range d.transactions {
			transactions = append(transactions, transaction)
		}
		d.lifecycleMu.Unlock()

		var closeErrors []error
		for _, transaction := range transactions {
			if !transaction.terminal.CompareAndSwap(false, true) {
				continue
			}
			rollbackCtx, cancel := context.WithTimeout(context.Background(), defaultSQLiteCloseTimeout)
			err := d.backend.rollback(rollbackCtx, transaction.leaseKey)
			cancel()
			if !expectedCloseRollbackError(err) {
				closeErrors = append(closeErrors, err)
			}
			d.unregisterTransaction(transaction)
		}

		d.lifecycleMu.Lock()
		for d.active != 0 {
			d.lifecycle.Wait()
		}
		d.lifecycleMu.Unlock()

		if err := d.backend.close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
		d.closeErr = errors.Join(closeErrors...)
		close(d.closeDone)
	})
	<-d.closeDone
	return d.closeErr
}

func expectedCloseRollbackError(err error) bool {
	if err == nil {
		return true
	}
	var structured *SQLiteError
	return errors.As(err, &structured) &&
		(structured.Code == "transaction_expired" || structured.Code == "invalid_lease_key")
}

func sqliteClosedError() error {
	return &SQLiteError{Code: "sqlite_endpoint_closed", Message: "actor SQLite handle is closed for this generation"}
}

type sqliteTx struct {
	owner    *DB
	backend  sqliteBackend
	leaseKey string
	terminal atomic.Bool
}

func (t *sqliteTx) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	if t == nil || t.backend == nil {
		return Result{}, &SQLiteError{Code: "invalid_lease_key", Message: "SQLite transaction is terminal"}
	}
	if err := validateSQLiteContext(ctx); err != nil {
		return Result{}, err
	}
	if err := validateSQLiteSQL(sql); err != nil {
		return Result{}, err
	}
	values, err := encodeSQLiteArgs(args)
	if err != nil {
		return Result{}, err
	}
	if err := t.owner.beginOperation(); err != nil {
		return Result{}, err
	}
	defer t.owner.finishOperation()
	if t.terminal.Load() {
		return Result{}, &SQLiteError{Code: "invalid_lease_key", Message: "SQLite transaction is terminal"}
	}
	return t.backend.exec(ctx, sql, values, &t.leaseKey)
}

func (t *sqliteTx) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if t == nil || t.backend == nil {
		return Rows{}, &SQLiteError{Code: "invalid_lease_key", Message: "SQLite transaction is terminal"}
	}
	if err := validateSQLiteContext(ctx); err != nil {
		return Rows{}, err
	}
	if err := validateSQLiteSQL(sql); err != nil {
		return Rows{}, err
	}
	values, err := encodeSQLiteArgs(args)
	if err != nil {
		return Rows{}, err
	}
	if err := t.owner.beginOperation(); err != nil {
		return Rows{}, err
	}
	defer t.owner.finishOperation()
	if t.terminal.Load() {
		return Rows{}, &SQLiteError{Code: "invalid_lease_key", Message: "SQLite transaction is terminal"}
	}
	return t.backend.query(ctx, sql, values, &t.leaseKey)
}

func (t *sqliteTx) Commit(ctx context.Context) error {
	if t == nil || t.backend == nil {
		return &SQLiteError{Code: "invalid_lease_key", Message: "SQLite transaction is terminal"}
	}
	if err := validateSQLiteContext(ctx); err != nil {
		return err
	}
	if err := t.owner.beginOperation(); err != nil {
		return err
	}
	defer t.owner.finishOperation()
	if !t.terminal.CompareAndSwap(false, true) {
		return &SQLiteError{Code: "invalid_lease_key", Message: "SQLite transaction is terminal"}
	}
	defer t.owner.unregisterTransaction(t)
	return t.backend.commit(ctx, t.leaseKey)
}

func (t *sqliteTx) Rollback(ctx context.Context) error {
	if t == nil || t.backend == nil {
		return &SQLiteError{Code: "invalid_lease_key", Message: "SQLite transaction is terminal"}
	}
	if err := validateSQLiteContext(ctx); err != nil {
		return err
	}
	if err := t.owner.beginOperation(); err != nil {
		return err
	}
	defer t.owner.finishOperation()
	if !t.terminal.CompareAndSwap(false, true) {
		return &SQLiteError{Code: "invalid_lease_key", Message: "SQLite transaction is terminal"}
	}
	defer t.owner.unregisterTransaction(t)
	return t.backend.rollback(ctx, t.leaseKey)
}

type ffiSQLiteBackend struct {
	session *pump.ActorSession
}

func (b ffiSQLiteBackend) exec(ctx context.Context, sql string, args []wire.SQLiteValue, leaseKey *string) (Result, error) {
	response, err := b.session.SQLiteExec(ctx, sql, args, leaseKey)
	if err != nil {
		return Result{}, publicSQLiteError(err)
	}
	return Result{RowsAffected: response.RowsAffected, LastInsertID: pointerValue(response.LastInsertID)}, nil
}

func (b ffiSQLiteBackend) query(ctx context.Context, sql string, args []wire.SQLiteValue, leaseKey *string) (Rows, error) {
	response, err := b.session.SQLiteQuery(ctx, sql, args, leaseKey)
	if err != nil {
		return Rows{}, publicSQLiteError(err)
	}
	return decodeSQLiteRows(response.Columns, response.Values)
}

func (b ffiSQLiteBackend) begin(ctx context.Context, leaseKey string, timeout time.Duration) error {
	return publicSQLiteError(b.session.SQLiteBegin(ctx, leaseKey, timeout))
}

func (b ffiSQLiteBackend) commit(ctx context.Context, leaseKey string) error {
	return publicSQLiteError(b.session.SQLiteCommit(ctx, leaseKey))
}

func (b ffiSQLiteBackend) rollback(ctx context.Context, leaseKey string) error {
	return publicSQLiteError(b.session.SQLiteRollback(ctx, leaseKey))
}

func (ffiSQLiteBackend) close() error { return nil }

type socketSQLiteBackend struct {
	client *sqlitesocket.Client
}

func (b *socketSQLiteBackend) exec(ctx context.Context, sql string, args []wire.SQLiteValue, leaseKey *string) (Result, error) {
	response, err := b.client.Exec(ctx, sql, args, leaseKey)
	if err != nil {
		return Result{}, publicSQLiteError(err)
	}
	return Result{RowsAffected: response.RowsAffected, LastInsertID: pointerValue(response.LastInsertID)}, nil
}

func (b *socketSQLiteBackend) query(ctx context.Context, sql string, args []wire.SQLiteValue, leaseKey *string) (Rows, error) {
	response, err := b.client.Query(ctx, sql, args, leaseKey)
	if err != nil {
		return Rows{}, publicSQLiteError(err)
	}
	return decodeSQLiteRows(response.Columns, response.Values)
}

func (b *socketSQLiteBackend) begin(ctx context.Context, leaseKey string, timeout time.Duration) error {
	return publicSQLiteError(b.client.Begin(ctx, leaseKey, timeout))
}

func (b *socketSQLiteBackend) commit(ctx context.Context, leaseKey string) error {
	return publicSQLiteError(b.client.Commit(ctx, leaseKey))
}

func (b *socketSQLiteBackend) rollback(ctx context.Context, leaseKey string) error {
	return publicSQLiteError(b.client.Rollback(ctx, leaseKey))
}

func (b *socketSQLiteBackend) close() error { return b.client.Close() }

type disabledSQLiteBackend struct {
	code    string
	message string
}

func (b disabledSQLiteBackend) failure() error {
	code := b.code
	if code == "" {
		code = "sqlite_transport_not_configured"
	}
	message := b.message
	if message == "" {
		message = "set Database: true on the actor and ensure rivet.Config.SQLiteTransport is not disabled"
	}
	return &SQLiteError{Code: code, Message: message}
}

func (b disabledSQLiteBackend) exec(context.Context, string, []wire.SQLiteValue, *string) (Result, error) {
	return Result{}, b.failure()
}
func (b disabledSQLiteBackend) query(context.Context, string, []wire.SQLiteValue, *string) (Rows, error) {
	return Rows{}, b.failure()
}
func (b disabledSQLiteBackend) begin(context.Context, string, time.Duration) error {
	return b.failure()
}
func (b disabledSQLiteBackend) commit(context.Context, string) error   { return b.failure() }
func (b disabledSQLiteBackend) rollback(context.Context, string) error { return b.failure() }
func (disabledSQLiteBackend) close() error                             { return nil }

var sqliteLeaseSequence atomic.Uint64

func validateSQLiteTransport(transport SQLiteTransport) error {
	switch transport {
	case SQLiteTransportFFI, SQLiteTransportSocket, SQLiteTransportDisabled:
		return nil
	default:
		return fmt.Errorf(
			"SQLiteTransport must be %q, %q, or %q",
			SQLiteTransportFFI, SQLiteTransportSocket, SQLiteTransportDisabled,
		)
	}
}

// wireSQLiteTransport maps the public disabled selection to the boundary's
// empty transport, which prevents every actor database from being provisioned.
func wireSQLiteTransport(transport SQLiteTransport) string {
	if transport == SQLiteTransportDisabled {
		return ""
	}
	return string(transport)
}

func validateSQLiteSQL(sql string) error {
	if sql == "" {
		return &SQLiteError{Code: "sqlite_sql_empty", Message: "SQL must not be empty"}
	}
	if len(sql) > maxSQLiteSQLBytes {
		return &SQLiteError{Code: "sqlite_sql_too_large", Message: "SQL exceeds the 1 MiB limit"}
	}
	if !utf8.ValidString(sql) {
		return &SQLiteError{Code: "sqlite_sql_invalid_utf8", Message: "SQL must be valid UTF-8"}
	}
	return nil
}

func validateSQLiteContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("SQLite context is nil")
	}
	return ctx.Err()
}

func encodeSQLiteArgs(args []any) ([]wire.SQLiteValue, error) {
	if len(args) > maxSQLiteArguments {
		return nil, &SQLiteError{Code: "sqlite_too_many_arguments", Message: "SQLite arguments exceed the 1024-value limit"}
	}
	values := make([]wire.SQLiteValue, len(args))
	for index, arg := range args {
		switch value := arg.(type) {
		case nil:
			values[index] = wire.SQLiteValue{Kind: "null"}
		case int64:
			v := value
			values[index] = wire.SQLiteValue{Kind: "integer", Integer: &v}
		case float64:
			bits := math.Float64bits(value)
			values[index] = wire.SQLiteValue{Kind: "real", Bits: &bits}
		case string:
			if len(value) > maxSQLiteValueBytes {
				return nil, &SQLiteError{Code: "sqlite_argument_too_large", Message: fmt.Sprintf("argument %d exceeds the 1 MiB value limit", index)}
			}
			if !utf8.ValidString(value) {
				return nil, &SQLiteError{Code: "sqlite_argument_invalid_utf8", Message: fmt.Sprintf("argument %d text must be valid UTF-8", index)}
			}
			v := value
			values[index] = wire.SQLiteValue{Kind: "text", Text: &v}
		case []byte:
			if len(value) > maxSQLiteValueBytes {
				return nil, &SQLiteError{Code: "sqlite_argument_too_large", Message: fmt.Sprintf("argument %d exceeds the 1 MiB value limit", index)}
			}
			v := append([]byte(nil), value...)
			if v == nil {
				v = []byte{}
			}
			values[index] = wire.SQLiteValue{Kind: "blob", Blob: &v}
		default:
			return nil, &SQLiteError{
				Code:    "sqlite_argument_type",
				Message: fmt.Sprintf("argument %d has unsupported Go type %T; use nil, int64, float64, string, or []byte", index, arg),
			}
		}
	}
	return values, nil
}

func decodeSQLiteRows(columns []string, values []wire.SQLiteValue) (Rows, error) {
	rows := Rows{Columns: append([]string(nil), columns...)}
	if len(columns) == 0 {
		if len(values) != 0 {
			return Rows{}, &SQLiteError{Code: "sqlite_result_invalid", Message: "result has values without columns"}
		}
		return rows, nil
	}
	if len(values)%len(columns) != 0 {
		return Rows{}, &SQLiteError{Code: "sqlite_result_invalid", Message: "result is not rectangular"}
	}
	rows.Values = make([][]any, 0, len(values)/len(columns))
	for offset := 0; offset < len(values); offset += len(columns) {
		row := make([]any, len(columns))
		for column := range columns {
			value, err := decodeSQLiteValue(values[offset+column])
			if err != nil {
				return Rows{}, err
			}
			row[column] = value
		}
		rows.Values = append(rows.Values, row)
	}
	return rows, nil
}

func decodeSQLiteValue(value wire.SQLiteValue) (any, error) {
	switch value.Kind {
	case "null":
		return nil, nil
	case "integer":
		if value.Integer != nil {
			return *value.Integer, nil
		}
	case "real":
		if value.Bits != nil {
			return math.Float64frombits(*value.Bits), nil
		}
	case "text":
		if value.Text != nil {
			return *value.Text, nil
		}
	case "blob":
		if value.Blob != nil {
			return append([]byte(nil), (*value.Blob)...), nil
		}
	}
	return nil, &SQLiteError{Code: "sqlite_result_invalid", Message: fmt.Sprintf("invalid SQLite value kind %q", value.Kind)}
}

func publicSQLiteError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var sqliteError *SQLiteError
	if errors.As(err, &sqliteError) {
		return sqliteError
	}
	switch value := err.(type) {
	case wire.WireError:
		result := &SQLiteError{Code: value.Code, Message: value.Message}
		if value.SQLiteCode != nil {
			result.SQLiteCode = *value.SQLiteCode
		}
		if value.StatementIndex != nil {
			result.StatementIndex = *value.StatementIndex
		}
		return result
	case *wire.WireError:
		return publicSQLiteError(*value)
	default:
		return &SQLiteError{Code: "sqlite_transport_error", Message: err.Error(), cause: err}
	}
}

func sqliteUnavailableError() error {
	return (&disabledSQLiteBackend{}).failure()
}

func pointerValue(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

// resolveSQLiteTransport applies the environment override, then defaults an
// empty selection to the all-platform FFI transport.
func resolveSQLiteTransport(configured SQLiteTransport, envOverride string) SQLiteTransport {
	if envOverride != "" {
		configured = SQLiteTransport(envOverride)
	}
	if configured == "" {
		return SQLiteTransportFFI
	}
	return configured
}
