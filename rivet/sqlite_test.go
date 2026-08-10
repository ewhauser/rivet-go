package rivet

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/internal/wire"
)

type lifecycleSQLiteBackend struct {
	execStarted    chan struct{}
	execRelease    chan struct{}
	beginStarted   chan struct{}
	beginRelease   chan struct{}
	rollbackCalled chan string
	closed         chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

func (b *lifecycleSQLiteBackend) exec(context.Context, string, []wire.SQLiteValue, *string) (Result, error) {
	close(b.execStarted)
	<-b.execRelease
	return Result{}, nil
}

func (*lifecycleSQLiteBackend) query(context.Context, string, []wire.SQLiteValue, *string) (Rows, error) {
	return Rows{}, nil
}

func (b *lifecycleSQLiteBackend) begin(context.Context, string, time.Duration) error {
	if b.beginStarted != nil {
		close(b.beginStarted)
		<-b.beginRelease
	}
	return nil
}
func (*lifecycleSQLiteBackend) commit(context.Context, string) error { return nil }
func (b *lifecycleSQLiteBackend) rollback(_ context.Context, leaseKey string) error {
	b.rollbackCalled <- leaseKey
	return nil
}
func (b *lifecycleSQLiteBackend) close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return b.closeErr
}

func TestSQLiteValueMappingPreservesAllPublicTypes(t *testing.T) {
	values, err := encodeSQLiteArgs([]any{nil, int64(-7), float64(1.25), "text", []byte{0, 1}, []byte{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 6 || values[0].Kind != "null" || values[1].Integer == nil || *values[1].Integer != -7 || values[2].Bits == nil || math.Float64frombits(*values[2].Bits) != 1.25 || values[3].Text == nil || *values[3].Text != "text" {
		t.Fatalf("encoded values = %#v", values)
	}
	if values[4].Blob == nil || len(*values[4].Blob) != 2 || values[5].Blob == nil || *values[5].Blob == nil || len(*values[5].Blob) != 0 {
		t.Fatalf("blob values = %#v %#v", values[4], values[5])
	}

	rows, err := decodeSQLiteRows(
		[]string{"n", "i", "r", "t", "b"},
		values[:5],
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Values) != 1 || rows.Values[0][0] != nil || rows.Values[0][1] != int64(-7) || rows.Values[0][2] != float64(1.25) || rows.Values[0][3] != "text" {
		t.Fatalf("decoded row = %#v", rows.Values)
	}
	blob, ok := rows.Values[0][4].([]byte)
	if !ok || len(blob) != 2 {
		t.Fatalf("decoded blob = %T %#v", rows.Values[0][4], rows.Values[0][4])
	}
}

func TestSQLiteArgumentTypeErrorIsStructured(t *testing.T) {
	_, err := encodeSQLiteArgs([]any{int(1)})
	var structured *SQLiteError
	if !errors.As(err, &structured) || structured.Code != "sqlite_argument_type" {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestSQLiteWireErrorKeepsStatementMetadata(t *testing.T) {
	code := int32(2067)
	statement := uint32(2)
	err := publicSQLiteError(wire.WireError{
		Code:           "sqlite_error",
		Message:        "unique constraint",
		SQLiteCode:     &code,
		StatementIndex: &statement,
	})
	var structured *SQLiteError
	if !errors.As(err, &structured) || structured.Code != "sqlite_error" || structured.SQLiteCode != code || structured.StatementIndex != statement {
		t.Fatalf("error = %#v", err)
	}
}

func TestSQLiteSleepFenceRollsBackLeaseAndWaitsForInflightOperation(t *testing.T) {
	backend := &lifecycleSQLiteBackend{
		execStarted:    make(chan struct{}),
		execRelease:    make(chan struct{}),
		rollbackCalled: make(chan string, 1),
		closed:         make(chan struct{}),
	}
	database := makeDB(backend)
	tx, err := database.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	execDone := make(chan error, 1)
	go func() {
		_, execErr := tx.Exec(context.Background(), "SELECT 1")
		execDone <- execErr
	}()
	select {
	case <-backend.execStarted:
	case <-time.After(time.Second):
		t.Fatal("transaction operation did not start")
	}

	prepareDone := make(chan error, 1)
	go func() {
		_, err := database.prepareForSleep()
		prepareDone <- err
	}()
	select {
	case <-backend.rollbackCalled:
	case <-time.After(time.Second):
		t.Fatal("sleep close did not roll back the open lease")
	}
	select {
	case err := <-prepareDone:
		t.Fatalf("sleep fence returned before the accepted operation finished: %v", err)
	default:
	}
	select {
	case <-backend.closed:
		t.Fatal("transport closed before the accepted operation finished")
	default:
	}

	close(backend.execRelease)
	if err := <-execDone; err != nil {
		t.Fatalf("accepted operation failed: %v", err)
	}
	if err := <-prepareDone; err != nil {
		t.Fatalf("sleep fence failed: %v", err)
	}
	select {
	case <-backend.closed:
		t.Fatal("sleep fence closed the transport before intent acceptance")
	default:
	}

	database.resumeAfterSleepFailure()
	_, err = tx.Exec(context.Background(), "SELECT 1")
	var structured *SQLiteError
	if !errors.As(err, &structured) || structured.Code != "invalid_lease_key" {
		t.Fatalf("old transaction error = %T %v, want invalid_lease_key", err, err)
	}
	if _, err = database.Query(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("database was not reusable after rejected sleep: %v", err)
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backend.closed:
	default:
		t.Fatal("transport was not closed during actor cleanup")
	}
}

func TestSleepIntentFailureRestoresSQLiteAdmission(t *testing.T) {
	backend := &lifecycleSQLiteBackend{closed: make(chan struct{})}
	database := makeDB(backend)
	intentErr := errors.New("sleep intent rejected")

	err := fenceSQLiteAndRequestSleep(database, func() error { return intentErr })
	if !errors.Is(err, intentErr) {
		t.Fatalf("Sleep error = %v, want %v", err, intentErr)
	}
	if _, err := database.Query(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("database was not reusable after rejected sleep: %v", err)
	}
	select {
	case <-backend.closed:
		t.Fatal("rejected sleep closed the SQLite transport")
	default:
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
}

func TestSleepIntentAcceptanceKeepsSQLiteFencedUntilCleanup(t *testing.T) {
	backend := &lifecycleSQLiteBackend{closed: make(chan struct{})}
	database := makeDB(backend)

	if err := fenceSQLiteAndRequestSleep(database, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	_, err := database.Query(context.Background(), "SELECT 1")
	var structured *SQLiteError
	if !errors.As(err, &structured) || structured.Code != "sqlite_endpoint_closed" {
		t.Fatalf("accepted-sleep database error = %T %v, want sqlite_endpoint_closed", err, err)
	}
	select {
	case <-backend.closed:
		t.Fatal("accepted sleep closed transport before actor cleanup")
	default:
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backend.closed:
	default:
		t.Fatal("actor cleanup did not close accepted-sleep transport")
	}
}

func TestRepeatedSleepReusesAcceptedSQLiteFence(t *testing.T) {
	backend := &lifecycleSQLiteBackend{closed: make(chan struct{})}
	database := makeDB(backend)
	requests := 0
	requestSleep := func() error {
		requests++
		return nil
	}

	if err := fenceSQLiteAndRequestSleep(database, requestSleep); err != nil {
		t.Fatal(err)
	}
	if err := fenceSQLiteAndRequestSleep(database, requestSleep); err != nil {
		t.Fatalf("repeated Sleep rejected the accepted SQLite fence: %v", err)
	}
	if requests != 2 {
		t.Fatalf("sleep requests = %d, want 2", requests)
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
}

func TestFailedRepeatedSleepDoesNotReopenAcceptedSQLiteFence(t *testing.T) {
	backend := &lifecycleSQLiteBackend{closed: make(chan struct{})}
	database := makeDB(backend)
	if err := fenceSQLiteAndRequestSleep(database, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	intentErr := errors.New("redundant sleep rejected")
	if err := fenceSQLiteAndRequestSleep(database, func() error { return intentErr }); !errors.Is(err, intentErr) {
		t.Fatalf("repeated Sleep error = %v, want %v", err, intentErr)
	}
	_, err := database.Query(context.Background(), "SELECT 1")
	var structured *SQLiteError
	if !errors.As(err, &structured) || structured.Code != "sqlite_endpoint_closed" {
		t.Fatalf("accepted-sleep database error = %T %v, want sqlite_endpoint_closed", err, err)
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSleepFenceRetiresTransactionRacingBegin(t *testing.T) {
	backend := &lifecycleSQLiteBackend{
		beginStarted:   make(chan struct{}),
		beginRelease:   make(chan struct{}),
		rollbackCalled: make(chan string, 1),
		closed:         make(chan struct{}),
	}
	database := makeDB(backend)
	beginResult := make(chan error, 1)
	go func() {
		_, err := database.Begin(context.Background())
		beginResult <- err
	}()
	select {
	case <-backend.beginStarted:
	case <-time.After(time.Second):
		t.Fatal("transaction begin did not reach backend")
	}

	prepareResult := make(chan error, 1)
	go func() {
		_, err := database.prepareForSleep()
		prepareResult <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		_, err := database.Query(context.Background(), "SELECT 1")
		var structured *SQLiteError
		if errors.As(err, &structured) && structured.Code == "sqlite_endpoint_closed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sleep fence did not stop admission")
		}
		time.Sleep(time.Millisecond)
	}
	close(backend.beginRelease)

	var structured *SQLiteError
	if err := <-beginResult; !errors.As(err, &structured) || structured.Code != "sqlite_endpoint_closed" {
		t.Fatalf("racing Begin error = %T %v, want sqlite_endpoint_closed", err, err)
	}
	select {
	case <-backend.rollbackCalled:
	case <-time.After(time.Second):
		t.Fatal("racing transaction lease was not rolled back")
	}
	if err := <-prepareResult; err != nil {
		t.Fatalf("prepare sleep: %v", err)
	}
	database.resumeAfterSleepFailure()
	if _, err := database.Query(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("database was not reusable after racing Begin: %v", err)
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteBeginRejectsASecondOpenTransaction(t *testing.T) {
	backend := &lifecycleSQLiteBackend{rollbackCalled: make(chan string, 1)}
	database := makeDB(backend)
	first, err := database.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Begin(context.Background())
	var structured *SQLiteError
	if !errors.As(err, &structured) || structured.Code != "transaction_already_open" {
		t.Fatalf("second Begin error = %T %v, want transaction_already_open", err, err)
	}
	if err := first.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}
