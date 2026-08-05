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
	rollbackCalled chan string
	closed         chan struct{}
	closeOnce      sync.Once
}

func (b *lifecycleSQLiteBackend) exec(context.Context, string, []wire.SQLiteValue, *string) (Result, error) {
	close(b.execStarted)
	<-b.execRelease
	return Result{}, nil
}

func (*lifecycleSQLiteBackend) query(context.Context, string, []wire.SQLiteValue, *string) (Rows, error) {
	return Rows{}, nil
}

func (*lifecycleSQLiteBackend) begin(context.Context, string, time.Duration) error { return nil }
func (*lifecycleSQLiteBackend) commit(context.Context, string) error               { return nil }
func (b *lifecycleSQLiteBackend) rollback(_ context.Context, leaseKey string) error {
	b.rollbackCalled <- leaseKey
	return nil
}
func (b *lifecycleSQLiteBackend) close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
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

func TestSQLiteCloseRollsBackLeaseAndWaitsForInflightOperation(t *testing.T) {
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

	closeDone := make(chan error, 1)
	go func() { closeDone <- database.closeForSleep() }()
	select {
	case <-backend.rollbackCalled:
	case <-time.After(time.Second):
		t.Fatal("sleep close did not roll back the open lease")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("sleep close returned before the accepted operation finished: %v", err)
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
	if err := <-closeDone; err != nil {
		t.Fatalf("sleep close failed: %v", err)
	}
	select {
	case <-backend.closed:
	default:
		t.Fatal("transport was not closed")
	}

	_, err = tx.Exec(context.Background(), "SELECT 1")
	var structured *SQLiteError
	if !errors.As(err, &structured) || structured.Code != "sqlite_endpoint_closed" {
		t.Fatalf("old transaction error = %T %v, want sqlite_endpoint_closed", err, err)
	}
	_, err = database.Query(context.Background(), "SELECT 1")
	if !errors.As(err, &structured) || structured.Code != "sqlite_endpoint_closed" {
		t.Fatalf("closed database error = %T %v, want sqlite_endpoint_closed", err, err)
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
