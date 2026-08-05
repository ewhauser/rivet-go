package rivet

import (
	"errors"
	"math"
	"testing"

	"github.com/ewhauser/rivet-go/internal/wire"
)

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
