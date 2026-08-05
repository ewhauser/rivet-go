package pump

import (
	"errors"
	"testing"

	"github.com/ewhauser/rivet-go/internal/wire"
)

func TestSQLiteResponseAssemblerPreservesFirstChunkMetadata(t *testing.T) {
	firstText := "first"
	secondText := "second"
	lastInsertID := int64(11)
	var assembler sqliteResponseAssembler
	done, err := assembler.add(wire.Event{
		ChunkIndex:      0,
		Columns:         []string{"value"},
		SQLiteValues:    []wire.SQLiteValue{{Kind: "text", Text: &firstText}},
		RowsAffected:    7,
		LastInsertID:    &lastInsertID,
		SQLiteRequestID: 1,
	})
	if err != nil || done {
		t.Fatalf("first chunk = (done %v, err %v)", done, err)
	}
	done, err = assembler.add(wire.Event{
		ChunkIndex:      1,
		Done:            true,
		SQLiteValues:    []wire.SQLiteValue{{Kind: "text", Text: &secondText}},
		SQLiteRequestID: 1,
	})
	if err != nil || !done {
		t.Fatalf("second chunk = (done %v, err %v)", done, err)
	}
	response := assembler.response
	if len(response.Columns) != 1 || len(response.Values) != 2 || response.RowsAffected != 7 || response.LastInsertID == nil || *response.LastInsertID != 11 {
		t.Fatalf("assembled response = %#v", response)
	}
}

func TestSQLiteResponseAssemblerEnforcesTotalLimit(t *testing.T) {
	var assembler sqliteResponseAssembler
	value := string(make([]byte, (1<<20)-16))
	for index := uint32(0); index < 31; index++ {
		event := wire.Event{
			ChunkIndex:      index,
			SQLiteValues:    []wire.SQLiteValue{{Kind: "text", Text: &value}},
			SQLiteRequestID: 1,
		}
		if index == 0 {
			event.Columns = []string{"value"}
		}
		if done, err := assembler.add(event); err != nil || done {
			t.Fatalf("chunk %d = (done %v, err %v)", index, done, err)
		}
	}
	boundaryValue := string(make([]byte, (1<<20)-16-len("value")))
	if done, err := assembler.add(wire.Event{
		ChunkIndex:      31,
		SQLiteValues:    []wire.SQLiteValue{{Kind: "text", Text: &boundaryValue}},
		SQLiteRequestID: 1,
	}); err != nil || done {
		t.Fatalf("exact-limit chunk = (done %v, err %v)", done, err)
	}
	_, err := assembler.add(wire.Event{
		ChunkIndex:      32,
		Done:            true,
		SQLiteValues:    []wire.SQLiteValue{{Kind: "null"}},
		SQLiteRequestID: 1,
	})
	var structured wire.WireError
	if !errors.As(err, &structured) || structured.Code != "sqlite_result_too_large" {
		t.Fatalf("one-byte-over-limit error = %T %v", err, err)
	}
}
