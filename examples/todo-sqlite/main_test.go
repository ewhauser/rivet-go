package main

import (
	"reflect"
	"testing"

	"github.com/ewhauser/rivet-go/rivet"
)

func TestDecodeTodos(t *testing.T) {
	t.Parallel()
	rows := rivet.Rows{
		Columns: []string{"id", "title", "completed", "created_at"},
		Values: [][]any{
			{int64(1), "first", int64(0), int64(100)},
			{int64(2), "second", int64(1), int64(200)},
		},
	}
	want := []todo{
		{ID: 1, Title: "first", CreatedAt: 100},
		{ID: 2, Title: "second", Completed: true, CreatedAt: 200},
	}
	got, err := decodeTodos(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded todos = %#v, want %#v", got, want)
	}
}

func TestDecodeTodosRejectsUnexpectedValues(t *testing.T) {
	t.Parallel()
	_, err := decodeTodos(rivet.Rows{Values: [][]any{{"not-an-id", "title", int64(0), int64(100)}}})
	if err == nil {
		t.Fatal("decodeTodos accepted an invalid ID type")
	}
}
