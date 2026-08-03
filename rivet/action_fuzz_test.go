package rivet

import (
	"context"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// FuzzTypedActionArgumentDecode drives the CBOR argument-decode path that
// core-delivered action calls hit before any handler runs. The invariant is
// error-or-result: arbitrary input must never panic or hang.
func FuzzTypedActionArgumentDecode(f *testing.F) {
	type args struct {
		Amount int               `json:"amount"`
		Labels map[string]string `json:"labels"`
	}
	valid, err := cbor.Marshal([]any{args{Amount: 3, Labels: map[string]string{"a": "b"}}})
	if err != nil {
		f.Fatal(err)
	}
	empty, err := cbor.Marshal([]any{})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(empty)
	f.Add([]byte{})

	handler := Action(func(ctx *Context[struct{}], a args) (int, error) {
		return a.Amount, nil
	})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = handler.invoke(context.Background(), nil, data)
	})
}
