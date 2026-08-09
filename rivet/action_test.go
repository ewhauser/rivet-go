package rivet

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/fxamacker/cbor/v2"
)

func encodedCBORBytes(t *testing.T, size int) cbor.RawMessage {
	t.Helper()
	if size < 5 {
		t.Fatalf("encoded CBOR size %d is too small for the test value", size)
	}
	encoded, err := cbor.Marshal(make([]byte, size-5))
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != size {
		t.Fatalf("encoded CBOR size = %d, want %d", len(encoded), size)
	}
	return cbor.RawMessage(encoded)
}

func TestTypedActionUsesCoreCBORArrayAndJSONTags(t *testing.T) {
	type state struct{ Count int }
	type argument struct {
		Increment int `json:"amount"`
	}
	type result struct {
		Value int `json:"count"`
	}
	actorContext := &Context[state]{}
	handler := Action(func(ctx *Context[state], input argument) (result, error) {
		ctx.State().Count += input.Increment
		return result{Value: ctx.State().Count}, nil
	})
	arguments, err := cbor.Marshal([]any{map[string]any{"amount": 3}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := handler.invoke(context.Background(), actorContext, arguments)
	if err != nil {
		t.Fatal(err)
	}
	var got result
	if err := cbor.Unmarshal(output, &got); err != nil {
		t.Fatal(err)
	}
	if got.Value != 3 || actorContext.State().Count != 3 {
		t.Fatalf("action result = %#v, state = %#v", got, actorContext.State())
	}
}

func TestRawActionPreservesCoreBytes(t *testing.T) {
	type state struct{}
	input, err := cbor.Marshal([]any{"raw"})
	if err != nil {
		t.Fatal(err)
	}
	want, err := cbor.Marshal(map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	handler := RawAction(func(_ *Context[state], got []byte) ([]byte, error) {
		if !reflect.DeepEqual(got, input) {
			t.Fatal("raw action input bytes changed at the SDK boundary")
		}
		return want, nil
	})
	output, err := handler.invoke(context.Background(), &Context[state]{}, input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(output, want) {
		t.Fatal("raw action output bytes changed at the SDK boundary")
	}
}

func TestActionResultSizeLimit(t *testing.T) {
	type state struct{}
	arguments, err := cbor.Marshal([]any{0})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name string
		size int
		raw  bool
		code string
	}{
		{name: "typed exact limit", size: maxActionResultBytes},
		{name: "typed over limit", size: maxActionResultBytes + 1, code: "action_result_too_large"},
		{name: "raw exact limit", size: maxActionResultBytes, raw: true},
		{name: "raw over limit", size: maxActionResultBytes + 1, raw: true, code: "action_result_too_large"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := encodedCBORBytes(t, test.size)
			var handler ActionHandler[state]
			if test.raw {
				handler = RawAction(func(*Context[state], []byte) ([]byte, error) {
					return result, nil
				})
			} else {
				handler = Action(func(*Context[state], int) (cbor.RawMessage, error) {
					return result, nil
				})
			}
			output, invokeErr := handler.invoke(context.Background(), &Context[state]{}, arguments)
			if test.code == "" {
				if invokeErr != nil {
					t.Fatal(invokeErr)
				}
				if len(output) != test.size {
					t.Fatalf("action result size = %d, want %d", len(output), test.size)
				}
				return
			}
			var structured pump.HandlerError
			if !errors.As(invokeErr, &structured) || structured.Code != test.code {
				t.Fatalf("action error = %#v, want %s HandlerError", invokeErr, test.code)
			}
		})
	}
}

func TestActionErrorsStayStructured(t *testing.T) {
	type state struct{}
	arguments, err := cbor.Marshal([]any{1})
	if err != nil {
		t.Fatal(err)
	}
	handler := Action(func(*Context[state], int) (int, error) {
		return 0, ActionError{Code: "quota_reached", Message: "quota reached"}
	})
	_, err = handler.invoke(context.Background(), &Context[state]{}, arguments)
	var structured pump.HandlerError
	if !errors.As(err, &structured) || structured.Code != "quota_reached" {
		t.Fatalf("action error = %#v, want quota_reached HandlerError", err)
	}
}

func TestInternalHandlerErrorsStayStructuredThroughActions(t *testing.T) {
	type state struct{}
	arguments, err := cbor.Marshal([]any{1})
	if err != nil {
		t.Fatal(err)
	}
	handler := Action(func(*Context[state], int) (int, error) {
		return 0, pump.HandlerError{
			Code:    "alarm_set_failed",
			Message: "core rejected alarm",
		}
	})
	_, err = handler.invoke(context.Background(), &Context[state]{}, arguments)
	var structured pump.HandlerError
	if !errors.As(err, &structured) || structured.Code != "alarm_set_failed" ||
		structured.Message != "core rejected alarm" {
		t.Fatalf("action error = %#v, want preserved alarm_set_failed", err)
	}
}

func TestTypedActionDecodeAndEncodeFailuresStayStructured(t *testing.T) {
	type state struct{}
	handler := Action(func(*Context[state], int) (chan int, error) {
		return make(chan int), nil
	})

	tests := []struct {
		name string
		args []any
		code string
	}{
		{name: "wrong arity", args: []any{}, code: "action_decode_failed"},
		{name: "wrong type", args: []any{"not-an-integer"}, code: "action_decode_failed"},
		{name: "result encode", args: []any{1}, code: "action_encode_failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := cbor.Marshal(test.args)
			if err != nil {
				t.Fatal(err)
			}
			_, err = handler.invoke(context.Background(), &Context[state]{}, encoded)
			var structured pump.HandlerError
			if !errors.As(err, &structured) || structured.Code != test.code {
				t.Fatalf("action error = %v, want %s", err, test.code)
			}
		})
	}
}

func TestActionInvokeRejectsNilContext(t *testing.T) {
	type state struct{}
	arguments, err := cbor.Marshal([]any{1})
	if err != nil {
		t.Fatal(err)
	}
	handler := Action(func(*Context[state], int) (int, error) { return 1, nil })
	_, err = handler.invoke(nil, &Context[state]{}, arguments)
	var structured pump.HandlerError
	if !errors.As(err, &structured) || structured.Code != "action_context_invalid" {
		t.Fatalf("nil-context error = %v, want action_context_invalid", err)
	}
}

func TestActionWithContextObservesDeadline(t *testing.T) {
	type state struct{}
	arguments, err := cbor.Marshal([]any{struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	handler := ActionWithContext(func(ctx context.Context, _ *Context[state], _ struct{}) (int, error) {
		if _, ok := ctx.Deadline(); !ok {
			return 0, errors.New("action context has no deadline")
		}
		<-ctx.Done()
		return 0, ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = handler.invoke(ctx, &Context[state]{}, arguments)
	var structured pump.HandlerError
	if !errors.As(err, &structured) || structured.Code != "action_timed_out" {
		t.Fatalf("deadline error = %v, want action_timed_out", err)
	}
}
