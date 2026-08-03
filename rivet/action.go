package rivet

import (
	"context"
	"errors"
	"fmt"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/fxamacker/cbor/v2"
)

// Actions is the explicit action table for an Actor. The engine delivers
// action arguments as a CBOR array at v2.3.10; Action applies the same struct
// tags and typed value rules as encoding/json while handling that transport.
type Actions[T any] map[string]ActionHandler[T]

// ActionHandler is created by Action or RawAction.
type ActionHandler[T any] interface {
	invoke(context.Context, *Context[T], []byte) ([]byte, error)
}

type typedAction[T, A, R any] struct {
	handler func(*Context[T], A) (R, error)
}

type rawAction[T any] struct {
	handler func(*Context[T], []byte) ([]byte, error)
}

// ActionError lets a handler choose the structured client-visible error code.
// Empty codes fall back to action_failed.
type ActionError struct {
	Code    string
	Message string
}

func (e ActionError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return e.Code
	}
	return "action failed"
}

func (e ActionError) ActionCode() string { return e.Code }

// Action adapts a typed single-argument Go function to Rivet's CBOR action
// transport. JSON struct tags are honored by the codec.
func Action[T, A, R any](handler func(*Context[T], A) (R, error)) ActionHandler[T] {
	return typedAction[T, A, R]{handler: handler}
}

// RawAction bypasses typed argument and result encoding. Input is the exact
// CBOR argument array delivered by rivetkit-core and output must be one CBOR
// value suitable for the engine client response.
func RawAction[T any](handler func(*Context[T], []byte) ([]byte, error)) ActionHandler[T] {
	return rawAction[T]{handler: handler}
}

func (a typedAction[T, A, R]) invoke(
	_ context.Context,
	actorContext *Context[T],
	encoded []byte,
) ([]byte, error) {
	if a.handler == nil {
		return nil, errors.New("action handler is nil")
	}
	var arguments []cbor.RawMessage
	if err := cbor.Unmarshal(encoded, &arguments); err != nil {
		return nil, pump.HandlerError{
			Code:    "action_decode_failed",
			Message: fmt.Sprintf("decode action argument array: %v", err),
		}
	}
	if len(arguments) != 1 {
		return nil, pump.HandlerError{
			Code:    "action_decode_failed",
			Message: fmt.Sprintf("action requires one argument, received %d", len(arguments)),
		}
	}
	var argument A
	if err := cbor.Unmarshal(arguments[0], &argument); err != nil {
		return nil, pump.HandlerError{
			Code:    "action_decode_failed",
			Message: fmt.Sprintf("decode action argument: %v", err),
		}
	}
	result, err := a.handler(actorContext, argument)
	if err != nil {
		return nil, actionHandlerError(err)
	}
	encodedResult, err := cbor.Marshal(result)
	if err != nil {
		return nil, pump.HandlerError{
			Code:    "action_encode_failed",
			Message: fmt.Sprintf("encode action result: %v", err),
		}
	}
	return encodedResult, nil
}

func (a rawAction[T]) invoke(
	_ context.Context,
	actorContext *Context[T],
	encoded []byte,
) ([]byte, error) {
	if a.handler == nil {
		return nil, errors.New("raw action handler is nil")
	}
	result, err := a.handler(actorContext, append([]byte(nil), encoded...))
	if err != nil {
		return nil, actionHandlerError(err)
	}
	return append([]byte(nil), result...), nil
}

func actionHandlerError(err error) error {
	var structured interface {
		ActionCode() string
	}
	if errors.As(err, &structured) && structured.ActionCode() != "" {
		return pump.HandlerError{Code: structured.ActionCode(), Message: err.Error()}
	}
	return pump.HandlerError{Code: "action_failed", Message: err.Error()}
}
