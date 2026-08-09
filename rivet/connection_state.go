package rivet

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// ConnectionStateConfig defines typed state for every ActorConnect connection.
// Construct one with NewConnectionState.
type ConnectionStateConfig[T any] interface {
	initialize(*Context[T], *Connection) (any, error)
	decode([]byte) (any, error)
	encode(any) ([]byte, error)
}

type typedConnectionStateConfig[T, C any] struct {
	initializer func(*Context[T], *Connection) (C, error)
}

// NewConnectionState configures an actor-defined initializer for typed
// per-connection state. The initializer runs during core connection preflight,
// before the connection becomes visible through Context.Connections.
func NewConnectionState[T, C any](
	initializer func(*Context[T], *Connection) (C, error),
) ConnectionStateConfig[T] {
	return typedConnectionStateConfig[T, C]{initializer: initializer}
}

func (c typedConnectionStateConfig[T, C]) initialize(
	ctx *Context[T],
	connection *Connection,
) (any, error) {
	if c.initializer == nil {
		return nil, fmt.Errorf("connection state initializer is nil")
	}
	state, err := c.initializer(ctx, connection)
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (typedConnectionStateConfig[T, C]) decode(data []byte) (any, error) {
	state, err := decodeState[C](data)
	if err != nil {
		return nil, fmt.Errorf("decode connection state: %w", err)
	}
	return &state, nil
}

func (typedConnectionStateConfig[T, C]) encode(value any) ([]byte, error) {
	state, ok := value.(*C)
	if !ok || state == nil {
		return nil, fmt.Errorf("connection state has type %T, want %T", value, (*C)(nil))
	}
	encoded, err := encodeState(state)
	if err != nil {
		return nil, fmt.Errorf("encode connection state: %w", err)
	}
	return encoded, nil
}

// GetConnectionState returns the typed mutable state configured for connection.
// The pointer is generation-scoped and must only be used while handling actor
// work; retaining it in background goroutines can race later actor work.
func GetConnectionState[C any](connection *Connection) (*C, error) {
	if connection == nil {
		return nil, fmt.Errorf("connection is nil")
	}
	connection.stateMu.RLock()
	rawState := connection.state
	state, ok := rawState.(*C)
	connection.stateMu.RUnlock()
	if !ok || state == nil {
		return nil, fmt.Errorf("connection state has type %T, want %T", rawState, (*C)(nil))
	}
	return state, nil
}

// DecodeConnectionParameters decodes the connection's CBOR parameters into P.
// ActorConnect clients supply these parameters when opening the connection;
// calls without parameters decode the CBOR null value.
func DecodeConnectionParameters[P any](connection *Connection) (P, error) {
	var parameters P
	if connection == nil {
		return parameters, fmt.Errorf("connection is nil")
	}
	if err := cbor.Unmarshal(connection.Parameters(), &parameters); err != nil {
		return parameters, fmt.Errorf("decode connection parameters: %w", err)
	}
	return parameters, nil
}
