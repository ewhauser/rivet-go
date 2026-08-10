package rivet

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/ewhauser/rivet-go/internal/wire"
	"github.com/fxamacker/cbor/v2"
)

type testConnectionParameters struct {
	Username string `json:"username"`
}

type testConnectionState struct {
	Username string `json:"username"`
	Opens    int    `json:"opens"`
}

type sizedConnectionState struct {
	Size int
}

type actionAtomicityState struct {
	Count int
}

func (s sizedConnectionState) MarshalBinary() ([]byte, error) {
	return bytes.Repeat([]byte{'s'}, s.Size), nil
}

func (s *sizedConnectionState) UnmarshalBinary(data []byte) error {
	s.Size = len(data)
	return nil
}

func newConnectionTestContext[T any]() *Context[T] {
	return &Context[T]{
		connections: make(map[string]*Connection),
	}
}

func TestActorConnectLifecycleInitializesMutatesAndClosesTypedState(t *testing.T) {
	parameters, err := cbor.Marshal(testConnectionParameters{Username: "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	var opened *Connection
	var closed *Connection
	var initialized *Connection
	adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		ConnectionState: NewConnectionState(func(
			ctx *Context[struct{}], connection *Connection,
		) (testConnectionState, error) {
			initialized = connection
			if got := ctx.Connections(); len(got) != 0 {
				t.Fatalf("preflight exposed connection: %#v", got)
			}
			decoded, decodeErr := DecodeConnectionParameters[testConnectionParameters](connection)
			if decodeErr != nil {
				return testConnectionState{}, decodeErr
			}
			return testConnectionState{Username: decoded.Username}, nil
		}),
		OnActorConnect: func(ctx *Context[struct{}], connection *Connection) error {
			opened = connection
			if got := ctx.Connections(); len(got) != 1 || got[0] != connection {
				t.Fatalf("connection was not visible during connect hook: %#v", got)
			}
			state, stateErr := GetConnectionState[testConnectionState](connection)
			if stateErr != nil {
				return stateErr
			}
			state.Opens++
			return nil
		},
		OnActorDisconnect: func(ctx *Context[struct{}], connection *Connection) {
			closed = connection
			if got := ctx.Connections(); len(got) != 0 {
				t.Fatalf("closed connection remained enumerable: %#v", got)
			}
		},
	}}
	actorContext := newConnectionTestContext[struct{}]()
	snapshot := wire.Connection{
		ID: "connection-b", Parameters: parameters, Path: "/connect",
		Headers: map[string]string{"x-test": "one"}, CanHibernate: true, ActorConnect: true,
	}

	preflightState, err := adapter.ConnectionPreflight(context.Background(), nil, wire.Event{
		Kind: wire.EventConnectionPreflight, Connection: &snapshot,
	}, actorContext)
	if err != nil {
		t.Fatal(err)
	}
	if len(actorContext.Connections()) != 0 {
		t.Fatal("preflight connection became public before core accepted it")
	}
	snapshot.State = preflightState
	openState, err := adapter.ConnectionOpen(context.Background(), nil, wire.Event{
		Kind: wire.EventConnectionOpen, Connection: &snapshot,
	}, actorContext)
	if err != nil {
		t.Fatal(err)
	}
	if opened == nil || opened.ID() != snapshot.ID || !opened.CanHibernate() || opened.Resumed() {
		t.Fatalf("opened connection = %#v", opened)
	}
	if opened == initialized {
		t.Fatal("connection preflight retained its initializer object until open")
	}
	if initialized == nil || !initialized.Closed() {
		t.Fatalf("initializer connection remained usable after preflight: %#v", initialized)
	}
	var persisted testConnectionState
	if err := decodeJSONState(openState, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != (testConnectionState{Username: "Ada", Opens: 1}) {
		t.Fatalf("persisted open state = %#v", persisted)
	}

	closeState, err := adapter.ConnectionClose(context.Background(), nil, wire.Event{
		Kind: wire.EventConnectionClose, Connection: &snapshot,
	}, actorContext)
	if err != nil {
		t.Fatal(err)
	}
	if closed != opened || !closed.Closed() {
		t.Fatalf("disconnect hook connection = %#v, opened = %#v", closed, opened)
	}
	if !reflect.DeepEqual(closeState, openState) {
		t.Fatalf("close state = %q, want %q", closeState, openState)
	}
}

func TestActorStartRestoresHibernatedConnectionState(t *testing.T) {
	encoded, err := encodeState(&testConnectionState{Username: "Grace", Opens: 4})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		ConnectionState: NewConnectionState(func(*Context[struct{}], *Connection) (testConnectionState, error) {
			t.Fatal("initializer ran for a restored connection")
			return testConnectionState{}, nil
		}),
		OnStart: func(ctx *Context[struct{}]) error {
			connections := ctx.Connections()
			if len(connections) != 1 || connections[0].ID() != "restored" || !connections[0].Resumed() {
				t.Fatalf("restored connections = %#v", connections)
			}
			state, stateErr := GetConnectionState[testConnectionState](connections[0])
			if stateErr != nil {
				return stateErr
			}
			if *state != (testConnectionState{Username: "Grace", Opens: 4}) {
				t.Fatalf("restored state = %#v", state)
			}
			return nil
		},
	}}
	state, err := adapter.Start(context.Background(), nil, wire.Event{
		Kind: wire.EventActorStart,
		Connections: []wire.Connection{{
			ID: "restored", State: encoded, CanHibernate: true, Resumed: true, ActorConnect: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	actorContext := state.(*Context[struct{}])
	if actorContext.CurrentConnection() != nil {
		t.Fatal("actor start reported a current connection")
	}
	if err := adapter.Stop(context.Background(), nil, wire.Event{}, actorContext); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionStateHelpersRejectMissingAndWrongTypes(t *testing.T) {
	if _, err := GetConnectionState[testConnectionState](nil); err == nil {
		t.Fatal("nil connection state lookup succeeded")
	}
	connection := &Connection{state: new(int)}
	if _, err := GetConnectionState[testConnectionState](connection); err == nil {
		t.Fatal("wrong connection state type succeeded")
	}
	if _, err := DecodeConnectionParameters[testConnectionParameters](nil); err == nil {
		t.Fatal("nil connection parameter decode succeeded")
	}
	if current := (*Context[struct{}])(nil).CurrentConnection(); current != nil {
		t.Fatalf("nil context current connection = %#v", current)
	}
}

func TestConnectionStateSizeLimit(t *testing.T) {
	t.Run("exact limit", func(t *testing.T) {
		adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
			ConnectionState: NewConnectionState(func(*Context[struct{}], *Connection) (sizedConnectionState, error) {
				return sizedConnectionState{Size: maxConnectionStateBytes}, nil
			}),
		}}
		actorContext := newConnectionTestContext[struct{}]()
		encoded, err := adapter.ConnectionPreflight(context.Background(), nil, wire.Event{
			Kind:       wire.EventConnectionPreflight,
			Connection: &wire.Connection{ID: "exact", ActorConnect: true},
		}, actorContext)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != maxConnectionStateBytes {
			t.Fatalf("connection state size = %d, want %d", len(encoded), maxConnectionStateBytes)
		}
	})

	t.Run("preflight over limit", func(t *testing.T) {
		adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
			ConnectionState: NewConnectionState(func(*Context[struct{}], *Connection) (sizedConnectionState, error) {
				return sizedConnectionState{Size: maxConnectionStateBytes + 1}, nil
			}),
		}}
		actorContext := newConnectionTestContext[struct{}]()
		_, err := adapter.ConnectionPreflight(context.Background(), nil, wire.Event{
			Kind:       wire.EventConnectionPreflight,
			Connection: &wire.Connection{ID: "oversized", ActorConnect: true},
		}, actorContext)
		var structured pump.HandlerError
		if !errors.As(err, &structured) || structured.Code != "connection_state_too_large" {
			t.Fatalf("connection error = %#v, want connection_state_too_large HandlerError", err)
		}
		if len(actorContext.connections) != 0 {
			t.Fatalf("oversized preflight left active connections: %#v", actorContext.connections)
		}
	})

	t.Run("open mutation over limit rolls back", func(t *testing.T) {
		var opened *Connection
		adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
			ConnectionState: NewConnectionState(func(*Context[struct{}], *Connection) (sizedConnectionState, error) {
				return sizedConnectionState{Size: 1}, nil
			}),
			OnActorConnect: func(_ *Context[struct{}], connection *Connection) error {
				opened = connection
				state, err := GetConnectionState[sizedConnectionState](connection)
				if err != nil {
					return err
				}
				state.Size = maxConnectionStateBytes + 1
				return nil
			},
		}}
		actorContext := newConnectionTestContext[struct{}]()
		snapshot := wire.Connection{ID: "open-oversized", ActorConnect: true}
		preflight, err := adapter.ConnectionPreflight(context.Background(), nil, wire.Event{
			Kind: wire.EventConnectionPreflight, Connection: &snapshot,
		}, actorContext)
		if err != nil {
			t.Fatal(err)
		}
		snapshot.State = preflight
		_, err = adapter.ConnectionOpen(context.Background(), nil, wire.Event{
			Kind: wire.EventConnectionOpen, Connection: &snapshot,
		}, actorContext)
		var structured pump.HandlerError
		if !errors.As(err, &structured) || structured.Code != "connection_state_too_large" {
			t.Fatalf("connection error = %#v, want connection_state_too_large HandlerError", err)
		}
		if opened == nil || !opened.Closed() {
			t.Fatalf("oversized opened connection was not closed: %#v", opened)
		}
		if len(actorContext.connections) != 0 {
			t.Fatalf("oversized open left tracked connections: %#v", actorContext.connections)
		}
	})
}

func TestActorConnectActionValidatesConnectionStateBeforePersisting(t *testing.T) {
	adapter := &actorAdapter[actionAtomicityState]{definition: Actor[actionAtomicityState]{
		ConnectionState: NewConnectionState(func(
			*Context[actionAtomicityState], *Connection,
		) (sizedConnectionState, error) {
			return sizedConnectionState{Size: 1}, nil
		}),
		Actions: Actions[actionAtomicityState]{
			"overflow": Action(func(
				actor *Context[actionAtomicityState], _ struct{},
			) (int, error) {
				actor.State().Count++
				connection := actor.CurrentConnection()
				connectionState, err := GetConnectionState[sizedConnectionState](connection)
				if err != nil {
					return 0, err
				}
				connectionState.Size = maxConnectionStateBytes + 1
				return actor.State().Count, nil
			}),
		},
	}}
	actorContext := newConnectionTestContext[actionAtomicityState]()
	snapshot := wire.Connection{ID: "action-overflow", ActorConnect: true}
	preflight, err := adapter.ConnectionPreflight(context.Background(), nil, wire.Event{
		Kind: wire.EventConnectionPreflight, Connection: &snapshot,
	}, actorContext)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.State = preflight
	if _, err := adapter.ConnectionOpen(context.Background(), nil, wire.Event{
		Kind: wire.EventConnectionOpen, Connection: &snapshot,
	}, actorContext); err != nil {
		t.Fatal(err)
	}
	args, err := cbor.Marshal([]any{struct{}{}})
	if err != nil {
		t.Fatal(err)
	}

	result, err := adapter.Action(context.Background(), nil, wire.Event{
		Kind: wire.EventActionCall, Action: "overflow", Args: args, ConnID: &snapshot.ID,
	}, actorContext)
	var structured pump.HandlerError
	if !errors.As(err, &structured) || structured.Code != "connection_state_too_large" {
		t.Fatalf("action error = %#v, want connection_state_too_large HandlerError", err)
	}
	if result.Output != nil || result.ConnectionState != nil {
		t.Fatalf("failed action result = %#v, want empty", result)
	}
	if actorContext.State().Count != 1 {
		t.Fatalf("action was not invoked before connection-state validation: state=%#v", actorContext.State())
	}
}

func decodeJSONState[T any](data []byte, target *T) error {
	decoded, err := decodeState[T](data)
	if err != nil {
		return err
	}
	*target = decoded
	return nil
}
