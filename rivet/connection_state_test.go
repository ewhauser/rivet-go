package rivet

import (
	"context"
	"reflect"
	"testing"

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

func newConnectionTestContext[T any]() *Context[T] {
	return &Context[T]{
		connections:        make(map[string]*Connection),
		pendingConnections: make(map[string]*Connection),
	}
}

func TestActorConnectLifecycleInitializesMutatesAndClosesTypedState(t *testing.T) {
	parameters, err := cbor.Marshal(testConnectionParameters{Username: "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	var opened *Connection
	var closed *Connection
	adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		ConnectionState: NewConnectionState(func(
			ctx *Context[struct{}], connection *Connection,
		) (testConnectionState, error) {
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

func decodeJSONState[T any](data []byte, target *T) error {
	decoded, err := decodeState[T](data)
	if err != nil {
		return err
	}
	*target = decoded
	return nil
}
