package rivet

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ewhauser/rivet-go/internal/wire"
)

func TestWebSocketHooksPreserveConnectionAndFrameMetadata(t *testing.T) {
	type state struct{}
	seen := make([]string, 0, 3)
	var opened *Connection
	adapter := &actorAdapter[state]{definition: Actor[state]{
		OnConnect: func(_ *Context[state], connection *Connection) error {
			opened = connection
			seen = append(seen, "open")
			return nil
		},
		OnMessage: func(_ *Context[state], connection *Connection, message Message) {
			if connection != opened {
				t.Fatal("OnMessage received a different Connection object")
			}
			if !message.Binary || string(message.Data) != "payload" || message.MessageIndex != 9 {
				t.Fatalf("message = %#v", message)
			}
			seen = append(seen, "message")
		},
		OnDisconnect: func(_ *Context[state], connection *Connection) {
			if connection != opened {
				t.Fatal("OnDisconnect received a different Connection object")
			}
			seen = append(seen, "close")
		},
	}}
	actorContext := &Context[state]{connections: make(map[string]*Connection)}
	open := wire.Event{
		Kind:         wire.EventWSOpen,
		AID:          "actor",
		WSID:         "ws-1",
		Path:         "/chat?room=one",
		Headers:      map[string]string{"x-test": "one"},
		CanHibernate: false,
	}
	if err := adapter.WebSocketOpen(context.Background(), nil, open, actorContext); err != nil {
		t.Fatal(err)
	}
	if opened.ID() != "ws-1" || opened.Path() != "/chat?room=one" || opened.CanHibernate() {
		t.Fatalf("opened connection = %#v", opened)
	}
	headers := opened.Headers()
	headers["x-test"] = "changed"
	if opened.Headers()["x-test"] != "one" {
		t.Fatal("Headers returned mutable connection state")
	}
	if err := adapter.WebSocketMessage(context.Background(), nil, wire.Event{
		Kind:         wire.EventWSMessage,
		WSID:         "ws-1",
		Data:         []byte("payload"),
		Binary:       true,
		MessageIndex: 9,
	}, actorContext); err != nil {
		t.Fatal(err)
	}
	code := uint16(1000)
	if err := adapter.WebSocketClose(context.Background(), nil, wire.Event{
		Kind:      wire.EventWSClose,
		WSID:      "ws-1",
		CloseCode: &code,
		Reason:    "done",
	}, actorContext); err != nil {
		t.Fatal(err)
	}
	if !opened.Closed() {
		t.Fatal("connection was not marked closed")
	}
	gotCode, gotReason := opened.CloseInfo()
	if gotCode == nil || *gotCode != 1000 || gotReason != "done" {
		t.Fatalf("close info = %v %q", gotCode, gotReason)
	}
	if !reflect.DeepEqual(seen, []string{"open", "message", "close"}) {
		t.Fatalf("hook order = %#v", seen)
	}
}

func TestRejectedWebSocketDoesNotRunLaterHooks(t *testing.T) {
	type state struct{}
	messageCalled := false
	disconnectCalled := false
	adapter := &actorAdapter[state]{definition: Actor[state]{
		OnConnect: func(*Context[state], *Connection) error {
			return errors.New("rejected")
		},
		OnMessage: func(*Context[state], *Connection, Message) { messageCalled = true },
		OnDisconnect: func(*Context[state], *Connection) {
			disconnectCalled = true
		},
	}}
	actorContext := &Context[state]{connections: make(map[string]*Connection)}
	open := wire.Event{Kind: wire.EventWSOpen, AID: "actor", WSID: "rejected", Path: "/"}
	if err := adapter.WebSocketOpen(context.Background(), nil, open, actorContext); err == nil {
		t.Fatal("OnConnect rejection was ignored")
	}
	if err := adapter.WebSocketMessage(context.Background(), nil, wire.Event{
		Kind: wire.EventWSMessage, WSID: "rejected", Data: []byte("ignored"),
	}, actorContext); err != nil {
		t.Fatal(err)
	}
	if err := adapter.WebSocketClose(context.Background(), nil, wire.Event{
		Kind: wire.EventWSClose, WSID: "rejected",
	}, actorContext); err != nil {
		t.Fatal(err)
	}
	if messageCalled || disconnectCalled {
		t.Fatalf("rejected connection ran later hooks: message=%t disconnect=%t", messageCalled, disconnectCalled)
	}
}

func TestWebSocketCloseValidation(t *testing.T) {
	for _, code := range []uint16{1000, 1001, 1007, 1013, 3000, 4999} {
		if !validCloseCode(code) {
			t.Fatalf("valid close code %d rejected", code)
		}
	}
	for _, code := range []uint16{999, 1004, 1005, 1006, 1015, 2999, 5000} {
		if validCloseCode(code) {
			t.Fatalf("invalid close code %d accepted", code)
		}
	}
}
