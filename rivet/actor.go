package rivet

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/ewhauser/rivet-go/internal/wire"
)

// Actor defines typed state, lifecycle hooks, actions, raw HTTP handling, and
// raw gateway WebSocket handling.
type Actor[T any] struct {
	// HibernateWebSockets keeps raw gateway WebSockets connected while this
	// actor sleeps. It is opt-in because the pinned engine acknowledges every
	// message on hibernatable connections, adding per-message latency.
	HibernateWebSockets bool
	OnStart             func(*Context[T]) error
	OnStop              func(*Context[T]) error
	OnAlarm             func(*Context[T]) error
	Actions             Actions[T]
	// OnFetch handles buffered HTTP requests from the pinned core. Response
	// headers lock on the first WriteHeader or Write, concurrent Write calls are
	// serialized, and writes after OnFetch returns fail. Header must not be
	// mutated concurrently with a write. The M3 writer intentionally does not
	// implement http.Flusher because v2.3.10 buffers the complete response.
	OnFetch   func(*Context[T], http.ResponseWriter, *http.Request)
	OnConnect func(*Context[T], *Connection) error
	OnMessage func(*Context[T], *Connection, Message)
	// OnDisconnect runs for a connection close observed while the actor is
	// live. Hibernation itself is transparent and does not invoke this hook.
	OnDisconnect func(*Context[T], *Connection)
}

// Context is one live actor generation. State returns the generation-local
// typed value loaded from core's persisted snapshot.
type Context[T any] struct {
	session       *pump.ActorSession
	state         T
	saveMu        sync.Mutex
	connectionsMu sync.Mutex
	connections   map[string]*Connection
}

func (c *Context[T]) State() *T {
	if c == nil {
		return nil
	}
	return &c.state
}

func (c *Context[T]) Input() []byte {
	if c == nil || c.session == nil {
		return nil
	}
	return c.session.Input()
}

func (c *Context[T]) ActorID() string {
	if c == nil || c.session == nil {
		return ""
	}
	return c.session.AID()
}

func (c *Context[T]) Generation() uint64 {
	if c == nil || c.session == nil {
		return 0
	}
	return c.session.Generation()
}

// Schedule replaces this actor's one durable alarm. The engine wakes a
// sleeping actor before invoking OnAlarm.
func (c *Context[T]) Schedule(at time.Time) error {
	if c == nil || c.session == nil {
		return errors.New("actor context is unavailable")
	}
	timestamp := at.UnixMilli()
	return c.session.SetAlarm(&timestamp)
}

// ScheduleAfter replaces this actor's one durable alarm relative to now.
func (c *Context[T]) ScheduleAfter(delay time.Duration) error {
	return c.Schedule(time.Now().Add(delay))
}

// ClearSchedule removes this actor's pending alarm, if any.
func (c *Context[T]) ClearSchedule() error {
	if c == nil || c.session == nil {
		return errors.New("actor context is unavailable")
	}
	return c.session.SetAlarm(nil)
}

// Sleep requests engine-managed eviction after the current handler and
// already accepted actor work complete. Hibernatable WebSockets stay open.
func (c *Context[T]) Sleep() error {
	if c == nil || c.session == nil {
		return errors.New("actor context is unavailable")
	}
	return c.session.Sleep()
}

// Save serializes the current complete state and waits until rivetkit-core has
// persisted it. State is JSON by default; BinaryMarshaler/BinaryUnmarshaler on
// T or *T override JSON for custom binary formats.
func (c *Context[T]) Save(ctx context.Context) error {
	if c == nil || c.session == nil {
		return errors.New("actor context is unavailable")
	}
	if ctx == nil {
		return errors.New("save context is nil")
	}
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	state, err := encodeState(&c.state)
	if err != nil {
		return err
	}
	if err := c.session.Save(ctx, state); err != nil {
		return fmt.Errorf("persist actor state: %w", err)
	}
	return nil
}

type actorAdapter[T any] struct {
	definition Actor[T]
}

func (a *actorAdapter[T]) actionNames() []string {
	names := make([]string, 0, len(a.definition.Actions))
	for name := range a.definition.Actions {
		names = append(names, name)
	}
	return names
}

func (a *actorAdapter[T]) hibernateWebSockets() bool {
	return a.definition.HibernateWebSockets
}

func (a *actorAdapter[T]) Start(
	_ context.Context,
	session *pump.ActorSession,
	_ wire.Event,
) (any, error) {
	state, err := decodeState[T](session.PersistedState())
	if err != nil {
		return nil, err
	}
	actorContext := &Context[T]{
		session:     session,
		state:       state,
		connections: make(map[string]*Connection),
	}
	if a.definition.OnStart != nil {
		if err := a.definition.OnStart(actorContext); err != nil {
			return nil, err
		}
	}
	return actorContext, nil
}

func (a *actorAdapter[T]) Stop(
	_ context.Context,
	_ *pump.ActorSession,
	_ wire.Event,
	state any,
) error {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return errors.New("typed actor context is unavailable during stop")
	}
	if a.definition.OnStop == nil {
		return nil
	}
	return a.definition.OnStop(actorContext)
}

func (a *actorAdapter[T]) Action(
	ctx context.Context,
	_ *pump.ActorSession,
	event wire.Event,
	state any,
) ([]byte, error) {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return nil, errors.New("typed actor context is unavailable during action")
	}
	action := a.definition.Actions[event.Action]
	if action == nil {
		return nil, pump.HandlerError{
			Code:    "action_not_found",
			Message: fmt.Sprintf("action %q is not registered", event.Action),
		}
	}
	output, err := action.invoke(ctx, actorContext, event.Args)
	if err != nil {
		return nil, err
	}
	if err := actorContext.Save(ctx); err != nil {
		return nil, pump.HandlerError{
			Code:    "action_state_persist_failed",
			Message: err.Error(),
		}
	}
	return output, nil
}

func (a *actorAdapter[T]) Alarm(
	ctx context.Context,
	_ *pump.ActorSession,
	_ wire.Event,
	state any,
) error {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return errors.New("typed actor context is unavailable during alarm")
	}
	if a.definition.OnAlarm == nil {
		return pump.HandlerError{Code: "callback_not_found", Message: "actor has no OnAlarm handler"}
	}
	if err := a.definition.OnAlarm(actorContext); err != nil {
		return err
	}
	if err := actorContext.Save(ctx); err != nil {
		return pump.HandlerError{
			Code:    "alarm_state_persist_failed",
			Message: err.Error(),
		}
	}
	return nil
}

func (a *actorAdapter[T]) Fetch(
	_ context.Context,
	session *pump.ActorSession,
	event wire.Event,
	state any,
) error {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return errors.New("typed actor context is unavailable during fetch")
	}
	if a.definition.OnFetch == nil {
		return pump.HandlerError{Code: "callback_not_found", Message: "actor has no OnFetch handler"}
	}
	incoming, err := session.HTTPRequest(event)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		incoming.Context,
		incoming.Method,
		incoming.Path,
		incoming.Body,
	)
	if err != nil {
		return fmt.Errorf("build actor HTTP request: %w", err)
	}
	request.RequestURI = incoming.Path
	for name, value := range incoming.Headers {
		if strings.EqualFold(name, "host") {
			request.Host = value
			continue
		}
		request.Header.Set(name, value)
	}
	writer := newResponseWriter(session, event.RequestID, incoming.Context)
	a.definition.OnFetch(actorContext, writer, request)
	return writer.finish()
}

func (a *actorAdapter[T]) WebSocketOpen(
	_ context.Context,
	session *pump.ActorSession,
	event wire.Event,
	state any,
) error {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return errors.New("typed actor context is unavailable during OnConnect")
	}
	connection := newConnection(session, event)
	actorContext.connectionsMu.Lock()
	if _, exists := actorContext.connections[event.WSID]; exists {
		actorContext.connectionsMu.Unlock()
		return pump.HandlerError{Code: "ws_duplicate", Message: "WebSocket connection is already active"}
	}
	actorContext.connections[event.WSID] = connection
	actorContext.connectionsMu.Unlock()
	if event.Resumed {
		return nil
	}
	if a.definition.OnConnect == nil {
		return nil
	}
	if err := a.definition.OnConnect(actorContext, connection); err != nil {
		actorContext.connectionsMu.Lock()
		delete(actorContext.connections, event.WSID)
		actorContext.connectionsMu.Unlock()
		connection.markClosed()
		return err
	}
	return nil
}

func (a *actorAdapter[T]) WebSocketMessage(
	_ context.Context,
	_ *pump.ActorSession,
	event wire.Event,
	state any,
) error {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return errors.New("typed actor context is unavailable during OnMessage")
	}
	actorContext.connectionsMu.Lock()
	connection := actorContext.connections[event.WSID]
	actorContext.connectionsMu.Unlock()
	if connection == nil {
		return nil
	}
	if a.definition.OnMessage != nil {
		a.definition.OnMessage(actorContext, connection, Message{
			Data:         append([]byte(nil), event.Data...),
			Binary:       event.Binary,
			MessageIndex: event.MessageIndex,
		})
	}
	return nil
}

func (a *actorAdapter[T]) WebSocketClose(
	_ context.Context,
	_ *pump.ActorSession,
	event wire.Event,
	state any,
) error {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return errors.New("typed actor context is unavailable during OnDisconnect")
	}
	actorContext.connectionsMu.Lock()
	connection := actorContext.connections[event.WSID]
	delete(actorContext.connections, event.WSID)
	actorContext.connectionsMu.Unlock()
	if connection == nil {
		return nil
	}
	connection.setClose(event.CloseCode, event.Reason)
	if a.definition.OnDisconnect != nil {
		a.definition.OnDisconnect(actorContext, connection)
	}
	return nil
}

func (a *actorAdapter[T]) CloseWebSockets(
	_ context.Context,
	_ *pump.ActorSession,
	state any,
	reason string,
) {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return
	}
	actorContext.connectionsMu.Lock()
	connections := make([]*Connection, 0, len(actorContext.connections))
	for id, connection := range actorContext.connections {
		delete(actorContext.connections, id)
		connection.setClose(nil, reason)
		connections = append(connections, connection)
	}
	actorContext.connectionsMu.Unlock()
	if reason == "sleep" {
		return
	}
	if a.definition.OnDisconnect != nil {
		for _, connection := range connections {
			func() {
				defer func() { _ = recover() }()
				a.definition.OnDisconnect(actorContext, connection)
			}()
		}
	}
}

func decodeState[T any](data []byte) (T, error) {
	var state T
	// A nil snapshot means core reported a first start. A non-nil, zero-length
	// snapshot is persisted data and must still reach a custom binary decoder.
	if data == nil {
		return state, nil
	}
	if unmarshaler, ok := any(&state).(encoding.BinaryUnmarshaler); ok {
		if err := unmarshaler.UnmarshalBinary(data); err != nil {
			return state, fmt.Errorf("decode actor binary state: %w", err)
		}
		return state, nil
	}
	if unmarshaler, ok := any(state).(encoding.BinaryUnmarshaler); ok {
		if err := unmarshaler.UnmarshalBinary(data); err != nil {
			return state, fmt.Errorf("decode actor binary state: %w", err)
		}
		return state, nil
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode actor JSON state: %w", err)
	}
	return state, nil
}

func encodeState[T any](state *T) ([]byte, error) {
	if marshaler, ok := any(state).(encoding.BinaryMarshaler); ok {
		data, err := marshaler.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("encode actor binary state: %w", err)
		}
		return append([]byte(nil), data...), nil
	}
	if marshaler, ok := any(*state).(encoding.BinaryMarshaler); ok {
		data, err := marshaler.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("encode actor binary state: %w", err)
		}
		return append([]byte(nil), data...), nil
	}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode actor JSON state: %w", err)
	}
	return data, nil
}
