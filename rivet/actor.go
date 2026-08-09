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

var (
	ErrActorUnavailable = errors.New("actor context is unavailable")
	ErrActorStarting    = errors.New("actor generation is starting")
	ErrActorStopping    = errors.New("actor generation is stopping")
)

// Actor defines typed state, lifecycle hooks, actions, raw HTTP handling, and
// raw gateway WebSocket handling.
type Actor[T any] struct {
	// Database provisions a durable per-actor SQLite database and enables
	// Context.DB for this actor. It is opt-in because database actors use the
	// pinned LocalNative backend, whose live-generation crash recovery differs
	// from the default remote state/KV path.
	Database bool
	// HibernateWebSockets keeps raw gateway WebSockets connected while this
	// actor sleeps. It is opt-in because the pinned engine acknowledges every
	// message on hibernatable connections. A recorded single-machine loopback
	// echo comparison observed about 1.8 ms higher client p50 with hibernation;
	// that magnitude is workload-specific and is not a network-latency estimate.
	HibernateWebSockets bool
	// ConnectionState defines typed state initialized for every ActorConnect
	// connection.
	ConnectionState ConnectionStateConfig[T]
	OnStart         func(*Context[T]) error
	OnStop          func(*Context[T]) error
	OnAlarm         func(*Context[T]) error
	// Run is generation-owned background execution. It runs once per wake,
	// cooperates with actor serialization through RunContext.Queue and
	// RunContext.KeepAwake, and must return when ctx is cancelled. An error is
	// logged and ends Run for the current generation without crashing the actor.
	Run     func(context.Context, *RunContext[T]) error
	Actions Actions[T]
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
	// OnActorConnect and OnActorDisconnect observe ActorConnect lifecycle.
	OnActorConnect    func(*Context[T], *Connection) error
	OnActorDisconnect func(*Context[T], *Connection)
}

// Context is one live actor generation. State returns the generation-local
// typed value loaded from core's persisted snapshot.
type Context[T any] struct {
	session             *pump.ActorSession
	client              *Client
	db                  *DB
	kv                  *KV
	schedules           *ActionSchedules
	queue               *Queue
	state               T
	turnMu              sync.Mutex
	saveMu              sync.Mutex
	connectionsMu       sync.Mutex
	connections         map[string]*Connection
	pendingConnections  map[string]*Connection
	currentConnectionMu sync.RWMutex
	currentConnection   *Connection
	lifecycleMu         sync.Mutex
	lifecycleStarted    bool
	lifecycleStopping   bool
	destroyRequested    bool
	managedCtx          context.Context
	managedCancel       context.CancelFunc
	managedMu           sync.Mutex
	managedStopping     bool
	managedWG           sync.WaitGroup
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

// Client returns an actor-scoped Engine client. The client inherits endpoint,
// namespace, runner name, authentication, headers, and HTTP transport from the
// serving Config. Calls to this actor generation fail with ErrSelfCall instead
// of waiting forever on the actor's serialized action queue.
func (c *Context[T]) Client() *Client {
	if c == nil {
		return nil
	}
	return c.client
}

// Name returns the registered name of this actor type.
func (c *Context[T]) Name() string {
	if c == nil || c.session == nil {
		return ""
	}
	return c.session.Name()
}

// Key returns the engine-formatted actor key, or an empty string for an
// unkeyed actor. Rivet Engine v2.3.10 does not preserve individual key
// segments at the Go boundary.
func (c *Context[T]) Key() string {
	if c == nil || c.session == nil {
		return ""
	}
	return c.session.Key()
}

func (c *Context[T]) Generation() uint64 {
	if c == nil || c.session == nil {
		return 0
	}
	return c.session.Generation()
}

// CurrentConnection returns the core connection that invoked the current
// action. ActorConnect calls expose their long-lived connection; gateway HTTP
// calls are intentionally absent. It is also nil for scheduled actions,
// alarms, lifecycle hooks, and work that has no ActorConnect caller.
func (c *Context[T]) CurrentConnection() *Connection {
	if c == nil {
		return nil
	}
	c.currentConnectionMu.RLock()
	connection := c.currentConnection
	c.currentConnectionMu.RUnlock()
	return connection
}

// DB returns this actor generation's SQLite handle. The handle is safe for
// concurrent use. Transactions remain generation-local and expire after their
// lease timeout. Operations return sqlite_transport_not_configured unless the
// actor declares Database and the runner transport is active.
func (c *Context[T]) DB() *DB {
	if c == nil {
		return nil
	}
	return c.db
}

// KV returns this actor generation's low-level key-value store.
//
// Deprecated: Prefer typed actor state or DB for new actors.
func (c *Context[T]) KV() *KV {
	if c == nil {
		return nil
	}
	return c.kv
}

// Schedules returns this actor generation's durable one-shot action scheduler.
// It is independent from the compatibility Schedule/OnAlarm API below.
func (c *Context[T]) Schedules() *ActionSchedules {
	if c == nil {
		return nil
	}
	return c.schedules
}

// Queue returns this actor generation's durable queue. Blocking waits called
// from an actor handler yield the actor turn so Actor.Run can consume messages.
func (c *Context[T]) Queue() *Queue {
	if c == nil {
		return nil
	}
	return c.queue
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
// already accepted actor work complete. Raw WebSockets stay open only when the
// actor opted in to HibernateWebSockets; default sockets close during sleep.
func (c *Context[T]) Sleep() error {
	if c == nil || c.session == nil {
		return errors.New("actor context is unavailable")
	}
	// Fence both SQLite transports before requesting eviction so admitted work
	// finishes and an open lease is rolled back instead of gating sleep until
	// its timeout.
	if c.db != nil {
		if err := c.db.closeForSleep(); err != nil {
			return fmt.Errorf("close actor SQLite transport for sleep: %w", err)
		}
	}
	return c.session.Sleep()
}

// Destroy permanently destroys this actor after the current callback response
// is sent and already accepted actor work completes. Unlike Sleep, a destroyed
// actor cannot be woken again. All generation-scoped SQLite and WebSocket
// handles are closed during teardown.
func (c *Context[T]) Destroy() error {
	if c == nil || c.session == nil {
		return ErrActorUnavailable
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if !c.lifecycleStarted {
		return ErrActorStarting
	}
	if c.lifecycleStopping || c.destroyRequested {
		return ErrActorStopping
	}
	// Fence SQLite before core accepts destruction. This drains admitted calls
	// and rolls back an open transaction lease instead of letting it hold the
	// generation alive until its deadline.
	if c.db != nil {
		if err := c.db.closeForDestroy(); err != nil {
			return fmt.Errorf("close actor SQLite transport for destroy: %w", err)
		}
	}
	if err := c.session.Destroy(); err != nil {
		return actorDestroyError(err)
	}
	c.destroyRequested = true
	return nil
}

func (c *Context[T]) destructionRequested() bool {
	if c == nil {
		return false
	}
	c.lifecycleMu.Lock()
	requested := c.destroyRequested
	c.lifecycleMu.Unlock()
	return requested
}

func actorDestroyError(err error) error {
	if errors.Is(err, pump.ErrShuttingDown) {
		return fmt.Errorf("actor runner is shutting down: %w", ErrActorStopping)
	}
	var handler pump.HandlerError
	if !errors.As(err, &handler) {
		return err
	}
	switch handler.Code {
	case "starting", "actor_starting":
		return fmt.Errorf("%s: %w", handler.Message, ErrActorStarting)
	case "stopping", "destroying", "actor_stopping", "actor_generation_stale":
		return fmt.Errorf("%s: %w", handler.Message, ErrActorStopping)
	default:
		return err
	}
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
	clientMu   sync.RWMutex
	client     *Client
}

func (a *actorAdapter[T]) setClient(client *Client) {
	a.clientMu.Lock()
	a.client = client
	a.clientMu.Unlock()
}

func (a *actorAdapter[T]) clientForActor(actorID string) *Client {
	a.clientMu.RLock()
	client := a.client
	a.clientMu.RUnlock()
	return client.withSourceActor(actorID)
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

func (a *actorAdapter[T]) database() bool {
	return a.definition.Database
}

func (a *actorAdapter[T]) Start(
	ctx context.Context,
	session *pump.ActorSession,
	event wire.Event,
) (any, error) {
	state, err := decodeState[T](session.PersistedState())
	if err != nil {
		return nil, err
	}
	db, err := newDB(session, a.definition.Database)
	if err != nil {
		return nil, err
	}
	managedCtx, managedCancel := context.WithCancel(ctx)
	actorContext := &Context[T]{
		session:            session,
		client:             a.clientForActor(session.AID()),
		db:                 db,
		kv:                 newKV(session),
		schedules:          newActionSchedules(session, a.definition.Actions),
		queue:              newQueue(session),
		state:              state,
		connections:        make(map[string]*Connection),
		pendingConnections: make(map[string]*Connection),
		managedCtx:         managedCtx,
		managedCancel:      managedCancel,
	}
	for _, snapshot := range event.Connections {
		connection := newActorConnection(session, snapshot)
		if snapshot.ActorConnect && a.definition.ConnectionState != nil {
			connectionState, decodeErr := a.definition.ConnectionState.decode(snapshot.State)
			if decodeErr != nil {
				actorContext.managedCancel()
				_ = actorContext.db.close()
				return nil, fmt.Errorf("restore connection %q state: %w", snapshot.ID, decodeErr)
			}
			connection.state = connectionState
		}
		actorContext.connections[snapshot.ID] = connection
	}
	if a.definition.OnStart != nil {
		if err := a.definition.OnStart(actorContext); err != nil {
			actorContext.managedCancel()
			_ = actorContext.db.close()
			return nil, err
		}
	}
	actorContext.queue = actorContext.queue.forRun(actorContext.turnMu.Unlock, actorContext.turnMu.Lock)
	actorContext.lifecycleMu.Lock()
	actorContext.lifecycleStarted = true
	actorContext.lifecycleMu.Unlock()
	return actorContext, nil
}

func (a *actorAdapter[T]) Run(
	ctx context.Context,
	_ *pump.ActorSession,
	state any,
) error {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return errors.New("typed actor context is unavailable during Run")
	}
	if a.definition.Run == nil {
		return nil
	}
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	runContext := &RunContext[T]{Context: actorContext}
	runContext.queue = actorContext.queue
	err := a.definition.Run(ctx, runContext)
	if errors.Is(err, ErrActorAborted) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (a *actorAdapter[T]) RunEnabled() bool {
	return a != nil && a.definition.Run != nil
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
	actorContext.lifecycleMu.Lock()
	actorContext.lifecycleStopping = true
	actorContext.lifecycleMu.Unlock()
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	var stopErr error
	if a.definition.OnStop != nil {
		stopErr = a.definition.OnStop(actorContext)
	}
	if err := actorContext.drainManaged(); err != nil {
		actorContext.managedCancel()
		return err
	}
	actorContext.managedCancel()
	closeErr := actorContext.db.close()
	if stopErr != nil {
		return stopErr
	}
	return closeErr
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
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	action := a.definition.Actions[event.Action]
	if action == nil {
		return nil, pump.HandlerError{
			Code:    "action_not_found",
			Message: fmt.Sprintf("action %q is not registered", event.Action),
		}
	}
	var current *Connection
	if event.ConnID != nil {
		actorContext.connectionsMu.Lock()
		current = actorContext.connections[*event.ConnID]
		actorContext.connectionsMu.Unlock()
		if current == nil {
			return nil, pump.HandlerError{
				Code:    "connection_not_found",
				Message: fmt.Sprintf("calling connection %q is not active", *event.ConnID),
			}
		}
		if !current.actorConnect {
			current = nil
		}
	}
	actorContext.currentConnectionMu.Lock()
	actorContext.currentConnection = current
	actorContext.currentConnectionMu.Unlock()
	defer func() {
		actorContext.currentConnectionMu.Lock()
		actorContext.currentConnection = nil
		actorContext.currentConnectionMu.Unlock()
	}()
	output, err := action.invoke(ctx, actorContext, event.Args)
	if err != nil {
		return nil, err
	}
	if !actorContext.destructionRequested() {
		if err := actorContext.Save(ctx); err != nil {
			return nil, pump.HandlerError{
				Code:    "action_state_persist_failed",
				Message: err.Error(),
			}
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
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	if a.definition.OnAlarm == nil {
		return pump.HandlerError{Code: "callback_not_found", Message: "actor has no OnAlarm handler"}
	}
	if err := a.definition.OnAlarm(actorContext); err != nil {
		return err
	}
	if !actorContext.destructionRequested() {
		if err := actorContext.Save(ctx); err != nil {
			return pump.HandlerError{
				Code:    "alarm_state_persist_failed",
				Message: err.Error(),
			}
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
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
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

func (a *actorAdapter[T]) ConnectionPreflight(
	_ context.Context,
	session *pump.ActorSession,
	event wire.Event,
	state any,
) ([]byte, error) {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return nil, errors.New("typed actor context is unavailable during connection preflight")
	}
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	if event.Connection == nil {
		return nil, errors.New("connection preflight snapshot is unavailable")
	}
	connection := newActorConnection(session, *event.Connection)
	if connection.actorConnect && a.definition.ConnectionState != nil {
		connectionState, err := a.definition.ConnectionState.initialize(actorContext, connection)
		if err != nil {
			return nil, err
		}
		connection.state = connectionState
	}
	encoded, err := a.encodeConnectionState(connection)
	if err != nil {
		return nil, err
	}
	connection.encodedState = append([]byte(nil), encoded...)
	actorContext.connectionsMu.Lock()
	if _, exists := actorContext.connections[connection.ID()]; exists {
		actorContext.connectionsMu.Unlock()
		return nil, pump.HandlerError{Code: "connection_duplicate", Message: "connection is already active"}
	}
	if _, exists := actorContext.pendingConnections[connection.ID()]; exists {
		actorContext.connectionsMu.Unlock()
		return nil, pump.HandlerError{Code: "connection_duplicate", Message: "connection preflight is already pending"}
	}
	actorContext.pendingConnections[connection.ID()] = connection
	actorContext.connectionsMu.Unlock()
	return encoded, nil
}

func (a *actorAdapter[T]) ConnectionOpen(
	_ context.Context,
	session *pump.ActorSession,
	event wire.Event,
	state any,
) ([]byte, error) {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return nil, errors.New("typed actor context is unavailable during connection open")
	}
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	if event.Connection == nil {
		return nil, errors.New("connection open snapshot is unavailable")
	}
	actorContext.connectionsMu.Lock()
	connection := actorContext.pendingConnections[event.Connection.ID]
	delete(actorContext.pendingConnections, event.Connection.ID)
	if connection == nil {
		connection = newActorConnection(session, *event.Connection)
		if connection.actorConnect && a.definition.ConnectionState != nil {
			connectionState, err := a.definition.ConnectionState.decode(event.Connection.State)
			if err != nil {
				actorContext.connectionsMu.Unlock()
				return nil, err
			}
			connection.state = connectionState
		}
	}
	actorContext.connections[connection.ID()] = connection
	actorContext.connectionsMu.Unlock()

	if connection.actorConnect && a.definition.OnActorConnect != nil {
		if err := a.definition.OnActorConnect(actorContext, connection); err != nil {
			actorContext.connectionsMu.Lock()
			delete(actorContext.connections, connection.ID())
			actorContext.connectionsMu.Unlock()
			connection.markClosed()
			return nil, err
		}
	}
	return a.encodeConnectionState(connection)
}

func (a *actorAdapter[T]) ConnectionClose(
	_ context.Context,
	_ *pump.ActorSession,
	event wire.Event,
	state any,
) ([]byte, error) {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return nil, errors.New("typed actor context is unavailable during connection close")
	}
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	if event.Connection == nil {
		return nil, errors.New("connection close snapshot is unavailable")
	}
	actorContext.connectionsMu.Lock()
	connection := actorContext.connections[event.Connection.ID]
	delete(actorContext.connections, event.Connection.ID)
	delete(actorContext.pendingConnections, event.Connection.ID)
	actorContext.connectionsMu.Unlock()
	if connection == nil {
		connection = newActorConnection(actorContext.session, *event.Connection)
		if connection.actorConnect && a.definition.ConnectionState != nil {
			connectionState, err := a.definition.ConnectionState.decode(event.Connection.State)
			if err != nil {
				return nil, err
			}
			connection.state = connectionState
		}
	}
	connection.markClosed()
	if connection.actorConnect && a.definition.OnActorDisconnect != nil {
		a.definition.OnActorDisconnect(actorContext, connection)
	}
	return a.encodeConnectionState(connection)
}

func (a *actorAdapter[T]) ConnectionState(
	_ context.Context,
	_ *pump.ActorSession,
	connectionID string,
	state any,
) ([]byte, bool, error) {
	actorContext, ok := state.(*Context[T])
	if !ok || actorContext == nil {
		return nil, false, errors.New("typed actor context is unavailable while encoding connection state")
	}
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	actorContext.connectionsMu.Lock()
	connection := actorContext.connections[connectionID]
	if connection == nil {
		connection = actorContext.pendingConnections[connectionID]
	}
	actorContext.connectionsMu.Unlock()
	if connection == nil {
		return nil, false, nil
	}
	if !connection.actorConnect {
		return nil, false, nil
	}
	encoded, err := a.encodeConnectionState(connection)
	return encoded, true, err
}

func (a *actorAdapter[T]) encodeConnectionState(connection *Connection) ([]byte, error) {
	if connection == nil {
		return nil, errors.New("connection is unavailable")
	}
	connection.stateMu.Lock()
	defer connection.stateMu.Unlock()
	if !connection.actorConnect || a.definition.ConnectionState == nil {
		return append([]byte(nil), connection.encodedState...), nil
	}
	encoded, err := a.definition.ConnectionState.encode(connection.state)
	if err != nil {
		return nil, err
	}
	connection.encodedState = append(connection.encodedState[:0], encoded...)
	return append([]byte(nil), encoded...), nil
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
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
	actorContext.connectionsMu.Lock()
	connection := actorContext.connections[event.WSID]
	if connection == nil {
		connection = newConnection(session, event)
		actorContext.connections[event.WSID] = connection
	} else {
		connection.updateWebSocketMetadata(event)
	}
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
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
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
	actorContext.turnMu.Lock()
	defer actorContext.turnMu.Unlock()
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
		if connection.rawWebSocket {
			connections = append(connections, connection)
		}
	}
	for id, connection := range actorContext.pendingConnections {
		delete(actorContext.pendingConnections, id)
		connection.setClose(nil, reason)
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
