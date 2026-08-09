// Package pump turns the blocking native event pump into Go subscriptions,
// per-actor dispatch, and a concurrent batched submit API.
package pump

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ewhauser/rivet-go/internal/wire"
)

const (
	defaultPollTimeout     = 100 * time.Millisecond
	defaultShutdownTimeout = 10 * time.Second
	defaultSaveTimeout     = 35 * time.Second
	defaultIntentTimeout   = 35 * time.Second
	defaultHTTPSubmitLimit = 30 * time.Second
	defaultWSSubmitLimit   = 30 * time.Second
	defaultSQLiteTimeout   = 65 * time.Second
	defaultSubmitQueue     = 1_024
	maxSubmitBatch         = 1_024
	maxHTTPChunk           = 1 << 20
	maxWSMessage           = 1 << 20
	maxSQLiteResponseBytes = 32 * 1024 * 1024
)

var (
	ErrAlreadyStarted = errors.New("pump already started")
	ErrNotStarted     = errors.New("pump is not running")
	ErrShuttingDown   = errors.New("pump is shutting down")
)

const (
	metricEventsPolled      = "events_polled_total"
	metricEventBatches      = "event_batches_polled_total"
	metricCommandsSubmitted = "commands_submitted_total"
	metricSubmitBatches     = "submit_batches_total"
	metricBackpressureHits  = "backpressure_hits_total"
	metricActorStarts       = "actor_starts_total"
	metricActorStops        = "actor_stops_total"
	metricActorPanics       = "actor_panics_total"
	metricLiveActors        = "live_actors"
	metricLiveConnections   = "live_connections"
	metricPollLatency       = "poll_latency"
)

// Hooks is the dependency-free metrics sink used by the public SDK. Calls are
// serialized on a dispatcher outside runtime-critical goroutines.
type Hooks interface {
	Counter(string, int64)
	Gauge(string, int64)
	ObserveDuration(string, time.Duration)
}

// Options configures pump operability and drain behavior.
type Options struct {
	Hooks           Hooks
	Logger          *slog.Logger
	ShutdownTimeout time.Duration
	SQLiteTransport string
}

// Runner is implemented by the owned internal/ffi runner handle.
type Runner interface {
	Poll(time.Duration) ([]byte, error)
	Submit([]byte) error
	Shutdown(time.Duration) error
	Close()
}

// ActorHandler is the type-erased lifecycle adapter implemented by rivet's
// typed actor registrations. State returned from Start belongs to one actor
// generation and is passed back to Stop on that same actor goroutine.
type ActorHandler interface {
	Start(context.Context, *ActorSession, wire.Event) (any, error)
	Stop(context.Context, *ActorSession, wire.Event, any) error
}

type actorActionHandler interface {
	Action(context.Context, *ActorSession, wire.Event, any) ([]byte, error)
}

type actorConnectionHandler interface {
	ConnectionPreflight(context.Context, *ActorSession, wire.Event, any) ([]byte, error)
	ConnectionOpen(context.Context, *ActorSession, wire.Event, any) ([]byte, error)
	ConnectionClose(context.Context, *ActorSession, wire.Event, any) ([]byte, error)
	ConnectionState(context.Context, *ActorSession, string, any) ([]byte, bool, error)
}

type actorAlarmHandler interface {
	Alarm(context.Context, *ActorSession, wire.Event, any) error
}

type actorFetchHandler interface {
	Fetch(context.Context, *ActorSession, wire.Event, any) error
}

type actorWebSocketHandler interface {
	WebSocketOpen(context.Context, *ActorSession, wire.Event, any) error
	WebSocketMessage(context.Context, *ActorSession, wire.Event, any) error
	WebSocketClose(context.Context, *ActorSession, wire.Event, any) error
	CloseWebSockets(context.Context, *ActorSession, any, string)
}

// HandlerError is sent to Rust as the structured actor-local error arm of a
// lifecycle result. It never terminates the shared pump.
type HandlerError struct {
	Code    string
	Message string
	Cause   error
}

func (e HandlerError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

func (e HandlerError) Unwrap() error { return e.Cause }

// ActionCode preserves actor-local structured errors when they pass through a
// public action handler before the pump serializes them onto the wire.
func (e HandlerError) ActionCode() string { return e.Code }

type actorIdentity struct {
	aid        string
	generation uint64
}

type submitRequest struct {
	commands []wire.Command
	result   chan error
}

type subscriber struct {
	mu     sync.Mutex
	closed bool
	events chan wire.Event
}

// Subscription receives every event after it is created. Cancel is safe from
// any goroutine and closes Events.
type Subscription struct {
	Events <-chan wire.Event
	cancel func()
	once   sync.Once
}

func (s *Subscription) Cancel() {
	if s != nil && s.cancel != nil {
		s.once.Do(s.cancel)
	}
}

// ActorSession exposes actor-scoped core operations to a registered handler.
// A session is valid for exactly one aid/generation pair.
type ActorSession struct {
	pump           *Pump
	identity       actorIdentity
	name           string
	key            string
	input          []byte
	persistedState []byte
	done           chan struct{}

	saveMu       sync.Mutex
	saveResult   chan error
	savePoisoned bool
	saveStateMu  sync.Mutex

	lifecycleMu      sync.Mutex
	lifecycleCond    *sync.Cond
	accepting        bool
	activeSaves      int
	activeSQLite     int
	doneClosed       bool
	sqliteTransport  string
	sqliteSocketPath string
}

// HTTPRequest is one raw actor request delivered through the engine gateway.
// Body receives boundary chunks without blocking the pump goroutine.
type HTTPRequest struct {
	Context context.Context
	Method  string
	Path    string
	Headers map[string]string
	Body    io.ReadCloser
}

type SQLiteResponse struct {
	Columns      []string
	Values       []wire.SQLiteValue
	RowsAffected int64
	LastInsertID *int64
}

// ScheduledEvent is one pending durable action schedule returned by core.
// Args contains the action's CBOR argument array.
type ScheduledEvent struct {
	ID     string
	Action string
	Args   []byte
	RunAt  int64
}

type sqliteResponseAssembler struct {
	response   SQLiteResponse
	nextChunk  uint32
	totalBytes int
}

type httpRequestState struct {
	identity actorIdentity
	request  HTTPRequest
	cancel   context.CancelCauseFunc
	body     *httpRequestBody
}

type httpRequestBody struct {
	ctx      context.Context
	mu       sync.Mutex
	chunks   [][]byte
	offset   int
	finished bool
	err      error
	closed   bool
	wake     chan struct{}
}

func (s *ActorSession) AID() string {
	if s == nil {
		return ""
	}
	return s.identity.aid
}

func (s *ActorSession) Generation() uint64 {
	if s == nil {
		return 0
	}
	return s.identity.generation
}

func (s *ActorSession) Name() string {
	if s == nil {
		return ""
	}
	return s.name
}

func (s *ActorSession) Key() string {
	if s == nil {
		return ""
	}
	return s.key
}

func (s *ActorSession) Input() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.input...)
}

func (s *ActorSession) PersistedState() []byte {
	if s == nil {
		return nil
	}
	return cloneBytes(s.persistedState)
}

func (s *ActorSession) SQLiteTransport() string {
	if s == nil {
		return ""
	}
	return s.sqliteTransport
}

func (s *ActorSession) SQLiteSocketPath() string {
	if s == nil {
		return ""
	}
	return s.sqliteSocketPath
}

// HTTPRequest returns the request state prepared for an HttpRequest event.
func (s *ActorSession) HTTPRequest(event wire.Event) (*HTTPRequest, error) {
	if s == nil || s.pump == nil {
		return nil, errors.New("actor session is unavailable")
	}
	state := s.pump.httpRequest(event.RequestID)
	if state == nil || state.identity != s.identity {
		return nil, fmt.Errorf("HTTP request %d is unavailable for this actor", event.RequestID)
	}
	request := state.request
	request.Headers = cloneStringMap(request.Headers)
	return &request, nil
}

// StartHTTPResponse submits response metadata. Streaming responses are
// completed by one or more WriteHTTPResponseChunk calls.
func (s *ActorSession) StartHTTPResponse(
	ctx context.Context,
	requestID uint64,
	status uint16,
	headers map[string]string,
	body []byte,
	stream bool,
) error {
	return s.submitHTTP(ctx, wire.Command{
		Kind:      wire.CommandHTTPResponseStart,
		RequestID: requestID,
		Status:    status,
		Headers:   cloneStringMapOrEmpty(headers),
		Body:      cloneBytesOrEmpty(body),
		Stream:    stream,
	})
}

// WriteHTTPResponseChunk submits at most one 1 MiB response chunk.
func (s *ActorSession) WriteHTTPResponseChunk(
	ctx context.Context,
	requestID uint64,
	body []byte,
	finish bool,
) error {
	if len(body) > maxHTTPChunk {
		return fmt.Errorf("HTTP response chunk is %d bytes, maximum is %d", len(body), maxHTTPChunk)
	}
	return s.submitHTTP(ctx, wire.Command{
		Kind:      wire.CommandHTTPResponseChunk,
		RequestID: requestID,
		Body:      cloneBytesOrEmpty(body),
		Finish:    finish,
	})
}

// SendWebSocket submits one complete text or binary WebSocket message. A
// WebSocket message is never split into multiple frames; the boundary maximum
// is one MiB.
func (s *ActorSession) SendWebSocket(
	ctx context.Context,
	wsID string,
	data []byte,
	binary bool,
) error {
	if wsID == "" {
		return errors.New("WebSocket ID is empty")
	}
	if len(data) > maxWSMessage {
		return fmt.Errorf("WebSocket message is %d bytes, maximum is %d", len(data), maxWSMessage)
	}
	return s.submitWebSocket(ctx, wire.Command{
		Kind:   wire.CommandWSSend,
		WSID:   wsID,
		Data:   cloneBytesOrEmpty(data),
		Binary: binary,
	})
}

// CloseWebSocket asks core to close one connection. Actor-managed hibernation
// is driven by Sleep rather than exposed as a connection close option.
func (s *ActorSession) CloseWebSocket(
	ctx context.Context,
	wsID string,
	code uint16,
	reason string,
) error {
	if wsID == "" {
		return errors.New("WebSocket ID is empty")
	}
	return s.submitWebSocket(ctx, wire.Command{
		Kind:        wire.CommandWSClose,
		WSID:        wsID,
		CloseCode:   &code,
		CloseReason: &reason,
		Hibernate:   false,
	})
}

// Broadcast sends one named CBOR argument array to every live connection on
// this actor, optionally excluding one raw WebSocket connection.
func (s *ActorSession) Broadcast(
	ctx context.Context,
	event string,
	payload []byte,
	excludeConn *string,
) error {
	if event == "" {
		return errors.New("broadcast event is empty")
	}
	if len(payload) > maxWSMessage {
		return fmt.Errorf("broadcast payload is %d bytes, maximum is %d", len(payload), maxWSMessage)
	}
	return s.submitWebSocket(ctx, wire.Command{
		Kind:        wire.CommandBroadcast,
		AID:         s.AID(),
		Event:       event,
		Payload:     cloneBytesOrEmpty(payload),
		ExcludeConn: excludeConn,
	})
}

// SetAlarm replaces the actor's one durable Go alarm. A nil timestamp clears
// the schedule. The core schedule table and engine alarm transport own
// durability and wake-up delivery.
func (s *ActorSession) SetAlarm(alarmTS *int64) error {
	if s == nil || s.pump == nil {
		return errors.New("actor session is unavailable")
	}
	if !s.isAccepting() {
		return ErrShuttingDown
	}
	var timestamp *int64
	if alarmTS != nil {
		value := *alarmTS
		timestamp = &value
	}
	return s.submitActorIntent(wire.Command{
		Kind:    wire.CommandSetAlarm,
		AlarmTS: timestamp,
	})
}

// ScheduleAfter creates a durable one-shot action schedule relative to core's
// current time and waits for the exact actor generation to acknowledge it.
func (s *ActorSession) ScheduleAfter(
	ctx context.Context,
	delay time.Duration,
	action string,
	args []byte,
) (string, error) {
	if delay < 0 {
		return "", errors.New("schedule delay is negative")
	}
	event, err := s.submitActorOperation(ctx, wire.Command{
		Kind:         wire.CommandScheduleAfter,
		DelayMS:      uint64(delay / time.Millisecond),
		Action:       action,
		ScheduleArgs: cloneBytesOrEmpty(args),
	})
	if err != nil {
		return "", err
	}
	if event.ScheduleOperation != "create" || event.ScheduleID == "" {
		return "", HandlerError{Code: "schedule_result_invalid", Message: "native create result is incomplete"}
	}
	return event.ScheduleID, nil
}

// ScheduleAt creates a durable one-shot action schedule at a Unix millisecond
// timestamp and waits for the exact actor generation to acknowledge it.
func (s *ActorSession) ScheduleAt(
	ctx context.Context,
	runAt int64,
	action string,
	args []byte,
) (string, error) {
	event, err := s.submitActorOperation(ctx, wire.Command{
		Kind:         wire.CommandScheduleAt,
		RunAt:        runAt,
		Action:       action,
		ScheduleArgs: cloneBytesOrEmpty(args),
	})
	if err != nil {
		return "", err
	}
	if event.ScheduleOperation != "create" || event.ScheduleID == "" {
		return "", HandlerError{Code: "schedule_result_invalid", Message: "native create result is incomplete"}
	}
	return event.ScheduleID, nil
}

func (s *ActorSession) CancelSchedule(ctx context.Context, scheduleID string) (bool, error) {
	event, err := s.submitActorOperation(ctx, wire.Command{
		Kind:       wire.CommandScheduleCancel,
		ScheduleID: scheduleID,
	})
	if err != nil {
		return false, err
	}
	if event.ScheduleOperation != "cancel" || event.Cancelled == nil {
		return false, HandlerError{Code: "schedule_result_invalid", Message: "native cancel result is incomplete"}
	}
	return *event.Cancelled, nil
}

func (s *ActorSession) GetSchedule(
	ctx context.Context,
	scheduleID string,
) (ScheduledEvent, bool, error) {
	event, err := s.submitActorOperation(ctx, wire.Command{
		Kind:       wire.CommandScheduleGet,
		ScheduleID: scheduleID,
	})
	if err != nil {
		return ScheduledEvent{}, false, err
	}
	if event.ScheduleOperation != "get" || len(event.Schedules) > 1 {
		return ScheduledEvent{}, false, HandlerError{Code: "schedule_result_invalid", Message: "native get result is invalid"}
	}
	if len(event.Schedules) == 0 {
		return ScheduledEvent{}, false, nil
	}
	return scheduledEvent(event.Schedules[0]), true, nil
}

func (s *ActorSession) ListSchedules(ctx context.Context) ([]ScheduledEvent, error) {
	event, err := s.submitActorOperation(ctx, wire.Command{Kind: wire.CommandScheduleList})
	if err != nil {
		return nil, err
	}
	if event.ScheduleOperation != "list" {
		return nil, HandlerError{Code: "schedule_result_invalid", Message: "native list result is invalid"}
	}
	schedules := make([]ScheduledEvent, len(event.Schedules))
	for index, event := range event.Schedules {
		schedules[index] = scheduledEvent(event)
	}
	return schedules, nil
}

func scheduledEvent(event wire.ScheduledEvent) ScheduledEvent {
	return ScheduledEvent{
		ID:     event.ID,
		Action: event.Action,
		Args:   cloneBytesOrEmpty(event.Args),
		RunAt:  event.RunAt,
	}
}

// Sleep requests engine-managed eviction after already accepted actor work
// has drained. Hibernatable gateway WebSockets are detached without a
// disconnect and restored by core in the next generation.
func (s *ActorSession) Sleep() error {
	if s == nil || s.pump == nil {
		return errors.New("actor session is unavailable")
	}
	if !s.isAccepting() {
		return ErrShuttingDown
	}
	return s.submitActorIntent(wire.Command{
		Kind: wire.CommandSleepIntent,
	})
}

func (s *ActorSession) submitActorIntent(command wire.Command) error {
	_, err := s.submitActorOperation(context.Background(), command)
	return err
}

func (s *ActorSession) submitActorOperation(ctx context.Context, command wire.Command) (wire.Event, error) {
	if s == nil || s.pump == nil {
		return wire.Event{}, errors.New("actor session is unavailable")
	}
	if ctx == nil {
		return wire.Event{}, errors.New("actor operation context is nil")
	}
	if err := ctx.Err(); err != nil {
		return wire.Event{}, err
	}
	if !s.isAccepting() {
		return wire.Event{}, ErrShuttingDown
	}
	result := make(chan wire.Event, 1)
	operationID := s.pump.addActorIntentWaiter(result)
	command.OperationID = operationID
	command.AID = s.identity.aid
	command.Generation = s.identity.generation
	if err := s.pump.submitInternal(ctx, command); err != nil {
		s.pump.removeActorIntentWaiter(operationID)
		return wire.Event{}, err
	}
	timer := time.NewTimer(s.pump.intentTimeout)
	defer timer.Stop()
	select {
	case event := <-result:
		if event.Error != nil {
			return wire.Event{}, HandlerError{
				Code:    event.Error.Code,
				Message: event.Error.Message,
				Cause:   *event.Error,
			}
		}
		return event, nil
	case <-ctx.Done():
		s.pump.abandonActorIntentWaiter(operationID)
		return wire.Event{}, ctx.Err()
	case <-timer.C:
		s.pump.abandonActorIntentWaiter(operationID)
		return wire.Event{}, HandlerError{
			Code:    "actor_intent_timeout",
			Message: fmt.Sprintf("actor intent was not acknowledged within %s", s.pump.intentTimeout),
		}
	case <-s.done:
		s.pump.abandonActorIntentWaiter(operationID)
		return wire.Event{}, ErrShuttingDown
	case <-s.pump.done:
		s.pump.abandonActorIntentWaiter(operationID)
		return wire.Event{}, ErrShuttingDown
	}
}

func (s *ActorSession) submitWebSocket(ctx context.Context, command wire.Command) error {
	if s == nil || s.pump == nil {
		return errors.New("actor session is unavailable")
	}
	if ctx == nil {
		return errors.New("WebSocket submission context is nil")
	}
	retryCtx, cancel := context.WithTimeout(ctx, s.pump.wsSubmitLimit)
	defer cancel()
	backoff := time.Millisecond
	for {
		err := s.pump.submitInternal(retryCtx, command)
		if err == nil {
			return nil
		}
		if retryCtx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return HandlerError{
				Code:    "ws_backpressure_timeout",
				Message: fmt.Sprintf("WebSocket submission remained backpressured for %s", s.pump.wsSubmitLimit),
			}
		}
		var coded interface{ ErrorCode() string }
		if !errors.As(err, &coded) || coded.ErrorCode() != "backpressure" {
			return err
		}
		jitter := time.Duration(rand.Int64N(int64(backoff/2) + 1))
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-timer.C:
		case <-retryCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return HandlerError{
				Code:    "ws_backpressure_timeout",
				Message: fmt.Sprintf("WebSocket submission remained backpressured for %s", s.pump.wsSubmitLimit),
			}
		case <-s.done:
			if !timer.Stop() {
				<-timer.C
			}
			return ErrShuttingDown
		case <-s.pump.done:
			if !timer.Stop() {
				<-timer.C
			}
			return ErrShuttingDown
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
}

func (s *ActorSession) failHTTP(ctx context.Context, requestID uint64, failure *wire.WireError) error {
	return s.submitHTTP(ctx, wire.Command{
		Kind:      wire.CommandHTTPResponseStart,
		RequestID: requestID,
		Headers:   map[string]string{},
		Body:      []byte{},
		Error:     failure,
	})
}

func (s *ActorSession) submitHTTP(ctx context.Context, command wire.Command) error {
	if s == nil || s.pump == nil {
		return errors.New("actor session is unavailable")
	}
	if ctx == nil {
		return errors.New("HTTP response context is nil")
	}
	retryCtx, cancel := context.WithTimeout(ctx, s.pump.httpSubmitLimit)
	defer cancel()
	backoff := time.Millisecond
	for {
		err := s.pump.submitInternal(retryCtx, command)
		if err == nil {
			return nil
		}
		if retryCtx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return HandlerError{
				Code: "http_response_backpressure_timeout",
				Message: fmt.Sprintf(
					"HTTP response submission remained backpressured for %s",
					s.pump.httpSubmitLimit,
				),
			}
		}
		var coded interface{ ErrorCode() string }
		if !errors.As(err, &coded) || coded.ErrorCode() != "backpressure" {
			return err
		}
		jitter := time.Duration(rand.Int64N(int64(backoff/2) + 1))
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-timer.C:
		case <-retryCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return HandlerError{
				Code: "http_response_backpressure_timeout",
				Message: fmt.Sprintf(
					"HTTP response submission remained backpressured for %s",
					s.pump.httpSubmitLimit,
				),
			}
		case <-s.done:
			if !timer.Stop() {
				<-timer.C
			}
			return ErrShuttingDown
		case <-s.pump.done:
			if !timer.Stop() {
				<-timer.C
			}
			return ErrShuttingDown
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
}

// Save persists one complete actor-state snapshot through rivetkit-core and
// waits for StatePersisted. If the caller stops waiting after enqueue, the
// session rejects another save until the late acknowledgement is consumed, so
// it cannot be mistaken for a later save on the same actor.
func (s *ActorSession) Save(ctx context.Context, state []byte) error {
	if s == nil || s.pump == nil {
		return errors.New("actor session is unavailable")
	}
	if ctx == nil {
		return errors.New("save context is nil")
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	if err := s.beginSave(); err != nil {
		return err
	}
	defer s.finishSave()

	select {
	case <-ctx.Done():
		return saveContextError(ctx.Err())
	default:
	}

	result := make(chan error, 1)
	s.saveStateMu.Lock()
	if s.savePoisoned {
		s.saveStateMu.Unlock()
		return HandlerError{
			Code:    "state_persist_unresolved",
			Message: "a previous actor state save completed without its acknowledgement",
		}
	}
	if s.saveResult != nil {
		s.saveStateMu.Unlock()
		return errors.New("actor save is already pending")
	}
	s.saveResult = result
	s.saveStateMu.Unlock()

	err := s.pump.submitInternal(context.Background(), wire.Command{
		Kind:       wire.CommandSaveState,
		AID:        s.identity.aid,
		Generation: s.identity.generation,
		State:      append([]byte(nil), state...),
	})
	if err != nil {
		s.clearSaveResult(result)
		return err
	}

	timer := time.NewTimer(s.pump.saveTimeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		s.abandonSave(result)
		return saveContextError(ctx.Err())
	case <-timer.C:
		s.abandonSave(result)
		return HandlerError{
			Code:    "state_persist_timeout",
			Message: fmt.Sprintf("actor state save was not acknowledged within %s", s.pump.saveTimeout),
		}
	case <-s.done:
		return ErrShuttingDown
	case <-s.pump.done:
		return ErrShuttingDown
	}
}

// KVGet performs an actor-scoped core KV lookup. The public package wraps
// these byte-oriented operations without exposing wire types.
func (s *ActorSession) KVGet(ctx context.Context, key []byte) ([]byte, bool, error) {
	result, err := s.kv(ctx, wire.Command{
		Kind: wire.CommandKVGet,
		AID:  s.AID(),
		Key:  append([]byte(nil), key...),
	})
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), result.Value...), result.Value != nil, nil
}

func (s *ActorSession) KVList(
	ctx context.Context,
	prefix []byte,
	reverse bool,
	limit *uint32,
) ([]wire.KVEntry, error) {
	result, err := s.kv(ctx, wire.Command{
		Kind:    wire.CommandKVList,
		AID:     s.AID(),
		Prefix:  append([]byte(nil), prefix...),
		Reverse: reverse,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	entries := make([]wire.KVEntry, len(result.Entries))
	for i, entry := range result.Entries {
		entries[i] = wire.KVEntry{
			Key:   append([]byte(nil), entry.Key...),
			Value: append([]byte(nil), entry.Value...),
		}
	}
	return entries, nil
}

func (s *ActorSession) KVPut(ctx context.Context, key, value []byte) error {
	_, err := s.kv(ctx, wire.Command{
		Kind:  wire.CommandKVPut,
		AID:   s.AID(),
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	})
	return err
}

func (s *ActorSession) KVDelete(ctx context.Context, key []byte) error {
	_, err := s.kv(ctx, wire.Command{
		Kind: wire.CommandKVDelete,
		AID:  s.AID(),
		Key:  append([]byte(nil), key...),
	})
	return err
}

func (s *ActorSession) kv(ctx context.Context, command wire.Command) (wire.Event, error) {
	if s == nil || s.pump == nil {
		return wire.Event{}, errors.New("actor session is unavailable")
	}
	if ctx == nil {
		return wire.Event{}, errors.New("KV context is nil")
	}
	if !s.isAccepting() {
		return wire.Event{}, ErrShuttingDown
	}
	result := make(chan wire.Event, 1)
	kvID := s.pump.addKVWaiter(result)
	command.KVID = kvID

	if err := s.pump.submitInternal(ctx, command); err != nil {
		s.pump.removeKVWaiter(kvID)
		return wire.Event{}, err
	}
	select {
	case event := <-result:
		if event.Error != nil {
			return wire.Event{}, *event.Error
		}
		return event, nil
	case <-ctx.Done():
		s.pump.removeKVWaiter(kvID)
		return wire.Event{}, ctx.Err()
	case <-s.done:
		s.pump.removeKVWaiter(kvID)
		return wire.Event{}, ErrShuttingDown
	case <-s.pump.done:
		s.pump.removeKVWaiter(kvID)
		return wire.Event{}, ErrShuttingDown
	}
}

func (s *ActorSession) SQLiteExec(
	ctx context.Context,
	sql string,
	args []wire.SQLiteValue,
	leaseKey *string,
) (SQLiteResponse, error) {
	sqlArgs := make([]wire.SQLiteValue, len(args))
	copy(sqlArgs, args)
	return s.sqlite(ctx, wire.Command{
		Kind:     wire.CommandSQLiteExec,
		SQL:      sql,
		SQLArgs:  sqlArgs,
		LeaseKey: cloneStringPointer(leaseKey),
	})
}

func (s *ActorSession) SQLiteQuery(
	ctx context.Context,
	sql string,
	args []wire.SQLiteValue,
	leaseKey *string,
) (SQLiteResponse, error) {
	sqlArgs := make([]wire.SQLiteValue, len(args))
	copy(sqlArgs, args)
	return s.sqlite(ctx, wire.Command{
		Kind:     wire.CommandSQLiteQuery,
		SQL:      sql,
		SQLArgs:  sqlArgs,
		LeaseKey: cloneStringPointer(leaseKey),
	})
}

func (s *ActorSession) SQLiteBegin(
	ctx context.Context,
	leaseKey string,
	timeout time.Duration,
) error {
	_, err := s.sqlite(ctx, wire.Command{
		Kind:      wire.CommandSQLiteBegin,
		LeaseKey:  &leaseKey,
		TimeoutMS: durationMillis64(timeout),
	})
	return err
}

func (s *ActorSession) SQLiteCommit(ctx context.Context, leaseKey string) error {
	_, err := s.sqlite(ctx, wire.Command{
		Kind:     wire.CommandSQLiteCommit,
		LeaseKey: &leaseKey,
	})
	return err
}

func (s *ActorSession) SQLiteRollback(ctx context.Context, leaseKey string) error {
	_, err := s.sqlite(ctx, wire.Command{
		Kind:     wire.CommandSQLiteRollback,
		LeaseKey: &leaseKey,
	})
	return err
}

func (s *ActorSession) sqlite(ctx context.Context, command wire.Command) (SQLiteResponse, error) {
	if s == nil || s.pump == nil {
		return SQLiteResponse{}, errors.New("actor session is unavailable")
	}
	if ctx == nil {
		return SQLiteResponse{}, errors.New("SQLite context is nil")
	}
	if err := s.beginSQLite(); err != nil {
		return SQLiteResponse{}, err
	}
	defer s.finishSQLite()
	deadlineMS, err := sqliteDeadlineMillis(ctx)
	if err != nil {
		return SQLiteResponse{}, err
	}
	result := make(chan wire.Event, 64)
	requestID := s.pump.addSQLiteWaiter(result)
	command.SQLiteRequestID = requestID
	command.AID = s.AID()
	command.Generation = s.Generation()
	command.DeadlineMS = deadlineMS
	if err := s.pump.submitInternal(ctx, command); err != nil {
		s.pump.removeSQLiteWaiter(requestID)
		return SQLiteResponse{}, err
	}

	var assembler sqliteResponseAssembler
	for {
		select {
		case event := <-result:
			done, err := assembler.add(event)
			if err != nil {
				return SQLiteResponse{}, err
			}
			if done {
				return assembler.response, nil
			}
		case <-ctx.Done():
			s.pump.abandonSQLiteWaiter(requestID)
			return SQLiteResponse{}, ctx.Err()
		case <-s.done:
			s.pump.abandonSQLiteWaiter(requestID)
			return SQLiteResponse{}, ErrShuttingDown
		case <-s.pump.done:
			s.pump.abandonSQLiteWaiter(requestID)
			return SQLiteResponse{}, ErrShuttingDown
		}
	}
}

func (a *sqliteResponseAssembler) add(event wire.Event) (bool, error) {
	if event.ChunkIndex != a.nextChunk {
		return false, fmt.Errorf(
			"SQLite result chunk index %d follows %d",
			event.ChunkIndex,
			a.nextChunk,
		)
	}
	a.nextChunk++
	if event.Error != nil {
		return false, *event.Error
	}
	if event.ChunkIndex == 0 {
		a.response.Columns = append([]string(nil), event.Columns...)
		a.response.RowsAffected = event.RowsAffected
		a.response.LastInsertID = cloneInt64Pointer(event.LastInsertID)
		for _, column := range event.Columns {
			a.totalBytes += len(column)
		}
		if a.totalBytes > maxSQLiteResponseBytes {
			return false, wire.WireError{Code: "sqlite_result_too_large", Message: "SQLite result exceeds the 32 MiB result limit"}
		}
	} else if len(event.Columns) != 0 || event.RowsAffected != 0 || event.LastInsertID != nil {
		return false, errors.New("SQLite result repeated first-chunk metadata")
	}
	for _, value := range event.SQLiteValues {
		a.totalBytes += sqliteValueContentBytes(value)
		if a.totalBytes > maxSQLiteResponseBytes {
			return false, wire.WireError{Code: "sqlite_result_too_large", Message: "SQLite result exceeds the 32 MiB result limit"}
		}
	}
	a.response.Values = append(a.response.Values, event.SQLiteValues...)
	if !event.Done {
		return false, nil
	}
	if len(a.response.Columns) == 0 && len(a.response.Values) != 0 {
		return false, errors.New("SQLite result has values without columns")
	}
	if len(a.response.Columns) != 0 && len(a.response.Values)%len(a.response.Columns) != 0 {
		return false, errors.New("SQLite result is not rectangular")
	}
	return true, nil
}

func sqliteValueContentBytes(value wire.SQLiteValue) int {
	const fixedValueBytes = 16
	switch value.Kind {
	case "null":
		return 1
	case "integer", "real":
		return fixedValueBytes
	case "text":
		if value.Text != nil {
			return len(*value.Text) + fixedValueBytes
		}
	case "blob":
		if value.Blob != nil {
			return len(*value.Blob) + fixedValueBytes
		}
	}
	return fixedValueBytes
}

func sqliteDeadlineMillis(ctx context.Context) (uint32, error) {
	timeout := defaultSQLiteTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	if timeout <= 0 {
		return 0, context.DeadlineExceeded
	}
	milliseconds := timeout.Milliseconds()
	if timeout%time.Millisecond != 0 {
		milliseconds++
	}
	if milliseconds > int64(^uint32(0)) {
		milliseconds = int64(^uint32(0))
	}
	return uint32(milliseconds), nil
}

func durationMillis64(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	milliseconds := duration.Milliseconds()
	if duration%time.Millisecond != 0 {
		milliseconds++
	}
	return uint64(milliseconds)
}

func (s *ActorSession) completeSave(event wire.Event) bool {
	s.saveStateMu.Lock()
	result := s.saveResult
	s.saveResult = nil
	s.saveStateMu.Unlock()
	if result == nil {
		return false
	}
	if event.Error != nil {
		result <- *event.Error
	} else {
		result <- nil
	}
	return true
}

func (s *ActorSession) clearSaveResult(result chan error) {
	s.saveStateMu.Lock()
	if s.saveResult == result {
		s.saveResult = nil
	}
	s.saveStateMu.Unlock()
}

func (s *ActorSession) abandonSave(result chan error) {
	s.saveStateMu.Lock()
	if s.saveResult == result {
		s.saveResult = nil
		s.savePoisoned = true
		s.pump.markLateSave(s.identity)
	}
	s.saveStateMu.Unlock()
}

func (s *ActorSession) completeAbandonedSave() bool {
	s.saveStateMu.Lock()
	if !s.savePoisoned {
		s.saveStateMu.Unlock()
		return false
	}
	s.savePoisoned = false
	s.saveStateMu.Unlock()
	return s.pump.consumeLateSave(s.identity)
}

func (s *ActorSession) beginSave() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if !s.accepting {
		return ErrShuttingDown
	}
	s.activeSaves++
	return nil
}

func (s *ActorSession) beginSQLite() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if !s.accepting {
		return ErrShuttingDown
	}
	s.activeSQLite++
	return nil
}

func (s *ActorSession) finishSQLite() {
	s.lifecycleMu.Lock()
	s.activeSQLite--
	if s.activeSaves == 0 && s.activeSQLite == 0 {
		s.lifecycleCond.Broadcast()
	}
	s.lifecycleMu.Unlock()
}

func (s *ActorSession) finishSave() {
	s.lifecycleMu.Lock()
	s.activeSaves--
	if s.activeSaves == 0 && s.activeSQLite == 0 {
		s.lifecycleCond.Broadcast()
	}
	s.lifecycleMu.Unlock()
}

func (s *ActorSession) isAccepting() bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.accepting
}

// closeGracefully stops new actor-scoped work, waits for every state save
// already accepted by the session, and then wakes pending KV callers. The
// actor worker calls this before submitting ActorStopResult.
func (s *ActorSession) closeGracefully() {
	s.lifecycleMu.Lock()
	s.accepting = false
	for s.activeSaves != 0 || s.activeSQLite != 0 {
		s.lifecycleCond.Wait()
	}
	s.closeDoneLocked()
	s.lifecycleMu.Unlock()
}

// abort wakes pending actor operations before waiting for them. It is used
// when the whole pump is exiting and no durability acknowledgement can still
// be required.
func (s *ActorSession) abort() {
	if s != nil && s.pump != nil {
		s.pump.abortHTTPForActor(s.identity, ErrShuttingDown)
	}
	s.lifecycleMu.Lock()
	s.accepting = false
	s.closeDoneLocked()
	for s.activeSaves != 0 || s.activeSQLite != 0 {
		s.lifecycleCond.Wait()
	}
	s.lifecycleMu.Unlock()
}

func (s *ActorSession) closeDoneLocked() {
	if !s.doneClosed {
		close(s.done)
		s.doneClosed = true
	}
}

func saveContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return HandlerError{Code: "state_persist_timeout", Message: err.Error()}
	}
	return HandlerError{Code: "state_persist_canceled", Message: err.Error()}
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}

func cloneBytesOrEmpty(data []byte) []byte {
	if data == nil {
		return []byte{}
	}
	return cloneBytes(data)
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneStringMapOrEmpty(values map[string]string) map[string]string {
	if values == nil {
		return map[string]string{}
	}
	return cloneStringMap(values)
}

func newHTTPRequestBody(ctx context.Context) *httpRequestBody {
	return &httpRequestBody{ctx: ctx, wake: make(chan struct{})}
}

func (b *httpRequestBody) Read(destination []byte) (int, error) {
	for {
		b.mu.Lock()
		if len(b.chunks) != 0 {
			chunk := b.chunks[0]
			count := copy(destination, chunk[b.offset:])
			b.offset += count
			if b.offset == len(chunk) {
				b.chunks = b.chunks[1:]
				b.offset = 0
			}
			b.mu.Unlock()
			return count, nil
		}
		if b.closed {
			b.mu.Unlock()
			return 0, io.ErrClosedPipe
		}
		if b.finished {
			err := b.err
			b.mu.Unlock()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		wake := b.wake
		b.mu.Unlock()
		select {
		case <-wake:
		case <-b.ctx.Done():
			return 0, context.Cause(b.ctx)
		}
	}
}

func (b *httpRequestBody) Close() error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.signalLocked()
	}
	b.mu.Unlock()
	return nil
}

func (b *httpRequestBody) append(chunk []byte, finish bool) {
	b.mu.Lock()
	if !b.closed && !b.finished && len(chunk) != 0 {
		b.chunks = append(b.chunks, cloneBytes(chunk))
	}
	if finish {
		b.finished = true
	}
	b.signalLocked()
	b.mu.Unlock()
}

func (b *httpRequestBody) abort(err error) {
	b.mu.Lock()
	if !b.finished {
		b.finished = true
		b.err = err
	}
	b.signalLocked()
	b.mu.Unlock()
}

func (b *httpRequestBody) signalLocked() {
	close(b.wake)
	b.wake = make(chan struct{})
}

type actorWorker struct {
	pump    *Pump
	handler ActorHandler
	session *ActorSession
	ctx     context.Context
	cancel  context.CancelFunc
	mailbox *actorMailbox
}

// actorMailbox is the unbounded FIFO between the poll goroutine and one actor
// worker. Delivery must never block the poller: a worker can be inside a user
// handler that is itself waiting on a completion (KV, save, intent, SQLite)
// that only the poller can deliver, so any bounded hand-off from the poller to
// a worker can deadlock the whole runner. Boundedness here also provided no
// per-actor backpressure — a full queue stalled event delivery for every actor
// on the runner. Inflow stays bounded by the native side: per-connection
// admission queues, action deadlines, and hibernation acknowledgements all
// limit how many events the engine hands the pump ahead of handler progress.
type actorMailbox struct {
	mu     sync.Mutex
	queue  []wire.Event
	signal chan struct{}
}

func newActorMailbox() *actorMailbox {
	return &actorMailbox{signal: make(chan struct{}, 1)}
}

// put appends the event and never blocks. Delivery to a worker that has
// already exited is harmless: the mailbox is dropped with the worker.
func (m *actorMailbox) put(event wire.Event) {
	m.mu.Lock()
	m.queue = append(m.queue, event)
	m.mu.Unlock()
	select {
	case m.signal <- struct{}{}:
	default:
	}
}

// take returns the next event in FIFO order, blocking until one arrives or
// ctx is canceled.
func (m *actorMailbox) take(ctx context.Context) (wire.Event, bool) {
	for {
		m.mu.Lock()
		if len(m.queue) != 0 {
			event := m.queue[0]
			m.queue[0] = wire.Event{}
			m.queue = m.queue[1:]
			if len(m.queue) == 0 {
				m.queue = nil
			}
			m.mu.Unlock()
			return event, true
		}
		m.mu.Unlock()
		select {
		case <-m.signal:
		case <-ctx.Done():
			return wire.Event{}, false
		}
	}
}

type websocketOwner struct {
	identity     actorIdentity
	canHibernate bool
}

// Pump owns runner and closes it after RunnerStopped has been dispatched.
type Pump struct {
	runner          Runner
	pollTimeout     time.Duration
	shutdownTimeout time.Duration
	saveTimeout     time.Duration
	intentTimeout   time.Duration
	httpSubmitLimit time.Duration
	wsSubmitLimit   time.Duration
	sqliteTransport string
	handlers        map[string]ActorHandler

	started      atomic.Bool
	shuttingDown atomic.Bool
	submitQueue  chan submitRequest
	submitStop   chan struct{}
	submitOnce   sync.Once
	submitWG     sync.WaitGroup
	done         chan struct{}
	workerErrors chan error

	actorsMu sync.Mutex
	actors   map[actorIdentity]*actorWorker
	actorWG  sync.WaitGroup

	nextKVID      atomic.Uint64
	kvMu          sync.Mutex
	kvPending     map[uint64]chan wire.Event
	nextIntentID  atomic.Uint64
	intentMu      sync.Mutex
	intentPending map[uint64]chan wire.Event
	nextSQLiteID  atomic.Uint64
	sqliteMu      sync.Mutex
	sqlitePending map[uint64]chan wire.Event
	lateSaves     map[actorIdentity]struct{}
	httpMu        sync.Mutex
	httpPending   map[uint64]*httpRequestState
	wsMu          sync.Mutex
	websockets    map[string]websocketOwner

	subsMu    sync.Mutex
	subs      map[uint64]*subscriber
	nextSubID uint64
	resultMu  sync.Mutex
	result    error
	seenSeq   bool
	lastSeq   uint64
	hooks     *hookDispatcher
	logger    *slog.Logger
}

func New(runner Runner) *Pump {
	return NewWithHandlers(runner, nil)
}

func NewWithHandlers(runner Runner, handlers map[string]ActorHandler) *Pump {
	return NewWithOptions(runner, handlers, Options{})
}

func NewWithOptions(runner Runner, handlers map[string]ActorHandler, options Options) *Pump {
	ownedHandlers := make(map[string]ActorHandler, len(handlers))
	for name, handler := range handlers {
		ownedHandlers[name] = handler
	}
	shutdownTimeout := options.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	return &Pump{
		runner:          runner,
		pollTimeout:     defaultPollTimeout,
		shutdownTimeout: shutdownTimeout,
		saveTimeout:     defaultSaveTimeout,
		intentTimeout:   defaultIntentTimeout,
		httpSubmitLimit: defaultHTTPSubmitLimit,
		wsSubmitLimit:   defaultWSSubmitLimit,
		sqliteTransport: options.SQLiteTransport,
		handlers:        ownedHandlers,
		submitQueue:     make(chan submitRequest, defaultSubmitQueue),
		submitStop:      make(chan struct{}),
		done:            make(chan struct{}),
		workerErrors:    make(chan error, 1),
		actors:          make(map[actorIdentity]*actorWorker),
		kvPending:       make(map[uint64]chan wire.Event),
		intentPending:   make(map[uint64]chan wire.Event),
		sqlitePending:   make(map[uint64]chan wire.Event),
		lateSaves:       make(map[actorIdentity]struct{}),
		httpPending:     make(map[uint64]*httpRequestState),
		websockets:      make(map[string]websocketOwner),
		subs:            make(map[uint64]*subscriber),
		hooks:           newHookDispatcher(options.Hooks, options.Logger),
		logger:          options.Logger,
	}
}

// Subscribe registers a lossless event subscriber. A slow subscriber applies
// backpressure to polling, so callers should choose a buffer appropriate for
// their event handler and continue consuming until Events closes.
func (p *Pump) Subscribe(buffer int) *Subscription {
	if buffer < 0 {
		buffer = 0
	}
	sub := &subscriber{events: make(chan wire.Event, buffer)}
	p.subsMu.Lock()
	id := p.nextSubID
	p.nextSubID++
	p.subs[id] = sub
	p.subsMu.Unlock()
	return &Subscription{
		Events: sub.events,
		cancel: func() {
			p.subsMu.Lock()
			delete(p.subs, id)
			p.subsMu.Unlock()
			sub.close()
		},
	}
}

// Run starts exactly one native polling goroutine and blocks until clean
// shutdown or a fatal pump error.
func (p *Pump) Run(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	if p.runner == nil {
		return errors.New("pump runner is nil")
	}

	p.hooks.start()
	p.submitWG.Add(1)
	go p.submitLoop()
	go p.pollLoop(ctx)
	<-p.done

	p.resultMu.Lock()
	defer p.resultMu.Unlock()
	return p.result
}

// Submit batches commands from concurrent external callers before crossing
// the FFI. It rejects new work once graceful shutdown begins.
func (p *Pump) Submit(ctx context.Context, commands ...wire.Command) error {
	if !p.started.Load() {
		return ErrNotStarted
	}
	if p.shuttingDown.Load() {
		return ErrShuttingDown
	}
	return p.enqueueSubmit(ctx, commands)
}

// submitInternal remains available while core drains actors so OnStop,
// SaveState, and lifecycle results can complete before RunnerStopped.
func (p *Pump) submitInternal(ctx context.Context, commands ...wire.Command) error {
	if !p.started.Load() {
		return ErrNotStarted
	}
	return p.enqueueSubmit(ctx, commands)
}

func (p *Pump) enqueueSubmit(ctx context.Context, commands []wire.Command) error {
	request := submitRequest{
		commands: append([]wire.Command(nil), commands...),
		result:   make(chan error, 1),
	}
	select {
	case p.submitQueue <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.submitStop:
		return ErrShuttingDown
	case <-p.done:
		return ErrShuttingDown
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		select {
		case err := <-request.result:
			return err
		default:
			return ErrShuttingDown
		}
	}
}

func (p *Pump) pollLoop(ctx context.Context) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer func() {
		p.cancelActorWorkers()
		p.actorWG.Wait()
		p.stopSubmissions()
		p.submitWG.Wait()
		p.failKVWaiters()
		p.failActorIntentWaiters()
		p.failSQLiteWaiters()
		p.hooks.stop()
		p.runner.Close()
		p.closeSubscribers()
		close(p.done)
	}()

	shutdownRequested := false
	for {
		select {
		case err := <-p.workerErrors:
			p.setResult(err)
			return
		default:
		}

		if !shutdownRequested {
			select {
			case <-ctx.Done():
				shutdownRequested = true
				p.shuttingDown.Store(true)
				p.log(slog.LevelInfo, "runner drain started",
					slog.Duration("deadline", p.shutdownTimeout),
					slog.String("cause", context.Cause(ctx).Error()),
				)
				if err := p.runner.Shutdown(p.shutdownTimeout); err != nil {
					p.setResult(fmt.Errorf("shutdown native runner: %w", err))
					return
				}
			default:
			}
		}

		pollStarted := time.Now()
		data, err := p.runner.Poll(p.pollTimeout)
		if err != nil {
			p.setResult(fmt.Errorf("poll native runner: %w", err))
			return
		}
		batch, err := wire.DecodeEventBatch(data)
		if err != nil {
			p.setResult(err)
			return
		}
		if len(batch.Events) != 0 {
			// Empty batches are the intentional poll timeout, not event latency.
			p.observeDuration(metricPollLatency, time.Since(pollStarted))
			p.counter(metricEventBatches, 1)
		}
		if p.seenSeq && batch.Seq <= p.lastSeq {
			p.setResult(fmt.Errorf(
				"non-monotonic event batch sequence: got %d after %d",
				batch.Seq,
				p.lastSeq,
			))
			return
		}
		p.seenSeq = true
		p.lastSeq = batch.Seq
		p.counter(metricEventsPolled, int64(len(batch.Events)))
		for _, event := range batch.Events {
			if err := p.handleInternalEvent(event); err != nil {
				p.setResult(err)
				return
			}
			if !p.dispatch(event) {
				return
			}
			if event.Kind == wire.EventRunnerStopped {
				if event.DrainReport == nil {
					p.setResult(errors.New("RunnerStopped did not include a drain report"))
				} else if !event.DrainReport.Graceful {
					p.setResult(fmt.Errorf(
						"runner drain exceeded its deadline with %d actors remaining",
						event.DrainReport.ActorsRemaining,
					))
				}
				p.log(slog.LevelInfo, "runner drain completed",
					slog.Uint64("event_seq", batch.Seq),
				)
				return
			}
		}
	}
}

func (p *Pump) submitLoop() {
	defer p.submitWG.Done()
	for {
		select {
		case <-p.submitStop:
			p.rejectQueuedSubmissions()
			return
		case first := <-p.submitQueue:
			requests := []submitRequest{first}
			commands := append([]wire.Command(nil), first.commands...)
		collect:
			for len(commands) < maxSubmitBatch {
				select {
				case request := <-p.submitQueue:
					requests = append(requests, request)
					commands = append(commands, request.commands...)
				default:
					break collect
				}
			}
			encoded, err := wire.EncodeCommandBatch(wire.CommandBatch{Commands: commands})
			if err == nil {
				err = p.runner.Submit(encoded)
			}
			if err == nil {
				p.counter(metricSubmitBatches, 1)
				p.counter(metricCommandsSubmitted, int64(len(commands)))
			} else if errorCode(err) == "backpressure" {
				p.counter(metricBackpressureHits, 1)
			}
			for _, request := range requests {
				request.result <- err
			}
		}
	}
}

func (p *Pump) handleInternalEvent(event wire.Event) error {
	switch event.Kind {
	case wire.EventRunnerConnected:
		p.log(slog.LevelInfo, "runner connected", slog.String("runner_id", event.RunnerID))
	case wire.EventRunnerDisconnected:
		p.log(slog.LevelWarn, "runner disconnected", slog.String("reason", event.Reason))
	case wire.EventRunnerStopped:
		// The poll loop logs completion after subscribers observe this event.
	case wire.EventActorStart:
		identity := actorIdentity{aid: event.AID, generation: event.Generation}
		workerCtx, cancel := context.WithCancel(context.Background())
		session := &ActorSession{
			pump:             p,
			identity:         identity,
			name:             event.Name,
			key:              event.Key,
			input:            cloneBytes(event.Input),
			persistedState:   cloneBytes(event.PersistedState),
			done:             make(chan struct{}),
			accepting:        true,
			sqliteTransport:  p.sqliteTransport,
			sqliteSocketPath: event.SQLiteSocketPath,
		}
		session.lifecycleCond = sync.NewCond(&session.lifecycleMu)
		worker := &actorWorker{
			pump:    p,
			handler: p.handlers[event.Name],
			session: session,
			ctx:     workerCtx,
			cancel:  cancel,
			mailbox: newActorMailbox(),
		}
		p.actorsMu.Lock()
		if _, exists := p.actors[identity]; exists {
			p.actorsMu.Unlock()
			cancel()
			return fmt.Errorf("duplicate ActorStart for %s generation %d", event.AID, event.Generation)
		}
		p.actors[identity] = worker
		liveActors := len(p.actors)
		p.actorWG.Add(1)
		p.gauge(metricLiveActors, int64(liveActors))
		p.actorsMu.Unlock()
		p.counter(metricActorStarts, 1)
		p.log(slog.LevelDebug, "actor started",
			slog.String("actor_id", event.AID),
			slog.Uint64("generation", event.Generation),
			slog.String("actor_name", event.Name),
		)
		go worker.run(event)
	case wire.EventActorStop:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			return fmt.Errorf("ActorStop for unknown actor %s generation %d", event.AID, event.Generation)
		}
		runnerDraining := p.shuttingDown.Load()
		closeReason := "actor stopped"
		if runnerDraining {
			closeReason = "runner shutting down"
		}
		closeEvents, hibernateCommands := p.detachActorWebSockets(
			worker.session.identity,
			event.Reason == "sleep" && !runnerDraining,
			closeReason,
		)
		if len(hibernateCommands) != 0 {
			submitCtx, cancel := context.WithTimeout(context.Background(), p.wsSubmitLimit)
			err := p.submitInternal(submitCtx, hibernateCommands...)
			cancel()
			if err != nil {
				return fmt.Errorf("hibernate actor WebSockets: %w", err)
			}
		}
		for _, closeEvent := range closeEvents {
			worker.mailbox.put(closeEvent)
		}
		p.counter(metricActorStops, 1)
		p.log(slog.LevelDebug, "actor stopping",
			slog.String("actor_id", event.AID),
			slog.Uint64("generation", event.Generation),
			slog.String("reason", event.Reason),
		)
		worker.mailbox.put(event)
	case wire.EventActorAlarm:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			return fmt.Errorf("ActorAlarm for unknown actor %s generation %d", event.AID, event.Generation)
		}
		worker.mailbox.put(event)
	case wire.EventActorIntentResult, wire.EventActorScheduleResult:
		p.intentMu.Lock()
		result := p.intentPending[event.OperationID]
		delete(p.intentPending, event.OperationID)
		p.intentMu.Unlock()
		if result != nil {
			result <- event
		}
	case wire.EventActionCall:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			return fmt.Errorf("ActionCall for unknown actor %s generation %d", event.AID, event.Generation)
		}
		worker.mailbox.put(event)
	case wire.EventConnectionPreflight, wire.EventConnectionOpen, wire.EventConnectionClose:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			return fmt.Errorf("%s for unknown actor %s generation %d", event.Kind, event.AID, event.Generation)
		}
		worker.mailbox.put(event)
	case wire.EventHTTPRequest:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			return fmt.Errorf("HttpRequest for unknown actor %s generation %d", event.AID, event.Generation)
		}
		requestContext, cancel := context.WithCancelCause(worker.ctx)
		body := newHTTPRequestBody(requestContext)
		body.append(event.Body, !event.Stream)
		state := &httpRequestState{
			identity: worker.session.identity,
			request: HTTPRequest{
				Context: requestContext,
				Method:  event.Method,
				Path:    event.Path,
				Headers: cloneStringMap(event.Headers),
				Body:    body,
			},
			cancel: cancel,
			body:   body,
		}
		p.httpMu.Lock()
		if _, exists := p.httpPending[event.RequestID]; exists {
			p.httpMu.Unlock()
			cancel(errors.New("duplicate HTTP request ID"))
			return fmt.Errorf("duplicate HttpRequest req_id %d", event.RequestID)
		}
		p.httpPending[event.RequestID] = state
		p.httpMu.Unlock()
		worker.mailbox.put(event)
	case wire.EventHTTPRequestChunk:
		state := p.httpRequest(event.RequestID)
		if state == nil {
			return fmt.Errorf("HttpRequestChunk for unknown req_id %d", event.RequestID)
		}
		state.body.append(event.Body, event.Finish)
	case wire.EventHTTPRequestAbort:
		state := p.httpRequest(event.RequestID)
		if state == nil {
			return nil
		}
		abortErr := errors.New("HTTP request aborted by engine")
		state.cancel(abortErr)
		state.body.abort(abortErr)
	case wire.EventWSOpen:
		worker := p.currentActor(event.AID)
		if worker == nil {
			return fmt.Errorf("WsOpen for unknown actor %s", event.AID)
		}
		p.wsMu.Lock()
		if _, exists := p.websockets[event.WSID]; exists {
			p.wsMu.Unlock()
			return fmt.Errorf("duplicate WsOpen ws_id %s", event.WSID)
		}
		p.websockets[event.WSID] = websocketOwner{
			identity:     worker.session.identity,
			canHibernate: event.CanHibernate,
		}
		liveConnections := len(p.websockets)
		p.gauge(metricLiveConnections, int64(liveConnections))
		p.wsMu.Unlock()
		worker.mailbox.put(event)
	case wire.EventWSMessage:
		worker := p.websocketActor(event.WSID)
		if worker == nil {
			return nil
		}
		worker.mailbox.put(event)
	case wire.EventWSClose:
		worker := p.removeWebSocket(event.WSID)
		if worker == nil {
			return nil
		}
		worker.mailbox.put(event)
	case wire.EventStatePersisted:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			if p.consumeLateSave(actorIdentity{aid: event.AID, generation: event.Generation}) {
				return nil
			}
			return fmt.Errorf("StatePersisted for unknown actor %s generation %d", event.AID, event.Generation)
		}
		if !worker.session.completeSave(event) {
			if worker.session.completeAbandonedSave() {
				return nil
			}
			if p.consumeLateSave(actorIdentity{aid: event.AID, generation: event.Generation}) {
				return nil
			}
			return fmt.Errorf("StatePersisted without a pending save for %s generation %d", event.AID, event.Generation)
		}
	case wire.EventKVResult:
		p.kvMu.Lock()
		result := p.kvPending[event.KVID]
		delete(p.kvPending, event.KVID)
		p.kvMu.Unlock()
		if result == nil {
			return nil // A caller may have canceled after the command was enqueued.
		}
		result <- event
	case wire.EventSQLiteResult:
		p.sqliteMu.Lock()
		result, exists := p.sqlitePending[event.SQLiteRequestID]
		if event.Done {
			delete(p.sqlitePending, event.SQLiteRequestID)
		}
		p.sqliteMu.Unlock()
		if !exists || result == nil {
			return nil
		}
		result <- event
	}
	return nil
}

func (w *actorWorker) run(start wire.Event) {
	defer w.pump.actorWG.Done()
	defer w.cancel()
	defer w.pump.removeActor(w.session.identity, w)
	defer w.session.abort()

	state, lifecycleError := invokeStart(w.ctx, w.handler, w.session, start)
	w.pump.recordHandlerPanic(lifecycleError, "OnStart", w.session)
	if err := w.pump.submitInternal(w.ctx, wire.Command{
		Kind:       wire.CommandActorStartResult,
		AID:        w.session.identity.aid,
		Generation: w.session.identity.generation,
		OK:         lifecycleError == nil,
		Error:      lifecycleError,
	}); err != nil {
		w.pump.reportWorkerError(fmt.Errorf("submit ActorStartResult: %w", err))
		return
	}
	if lifecycleError != nil {
		return
	}
	stopReason := "actor worker stopped"
	defer func() {
		if wsHandler, ok := w.handler.(actorWebSocketHandler); ok {
			wsHandler.CloseWebSockets(context.Background(), w.session, state, stopReason)
		}
	}()

	for {
		event, ok := w.mailbox.take(w.ctx)
		if !ok {
			return
		}
		switch event.Kind {
		case wire.EventActionCall:
			actionTimeout := time.Duration(event.ActionTimeoutMS) * time.Millisecond
			actionContext, cancelAction := context.WithTimeout(w.ctx, actionTimeout)
			output, actionError := invokeAction(actionContext, w.handler, w.session, event, state)
			w.pump.recordHandlerPanic(actionError, "action "+event.Action, w.session)
			cancelAction()
			if actionError == nil && output == nil {
				output = []byte{}
			}
			var connectionState *[]byte
			_, tracksConnectionState := w.handler.(actorConnectionHandler)
			if actionError == nil && event.ConnID != nil && tracksConnectionState {
				encoded, tracked, stateError := invokeConnectionState(w.ctx, w.handler, w.session, *event.ConnID, state)
				if stateError != nil {
					actionError = stateError
					output = nil
				} else if tracked {
					connectionState = byteSlicePointer(encoded)
				}
			}
			if err := w.pump.submitInternal(w.ctx, wire.Command{
				Kind:            wire.CommandActionResult,
				CallID:          event.CallID,
				Output:          output,
				ConnectionState: connectionState,
				Error:           actionError,
			}); err != nil {
				w.pump.reportWorkerError(fmt.Errorf("submit ActionResult: %w", err))
				return
			}
			if actionError != nil && actionError.Code == "handler_panic" {
				return
			}
		case wire.EventConnectionPreflight, wire.EventConnectionOpen, wire.EventConnectionClose:
			connectionState, connectionError := invokeConnectionLifecycle(
				w.ctx, w.handler, w.session, event, state,
			)
			w.pump.recordHandlerPanic(connectionError, string(event.Kind), w.session)
			command := wire.Command{
				Kind:        wire.CommandConnectionResult,
				OperationID: event.OperationID,
				Error:       connectionError,
			}
			if connectionError == nil {
				command.ConnectionState = byteSlicePointer(connectionState)
			}
			if err := w.pump.submitInternal(w.ctx, command); err != nil {
				w.pump.reportWorkerError(fmt.Errorf("submit ConnectionResult: %w", err))
				return
			}
			if connectionError != nil && connectionError.Code == "handler_panic" {
				w.requestErrorStop()
			}
		case wire.EventActorAlarm:
			alarmError := invokeAlarm(w.ctx, w.handler, w.session, event, state)
			w.pump.recordHandlerPanic(alarmError, "OnAlarm", w.session)
			if err := w.pump.submitInternal(w.ctx, wire.Command{
				Kind:       wire.CommandAlarmHandled,
				AID:        w.session.identity.aid,
				Generation: w.session.identity.generation,
				Error:      alarmError,
			}); err != nil {
				w.pump.reportWorkerError(fmt.Errorf("submit AlarmHandled: %w", err))
				return
			}
			if alarmError != nil && alarmError.Code == "handler_panic" {
				w.requestErrorStop()
			}
		case wire.EventHTTPRequest:
			fetchError := invokeFetch(w.ctx, w.handler, w.session, event, state)
			w.pump.recordHandlerPanic(fetchError, "OnFetch", w.session)
			if fetchError != nil {
				if err := w.session.failHTTP(w.ctx, event.RequestID, fetchError); err != nil {
					w.pump.reportWorkerError(fmt.Errorf("submit HTTP handler error: %w", err))
					return
				}
			}
			w.pump.finishHTTPRequest(event.RequestID)
			if fetchError != nil && fetchError.Code == "handler_panic" {
				return
			}
		case wire.EventWSOpen:
			openError := invokeWebSocketOpen(w.ctx, w.handler, w.session, event, state)
			w.pump.recordHandlerPanic(openError, "OnConnect", w.session)
			if err := w.pump.submitInternal(w.ctx, wire.Command{
				Kind:   wire.CommandWSOpenResult,
				WSID:   event.WSID,
				Accept: openError == nil,
				Error:  openError,
			}); err != nil {
				w.pump.reportWorkerError(fmt.Errorf("submit WsOpenResult: %w", err))
				return
			}
			if openError != nil {
				w.pump.removeWebSocket(event.WSID)
				if openError.Code == "handler_panic" {
					w.requestErrorStop()
				}
			}
		case wire.EventWSMessage:
			needsAcknowledgement := w.pump.websocketCanHibernate(event.WSID)
			messageError := invokeWebSocketMessage(w.ctx, w.handler, w.session, event, state)
			w.pump.recordHandlerPanic(messageError, "OnMessage", w.session)
			if needsAcknowledgement {
				if err := w.pump.submitInternal(w.ctx, wire.Command{
					Kind:         wire.CommandWSMessageAck,
					WSID:         event.WSID,
					MessageIndex: event.MessageIndex,
				}); err != nil {
					w.pump.reportWorkerError(fmt.Errorf("submit WsMessageAck: %w", err))
					return
				}
			}
			if messageError != nil && messageError.Code == "handler_panic" {
				w.requestErrorStop()
			}
		case wire.EventWSClose:
			closeError := invokeWebSocketClose(w.ctx, w.handler, w.session, event, state)
			w.pump.recordHandlerPanic(closeError, "OnDisconnect", w.session)
			if closeError != nil && closeError.Code == "handler_panic" {
				w.requestErrorStop()
			}
		case wire.EventActorStop:
			stopReason = event.Reason
			lifecycleError = invokeStop(w.ctx, w.handler, w.session, event, state)
			w.pump.recordHandlerPanic(lifecycleError, "OnStop", w.session)
			w.session.closeGracefully()
			if err := w.pump.submitInternal(w.ctx, wire.Command{
				Kind:       wire.CommandActorStopResult,
				AID:        w.session.identity.aid,
				Generation: w.session.identity.generation,
				Error:      lifecycleError,
			}); err != nil {
				w.pump.reportWorkerError(fmt.Errorf("submit ActorStopResult: %w", err))
			}
			return
		default:
			w.pump.reportWorkerError(fmt.Errorf(
				"unexpected %s on actor worker %s generation %d",
				event.Kind,
				w.session.identity.aid,
				w.session.identity.generation,
			))
			return
		}
	}
}

func (w *actorWorker) requestErrorStop() {
	if err := w.pump.submitInternal(w.ctx, wire.Command{
		Kind: wire.CommandStopIntent,
		AID:  w.session.AID(),
	}); err != nil {
		w.pump.reportWorkerError(fmt.Errorf("submit WebSocket panic StopIntent: %w", err))
	}
}

func invokeStart(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
) (state any, lifecycleError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			state = nil
			lifecycleError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("OnStart panicked: %v", recovered),
			}
		}
	}()
	if handler == nil {
		return nil, &wire.WireError{
			Code:    "actor_not_registered",
			Message: fmt.Sprintf("actor type %q is not registered", event.Name),
		}
	}
	state, err := handler.Start(ctx, session, event)
	if err != nil {
		return nil, handlerWireError("OnStart", err)
	}
	return state, nil
}

func invokeStop(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
	state any,
) (lifecycleError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			lifecycleError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("OnStop panicked: %v", recovered),
			}
		}
	}()
	if handler == nil {
		return &wire.WireError{
			Code:    "actor_not_registered",
			Message: "actor handler is unavailable during stop",
		}
	}
	if err := handler.Stop(ctx, session, event, state); err != nil {
		return handlerWireError("OnStop", err)
	}
	return nil
}

func invokeAlarm(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
	state any,
) (handlerError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			handlerError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("OnAlarm panicked: %v", recovered),
			}
		}
	}()
	alarmHandler, ok := handler.(actorAlarmHandler)
	if !ok {
		return &wire.WireError{Code: "callback_not_found", Message: "actor has no OnAlarm handler"}
	}
	if err := alarmHandler.Alarm(ctx, session, event, state); err != nil {
		return handlerWireError("OnAlarm", err)
	}
	return nil
}

func invokeAction(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
	state any,
) (output []byte, actionError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			output = nil
			actionError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("action %q panicked: %v", event.Action, recovered),
			}
		}
	}()
	actionHandler, ok := handler.(actorActionHandler)
	if !ok {
		return nil, &wire.WireError{
			Code:    "action_not_found",
			Message: fmt.Sprintf("action %q is not registered", event.Action),
		}
	}
	output, err := actionHandler.Action(ctx, session, event, state)
	if err != nil {
		return nil, handlerWireError("action "+event.Action, err)
	}
	return cloneBytes(output), nil
}

func invokeConnectionLifecycle(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
	state any,
) (connectionState []byte, handlerError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			connectionState = nil
			handlerError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("%s panicked: %v", event.Kind, recovered),
			}
		}
	}()
	connectionHandler, ok := handler.(actorConnectionHandler)
	if !ok {
		return []byte{}, nil
	}
	var (
		encoded []byte
		err     error
	)
	switch event.Kind {
	case wire.EventConnectionPreflight:
		encoded, err = connectionHandler.ConnectionPreflight(ctx, session, event, state)
	case wire.EventConnectionOpen:
		encoded, err = connectionHandler.ConnectionOpen(ctx, session, event, state)
	case wire.EventConnectionClose:
		encoded, err = connectionHandler.ConnectionClose(ctx, session, event, state)
	default:
		return nil, &wire.WireError{Code: "handler_error", Message: "unknown connection lifecycle event"}
	}
	if err != nil {
		return nil, handlerWireError(string(event.Kind), err)
	}
	return cloneBytes(encoded), nil
}

func invokeConnectionState(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	connectionID string,
	state any,
) (connectionState []byte, tracked bool, handlerError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			connectionState = nil
			tracked = false
			handlerError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("encode connection state panicked: %v", recovered),
			}
		}
	}()
	connectionHandler, ok := handler.(actorConnectionHandler)
	if !ok {
		return nil, false, nil
	}
	encoded, tracked, err := connectionHandler.ConnectionState(ctx, session, connectionID, state)
	if err != nil {
		return nil, false, handlerWireError("encode connection state", err)
	}
	return cloneBytes(encoded), tracked, nil
}

func byteSlicePointer(data []byte) *[]byte {
	cloned := cloneBytes(data)
	if cloned == nil {
		cloned = []byte{}
	}
	return &cloned
}

func invokeFetch(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
	state any,
) (fetchError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			fetchError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("OnFetch panicked: %v", recovered),
			}
		}
	}()
	fetchHandler, ok := handler.(actorFetchHandler)
	if !ok {
		return &wire.WireError{
			Code:    "handler_error",
			Message: "actor has no OnFetch handler",
		}
	}
	if err := fetchHandler.Fetch(ctx, session, event, state); err != nil {
		return handlerWireError("OnFetch", err)
	}
	return nil
}

func invokeWebSocketOpen(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
	state any,
) (handlerError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			handlerError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("OnConnect panicked: %v", recovered),
			}
		}
	}()
	wsHandler, ok := handler.(actorWebSocketHandler)
	if !ok {
		return &wire.WireError{Code: "callback_not_found", Message: "actor has no OnConnect handler"}
	}
	if err := wsHandler.WebSocketOpen(ctx, session, event, state); err != nil {
		return handlerWireError("OnConnect", err)
	}
	return nil
}

func invokeWebSocketMessage(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
	state any,
) (handlerError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			handlerError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("OnMessage panicked: %v", recovered),
			}
		}
	}()
	wsHandler, ok := handler.(actorWebSocketHandler)
	if !ok {
		return &wire.WireError{Code: "callback_not_found", Message: "actor has no OnMessage handler"}
	}
	if err := wsHandler.WebSocketMessage(ctx, session, event, state); err != nil {
		return handlerWireError("OnMessage", err)
	}
	return nil
}

func invokeWebSocketClose(
	ctx context.Context,
	handler ActorHandler,
	session *ActorSession,
	event wire.Event,
	state any,
) (handlerError *wire.WireError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			handlerError = &wire.WireError{
				Code:    "handler_panic",
				Message: fmt.Sprintf("OnDisconnect panicked: %v", recovered),
			}
		}
	}()
	wsHandler, ok := handler.(actorWebSocketHandler)
	if !ok {
		return nil
	}
	if err := wsHandler.WebSocketClose(ctx, session, event, state); err != nil {
		return handlerWireError("OnDisconnect", err)
	}
	return nil
}

func handlerWireError(operation string, err error) *wire.WireError {
	var structured HandlerError
	if errors.As(err, &structured) {
		return &wire.WireError{Code: structured.Code, Message: structured.Message}
	}
	return &wire.WireError{
		Code:    "handler_error",
		Message: fmt.Sprintf("%s failed: %v", operation, err),
	}
}

func (p *Pump) actor(aid string, generation uint64) *actorWorker {
	p.actorsMu.Lock()
	defer p.actorsMu.Unlock()
	return p.actors[actorIdentity{aid: aid, generation: generation}]
}

func (p *Pump) currentActor(aid string) *actorWorker {
	p.actorsMu.Lock()
	defer p.actorsMu.Unlock()
	var current *actorWorker
	var generation uint64
	for identity, worker := range p.actors {
		if identity.aid == aid && (current == nil || identity.generation > generation) {
			current = worker
			generation = identity.generation
		}
	}
	return current
}

func (p *Pump) websocketActor(wsID string) *actorWorker {
	p.wsMu.Lock()
	owner, ok := p.websockets[wsID]
	p.wsMu.Unlock()
	if !ok {
		return nil
	}
	return p.actor(owner.identity.aid, owner.identity.generation)
}

func (p *Pump) websocketCanHibernate(wsID string) bool {
	p.wsMu.Lock()
	owner, ok := p.websockets[wsID]
	p.wsMu.Unlock()
	return ok && owner.canHibernate
}

func (p *Pump) removeWebSocket(wsID string) *actorWorker {
	p.wsMu.Lock()
	owner, ok := p.websockets[wsID]
	delete(p.websockets, wsID)
	liveConnections := len(p.websockets)
	if ok {
		p.gauge(metricLiveConnections, int64(liveConnections))
	}
	p.wsMu.Unlock()
	if !ok {
		return nil
	}
	return p.actor(owner.identity.aid, owner.identity.generation)
}

func (p *Pump) detachActorWebSockets(
	identity actorIdentity,
	hibernate bool,
	reason string,
) ([]wire.Event, []wire.Command) {
	p.wsMu.Lock()
	events := make([]wire.Event, 0)
	commands := make([]wire.Command, 0)
	code := uint16(1001)
	for wsID, owner := range p.websockets {
		if owner.identity != identity {
			continue
		}
		delete(p.websockets, wsID)
		if hibernate && owner.canHibernate {
			commands = append(commands, wire.Command{
				Kind:      wire.CommandWSClose,
				WSID:      wsID,
				Hibernate: true,
			})
			continue
		}
		events = append(events, wire.Event{
			Kind:      wire.EventWSClose,
			WSID:      wsID,
			CloseCode: &code,
			Reason:    reason,
		})
	}
	liveConnections := len(p.websockets)
	p.gauge(metricLiveConnections, int64(liveConnections))
	p.wsMu.Unlock()
	return events, commands
}

func (p *Pump) httpRequest(requestID uint64) *httpRequestState {
	p.httpMu.Lock()
	defer p.httpMu.Unlock()
	return p.httpPending[requestID]
}

func (p *Pump) finishHTTPRequest(requestID uint64) {
	p.httpMu.Lock()
	state := p.httpPending[requestID]
	delete(p.httpPending, requestID)
	p.httpMu.Unlock()
	if state != nil {
		_ = state.request.Body.Close()
		state.cancel(nil)
	}
}

func (p *Pump) abortHTTPForActor(identity actorIdentity, cause error) {
	p.httpMu.Lock()
	requests := make([]*httpRequestState, 0)
	for requestID, state := range p.httpPending {
		if state.identity == identity {
			delete(p.httpPending, requestID)
			requests = append(requests, state)
		}
	}
	p.httpMu.Unlock()
	for _, state := range requests {
		state.cancel(cause)
		state.body.abort(cause)
	}
}

func (p *Pump) removeActor(identity actorIdentity, worker *actorWorker) {
	_, _ = p.detachActorWebSockets(identity, false, "actor worker stopped")
	p.actorsMu.Lock()
	removed := false
	if p.actors[identity] == worker {
		delete(p.actors, identity)
		removed = true
	}
	liveActors := len(p.actors)
	if removed {
		p.gauge(metricLiveActors, int64(liveActors))
	}
	p.actorsMu.Unlock()
}

func (p *Pump) cancelActorWorkers() {
	p.actorsMu.Lock()
	workers := make([]*actorWorker, 0, len(p.actors))
	for _, worker := range p.actors {
		workers = append(workers, worker)
	}
	p.actorsMu.Unlock()
	for _, worker := range workers {
		worker.cancel()
		worker.session.abort()
	}
}

func (p *Pump) markLateSave(identity actorIdentity) {
	p.actorsMu.Lock()
	p.lateSaves[identity] = struct{}{}
	p.actorsMu.Unlock()
}

func (p *Pump) consumeLateSave(identity actorIdentity) bool {
	p.actorsMu.Lock()
	defer p.actorsMu.Unlock()
	if _, exists := p.lateSaves[identity]; !exists {
		return false
	}
	delete(p.lateSaves, identity)
	return true
}

func (p *Pump) reportWorkerError(err error) {
	select {
	case p.workerErrors <- err:
	default:
	}
}

func (p *Pump) removeKVWaiter(kvID uint64) {
	p.kvMu.Lock()
	delete(p.kvPending, kvID)
	p.kvMu.Unlock()
}

func (p *Pump) addKVWaiter(result chan wire.Event) uint64 {
	p.kvMu.Lock()
	defer p.kvMu.Unlock()
	for {
		kvID := p.nextKVID.Add(1)
		if kvID == 0 {
			continue
		}
		if _, pending := p.kvPending[kvID]; pending {
			continue
		}
		p.kvPending[kvID] = result
		return kvID
	}
}

func (p *Pump) failKVWaiters() {
	p.kvMu.Lock()
	for kvID := range p.kvPending {
		delete(p.kvPending, kvID)
	}
	p.kvMu.Unlock()
}

func (p *Pump) removeActorIntentWaiter(operationID uint64) {
	p.intentMu.Lock()
	delete(p.intentPending, operationID)
	p.intentMu.Unlock()
}

func (p *Pump) abandonActorIntentWaiter(operationID uint64) {
	p.intentMu.Lock()
	if _, pending := p.intentPending[operationID]; pending {
		// Retain a tombstone until a late native completion consumes it. This
		// keeps a wrapped operation ID from being assigned to a different wait.
		p.intentPending[operationID] = nil
	}
	p.intentMu.Unlock()
}

func (p *Pump) addActorIntentWaiter(result chan wire.Event) uint64 {
	p.intentMu.Lock()
	defer p.intentMu.Unlock()
	for {
		operationID := p.nextIntentID.Add(1)
		if operationID == 0 {
			continue
		}
		if _, pending := p.intentPending[operationID]; pending {
			continue
		}
		p.intentPending[operationID] = result
		return operationID
	}
}

func (p *Pump) failActorIntentWaiters() {
	p.intentMu.Lock()
	for operationID := range p.intentPending {
		delete(p.intentPending, operationID)
	}
	p.intentMu.Unlock()
}

func (p *Pump) addSQLiteWaiter(result chan wire.Event) uint64 {
	p.sqliteMu.Lock()
	defer p.sqliteMu.Unlock()
	for {
		requestID := p.nextSQLiteID.Add(1)
		if requestID == 0 {
			continue
		}
		if _, pending := p.sqlitePending[requestID]; pending {
			continue
		}
		p.sqlitePending[requestID] = result
		return requestID
	}
}

func (p *Pump) removeSQLiteWaiter(requestID uint64) {
	p.sqliteMu.Lock()
	delete(p.sqlitePending, requestID)
	p.sqliteMu.Unlock()
}

func (p *Pump) abandonSQLiteWaiter(requestID uint64) {
	p.sqliteMu.Lock()
	if _, pending := p.sqlitePending[requestID]; pending {
		p.sqlitePending[requestID] = nil
	}
	p.sqliteMu.Unlock()
}

func (p *Pump) failSQLiteWaiters() {
	p.sqliteMu.Lock()
	for requestID := range p.sqlitePending {
		delete(p.sqlitePending, requestID)
	}
	p.sqliteMu.Unlock()
}

func (p *Pump) stopSubmissions() {
	p.submitOnce.Do(func() {
		p.shuttingDown.Store(true)
		close(p.submitStop)
	})
}

func (p *Pump) rejectQueuedSubmissions() {
	for {
		select {
		case request := <-p.submitQueue:
			request.result <- ErrShuttingDown
		default:
			return
		}
	}
}

func (p *Pump) dispatch(event wire.Event) bool {
	p.subsMu.Lock()
	subscribers := make([]*subscriber, 0, len(p.subs))
	for _, sub := range p.subs {
		subscribers = append(subscribers, sub)
	}
	p.subsMu.Unlock()
	for _, sub := range subscribers {
		if !sub.send(event, p.done) {
			return false
		}
	}
	return true
}

func (s *subscriber) send(event wire.Event, done <-chan struct{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return true
	}
	select {
	case s.events <- event:
		return true
	case <-done:
		return false
	}
}

func (s *subscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
}

func (p *Pump) closeSubscribers() {
	p.subsMu.Lock()
	subscribers := make([]*subscriber, 0, len(p.subs))
	for id, sub := range p.subs {
		delete(p.subs, id)
		subscribers = append(subscribers, sub)
	}
	p.subsMu.Unlock()
	for _, sub := range subscribers {
		sub.close()
	}
}

func (p *Pump) setResult(err error) {
	p.resultMu.Lock()
	if p.result == nil {
		p.result = err
	}
	p.resultMu.Unlock()
}

func (p *Pump) recordHandlerPanic(failure *wire.WireError, operation string, session *ActorSession) {
	if failure == nil || failure.Code != "handler_panic" {
		return
	}
	p.counter(metricActorPanics, 1)
	attributes := []slog.Attr{slog.String("operation", operation)}
	if session != nil {
		attributes = append(attributes,
			slog.String("actor_id", session.AID()),
			slog.Uint64("generation", session.Generation()),
		)
	}
	p.log(slog.LevelError, "actor handler panicked", attributes...)
}

func (p *Pump) counter(name string, delta int64) {
	if p == nil || delta == 0 {
		return
	}
	p.hooks.enqueue(hookObservation{kind: hookCounter, name: name, value: delta})
}

func (p *Pump) gauge(name string, value int64) {
	if p == nil {
		return
	}
	p.hooks.enqueue(hookObservation{kind: hookGauge, name: name, value: value})
}

func (p *Pump) observeDuration(name string, value time.Duration) {
	if p == nil {
		return
	}
	p.hooks.enqueue(hookObservation{kind: hookDuration, name: name, duration: value})
}

func (p *Pump) log(level slog.Level, message string, attributes ...slog.Attr) {
	if p == nil || p.logger == nil {
		return
	}
	p.logger.LogAttrs(context.Background(), level, message, attributes...)
}

func errorCode(err error) string {
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) {
		return coded.ErrorCode()
	}
	return ""
}

type hookKind uint8

const (
	hookCounter hookKind = iota + 1
	hookGauge
	hookDuration
)

type hookObservation struct {
	kind     hookKind
	name     string
	value    int64
	duration time.Duration
}

// hookDispatcher keeps user hooks off the poll, submit, and actor goroutines.
// Queueing is safe while a pump state lock is held because user code runs only
// after the dispatcher releases its own mutex.
type hookDispatcher struct {
	sink    Hooks
	logger  *slog.Logger
	mu      sync.Mutex
	cond    *sync.Cond
	queue   []hookObservation
	stopNow bool
	started bool
	done    chan struct{}
}

func newHookDispatcher(sink Hooks, logger *slog.Logger) *hookDispatcher {
	if sink == nil {
		return nil
	}
	dispatcher := &hookDispatcher{sink: sink, logger: logger, done: make(chan struct{})}
	dispatcher.cond = sync.NewCond(&dispatcher.mu)
	return dispatcher
}

func (d *hookDispatcher) start() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.mu.Unlock()
	go d.run()
}

func (d *hookDispatcher) enqueue(observation hookObservation) {
	if d == nil {
		return
	}
	d.mu.Lock()
	if !d.stopNow {
		d.queue = append(d.queue, observation)
		d.cond.Signal()
	}
	d.mu.Unlock()
}

func (d *hookDispatcher) stop() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.stopNow = true
	d.cond.Broadcast()
	d.mu.Unlock()
	<-d.done
}

func (d *hookDispatcher) run() {
	defer close(d.done)
	for {
		d.mu.Lock()
		for len(d.queue) == 0 && !d.stopNow {
			d.cond.Wait()
		}
		if len(d.queue) == 0 && d.stopNow {
			d.mu.Unlock()
			return
		}
		observation := d.queue[0]
		d.queue[0] = hookObservation{}
		d.queue = d.queue[1:]
		d.mu.Unlock()
		d.call(observation)
	}
}

func (d *hookDispatcher) call(observation hookObservation) {
	defer func() {
		if recovered := recover(); recovered != nil && d.logger != nil {
			d.logger.Warn("operability hook panicked", slog.Any("panic", recovered))
		}
	}()
	switch observation.kind {
	case hookCounter:
		d.sink.Counter(observation.name, observation.value)
	case hookGauge:
		d.sink.Gauge(observation.name, observation.value)
	case hookDuration:
		d.sink.ObserveDuration(observation.name, observation.duration)
	}
}
