// Package pump turns the blocking native event pump into Go subscriptions,
// per-actor dispatch, and a concurrent batched submit API.
package pump

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	defaultSubmitQueue     = 1_024
	maxSubmitBatch         = 1_024
	actorEventQueue        = 64
	maxHTTPChunk           = 1 << 20
	maxWSMessage           = 1 << 20
)

var (
	ErrAlreadyStarted = errors.New("pump already started")
	ErrNotStarted     = errors.New("pump is not running")
	ErrShuttingDown   = errors.New("pump is shutting down")
)

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
	input          []byte
	persistedState []byte
	done           chan struct{}

	saveMu       sync.Mutex
	saveResult   chan error
	savePoisoned bool
	saveStateMu  sync.Mutex

	lifecycleMu   sync.Mutex
	lifecycleCond *sync.Cond
	accepting     bool
	activeSaves   int
	doneClosed    bool
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
	result := make(chan wire.Event, 1)
	operationID := s.pump.addActorIntentWaiter(result)
	command.OperationID = operationID
	command.AID = s.identity.aid
	command.Generation = s.identity.generation
	if err := s.pump.submitInternal(context.Background(), command); err != nil {
		s.pump.removeActorIntentWaiter(operationID)
		return err
	}
	timer := time.NewTimer(s.pump.intentTimeout)
	defer timer.Stop()
	select {
	case event := <-result:
		if event.Error != nil {
			return HandlerError{
				Code:    event.Error.Code,
				Message: event.Error.Message,
				Cause:   *event.Error,
			}
		}
		return nil
	case <-timer.C:
		s.pump.abandonActorIntentWaiter(operationID)
		return HandlerError{
			Code:    "actor_intent_timeout",
			Message: fmt.Sprintf("actor intent was not acknowledged within %s", s.pump.intentTimeout),
		}
	case <-s.done:
		s.pump.abandonActorIntentWaiter(operationID)
		return ErrShuttingDown
	case <-s.pump.done:
		s.pump.abandonActorIntentWaiter(operationID)
		return ErrShuttingDown
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

// KVGet performs an actor-scoped core KV lookup. KV is retained at the M2
// boundary for compatibility even though the public typed SDK uses state.
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

func (s *ActorSession) finishSave() {
	s.lifecycleMu.Lock()
	s.activeSaves--
	if s.activeSaves == 0 {
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
	for s.activeSaves != 0 {
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
	for s.activeSaves != 0 {
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
	events  chan wire.Event
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
}

func New(runner Runner) *Pump {
	return NewWithHandlers(runner, nil)
}

func NewWithHandlers(runner Runner, handlers map[string]ActorHandler) *Pump {
	ownedHandlers := make(map[string]ActorHandler, len(handlers))
	for name, handler := range handlers {
		ownedHandlers[name] = handler
	}
	return &Pump{
		runner:          runner,
		pollTimeout:     defaultPollTimeout,
		shutdownTimeout: defaultShutdownTimeout,
		saveTimeout:     defaultSaveTimeout,
		intentTimeout:   defaultIntentTimeout,
		httpSubmitLimit: defaultHTTPSubmitLimit,
		wsSubmitLimit:   defaultWSSubmitLimit,
		handlers:        ownedHandlers,
		submitQueue:     make(chan submitRequest, defaultSubmitQueue),
		submitStop:      make(chan struct{}),
		done:            make(chan struct{}),
		workerErrors:    make(chan error, 1),
		actors:          make(map[actorIdentity]*actorWorker),
		kvPending:       make(map[uint64]chan wire.Event),
		intentPending:   make(map[uint64]chan wire.Event),
		lateSaves:       make(map[actorIdentity]struct{}),
		httpPending:     make(map[uint64]*httpRequestState),
		websockets:      make(map[string]websocketOwner),
		subs:            make(map[uint64]*subscriber),
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
				if err := p.runner.Shutdown(p.shutdownTimeout); err != nil {
					p.setResult(fmt.Errorf("shutdown native runner: %w", err))
					return
				}
			default:
			}
		}

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
		for _, event := range batch.Events {
			if err := p.handleInternalEvent(event); err != nil {
				p.setResult(err)
				return
			}
			if !p.dispatch(event) {
				return
			}
			if event.Kind == wire.EventRunnerStopped {
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
			for _, request := range requests {
				request.result <- err
			}
		}
	}
}

func (p *Pump) handleInternalEvent(event wire.Event) error {
	switch event.Kind {
	case wire.EventActorStart:
		identity := actorIdentity{aid: event.AID, generation: event.Generation}
		workerCtx, cancel := context.WithCancel(context.Background())
		session := &ActorSession{
			pump:           p,
			identity:       identity,
			input:          cloneBytes(event.Input),
			persistedState: cloneBytes(event.PersistedState),
			done:           make(chan struct{}),
			accepting:      true,
		}
		session.lifecycleCond = sync.NewCond(&session.lifecycleMu)
		worker := &actorWorker{
			pump:    p,
			handler: p.handlers[event.Name],
			session: session,
			ctx:     workerCtx,
			cancel:  cancel,
			events:  make(chan wire.Event, actorEventQueue),
		}
		p.actorsMu.Lock()
		if _, exists := p.actors[identity]; exists {
			p.actorsMu.Unlock()
			cancel()
			return fmt.Errorf("duplicate ActorStart for %s generation %d", event.AID, event.Generation)
		}
		p.actors[identity] = worker
		p.actorWG.Add(1)
		p.actorsMu.Unlock()
		go worker.run(event)
	case wire.EventActorStop:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			return fmt.Errorf("ActorStop for unknown actor %s generation %d", event.AID, event.Generation)
		}
		closeEvents, hibernateCommands := p.detachActorWebSockets(
			worker.session.identity,
			event.Reason == "sleep",
			"actor stopped",
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
			worker.events <- closeEvent
		}
		worker.events <- event
	case wire.EventActorAlarm:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			return fmt.Errorf("ActorAlarm for unknown actor %s generation %d", event.AID, event.Generation)
		}
		worker.events <- event
	case wire.EventActorIntentResult:
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
		worker.events <- event
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
		worker.events <- event
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
		p.wsMu.Unlock()
		worker.events <- event
	case wire.EventWSMessage:
		worker := p.websocketActor(event.WSID)
		if worker == nil {
			return nil
		}
		worker.events <- event
	case wire.EventWSClose:
		worker := p.removeWebSocket(event.WSID)
		if worker == nil {
			return nil
		}
		worker.events <- event
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
	}
	return nil
}

func (w *actorWorker) run(start wire.Event) {
	defer w.pump.actorWG.Done()
	defer w.cancel()
	defer w.pump.removeActor(w.session.identity, w)
	defer w.session.abort()

	state, lifecycleError := invokeStart(w.ctx, w.handler, w.session, start)
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
		var event wire.Event
		select {
		case <-w.ctx.Done():
			return
		case event = <-w.events:
		}
		switch event.Kind {
		case wire.EventActionCall:
			actionTimeout := time.Duration(event.ActionTimeoutMS) * time.Millisecond
			actionContext, cancelAction := context.WithTimeout(w.ctx, actionTimeout)
			output, actionError := invokeAction(actionContext, w.handler, w.session, event, state)
			cancelAction()
			if actionError == nil && output == nil {
				output = []byte{}
			}
			if err := w.pump.submitInternal(w.ctx, wire.Command{
				Kind:   wire.CommandActionResult,
				CallID: event.CallID,
				Output: output,
				Error:  actionError,
			}); err != nil {
				w.pump.reportWorkerError(fmt.Errorf("submit ActionResult: %w", err))
				return
			}
			if actionError != nil && actionError.Code == "handler_panic" {
				return
			}
		case wire.EventActorAlarm:
			alarmError := invokeAlarm(w.ctx, w.handler, w.session, event, state)
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
			messageError := invokeWebSocketMessage(w.ctx, w.handler, w.session, event, state)
			if err := w.pump.submitInternal(w.ctx, wire.Command{
				Kind:         wire.CommandWSMessageAck,
				WSID:         event.WSID,
				MessageIndex: event.MessageIndex,
			}); err != nil {
				w.pump.reportWorkerError(fmt.Errorf("submit WsMessageAck: %w", err))
				return
			}
			if messageError != nil && messageError.Code == "handler_panic" {
				w.requestErrorStop()
			}
		case wire.EventWSClose:
			closeError := invokeWebSocketClose(w.ctx, w.handler, w.session, event, state)
			if closeError != nil && closeError.Code == "handler_panic" {
				w.requestErrorStop()
			}
		case wire.EventActorStop:
			stopReason = event.Reason
			lifecycleError = invokeStop(w.ctx, w.handler, w.session, event, state)
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

func (p *Pump) removeWebSocket(wsID string) *actorWorker {
	p.wsMu.Lock()
	owner, ok := p.websockets[wsID]
	delete(p.websockets, wsID)
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
	if p.actors[identity] == worker {
		delete(p.actors, identity)
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
