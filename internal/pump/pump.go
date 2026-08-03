// Package pump turns the blocking native event pump into Go subscriptions,
// per-actor dispatch, and a concurrent batched submit API.
package pump

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ewhauser/rivet-go/internal/wire"
)

const (
	defaultPollTimeout     = 100 * time.Millisecond
	defaultShutdownTimeout = 10 * time.Second
	defaultSubmitQueue     = 1_024
	maxSubmitBatch         = 64
	actorEventQueue        = 64
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

// HandlerError is sent to Rust as the structured actor-local error arm of a
// lifecycle result. It never terminates the shared pump.
type HandlerError struct {
	Code    string
	Message string
}

func (e HandlerError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

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
	done           <-chan struct{}

	saveMu      sync.Mutex
	saveResult  chan error
	saveStateMu sync.Mutex
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
	return append([]byte(nil), s.persistedState...)
}

// Save persists one complete actor-state snapshot through rivetkit-core and
// waits for StatePersisted. Once enqueued, the save observes its native result
// even if the caller context is canceled, preventing an uncorrelated late ack
// from being mistaken for a later save on the same actor.
func (s *ActorSession) Save(ctx context.Context, state []byte) error {
	if s == nil || s.pump == nil {
		return errors.New("actor session is unavailable")
	}
	if ctx == nil {
		return errors.New("save context is nil")
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	result := make(chan error, 1)
	s.saveStateMu.Lock()
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

	select {
	case err := <-result:
		return err
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
	kvID := s.pump.nextKVID.Add(1)
	command.KVID = kvID
	result := make(chan wire.Event, 1)
	s.pump.kvMu.Lock()
	s.pump.kvPending[kvID] = result
	s.pump.kvMu.Unlock()

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

type actorWorker struct {
	pump    *Pump
	handler ActorHandler
	session *ActorSession
	ctx     context.Context
	cancel  context.CancelFunc
	events  chan wire.Event
}

// Pump owns runner and closes it after RunnerStopped has been dispatched.
type Pump struct {
	runner          Runner
	pollTimeout     time.Duration
	shutdownTimeout time.Duration
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

	nextKVID  atomic.Uint64
	kvMu      sync.Mutex
	kvPending map[uint64]chan wire.Event

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
		handlers:        ownedHandlers,
		submitQueue:     make(chan submitRequest, defaultSubmitQueue),
		submitStop:      make(chan struct{}),
		done:            make(chan struct{}),
		workerErrors:    make(chan error, 1),
		actors:          make(map[actorIdentity]*actorWorker),
		kvPending:       make(map[uint64]chan wire.Event),
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
			for len(requests) < maxSubmitBatch {
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
		worker := &actorWorker{
			pump:    p,
			handler: p.handlers[event.Name],
			session: &ActorSession{
				pump:           p,
				identity:       identity,
				input:          append([]byte(nil), event.Input...),
				persistedState: append([]byte(nil), event.PersistedState...),
				done:           workerCtx.Done(),
			},
			ctx:    workerCtx,
			cancel: cancel,
			events: make(chan wire.Event, actorEventQueue),
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
		worker.events <- event
	case wire.EventStatePersisted:
		worker := p.actor(event.AID, event.Generation)
		if worker == nil {
			return fmt.Errorf("StatePersisted for unknown actor %s generation %d", event.AID, event.Generation)
		}
		if !worker.session.completeSave(event) {
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

	for {
		var event wire.Event
		select {
		case <-w.ctx.Done():
			return
		case event = <-w.events:
		}
		if event.Kind != wire.EventActorStop {
			w.pump.reportWorkerError(fmt.Errorf(
				"unexpected %s on actor worker %s generation %d",
				event.Kind,
				w.session.identity.aid,
				w.session.identity.generation,
			))
			return
		}
		lifecycleError = invokeStop(w.ctx, w.handler, w.session, event, state)
		if err := w.pump.submitInternal(w.ctx, wire.Command{
			Kind:       wire.CommandActorStopResult,
			AID:        w.session.identity.aid,
			Generation: w.session.identity.generation,
			Error:      lifecycleError,
		}); err != nil {
			w.pump.reportWorkerError(fmt.Errorf("submit ActorStopResult: %w", err))
		}
		return
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

func (p *Pump) removeActor(identity actorIdentity, worker *actorWorker) {
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
	}
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

func (p *Pump) failKVWaiters() {
	p.kvMu.Lock()
	for kvID := range p.kvPending {
		delete(p.kvPending, kvID)
	}
	p.kvMu.Unlock()
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
