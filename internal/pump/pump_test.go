package pump

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/internal/wire"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/goleak"
)

type fakeRunner struct {
	events       chan []byte
	closed       chan struct{}
	closeOnce    sync.Once
	polls        atomic.Int32
	maxPolls     atomic.Int32
	commandCount atomic.Int64
	nextSeq      atomic.Uint64
	stoppedPoll  atomic.Bool
	closeEarly   atomic.Bool
	submitted    chan wire.Command
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		events:    make(chan []byte, 128),
		closed:    make(chan struct{}),
		submitted: make(chan wire.Command, 4096),
	}
}

func (r *fakeRunner) Poll(timeout time.Duration) ([]byte, error) {
	active := r.polls.Add(1)
	defer r.polls.Add(-1)
	for {
		maximum := r.maxPolls.Load()
		if active <= maximum || r.maxPolls.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case event := <-r.events:
		var batch wire.EventBatch
		if err := decodeEventBatch(event, &batch); err == nil {
			for _, item := range batch.Events {
				if item.Kind == wire.EventRunnerStopped {
					r.stoppedPoll.Store(true)
				}
			}
		}
		return event, nil
	case <-time.After(timeout):
		return encodeEventBatch(r.nextSeq.Add(1)), nil
	}
}

func (r *fakeRunner) Submit(data []byte) error {
	var batch wire.CommandBatch
	if err := decodeCommandBatch(data, &batch); err != nil {
		return err
	}
	r.commandCount.Add(int64(len(batch.Commands)))
	for _, command := range batch.Commands {
		r.submitted <- command
	}
	return nil
}

func (r *fakeRunner) Shutdown(time.Duration) error {
	r.events <- encodeEventBatch(r.nextSeq.Add(1), wire.Event{
		Kind: wire.EventRunnerStopped,
		DrainReport: &wire.DrainReport{
			Graceful: true,
		},
	})
	return nil
}

func (r *fakeRunner) Close() {
	if !r.stoppedPoll.Load() {
		r.closeEarly.Store(true)
	}
	r.closeOnce.Do(func() { close(r.closed) })
}

func (r *fakeRunner) emit(events ...wire.Event) {
	r.events <- encodeEventBatch(r.nextSeq.Add(1), events...)
}

func TestConcurrentSubmittersAndCleanShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	p := New(runner)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- p.Run(ctx) }()
	for !p.started.Load() {
		time.Sleep(time.Millisecond)
	}

	const goroutines = 24
	const submitsPerGoroutine = 40
	var wait sync.WaitGroup
	wait.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wait.Done()
			for j := 0; j < submitsPerGoroutine; j++ {
				if err := p.Submit(context.Background(), wire.Command{Kind: "test"}); err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
			}
		}()
	}
	wait.Wait()
	if got, want := runner.commandCount.Load(), int64(goroutines*submitsPerGoroutine); got != want {
		t.Fatalf("submitted command count = %d, want %d", got, want)
	}
	cancel()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pump did not shut down")
	}
	if runner.maxPolls.Load() != 1 {
		t.Fatalf("maximum concurrent polls = %d, want 1", runner.maxPolls.Load())
	}
	if runner.closeEarly.Load() {
		t.Fatal("runner closed before the pump polled RunnerStopped")
	}
	select {
	case <-runner.closed:
	default:
		t.Fatal("runner was not closed")
	}
}

func TestDispatchesRunnerStoppedBeforeClosingSubscription(t *testing.T) {
	runner := newFakeRunner()
	p := New(runner)
	subscription := p.Subscribe(1)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	cancel()

	event, ok := <-subscription.Events
	if !ok {
		t.Fatal("subscription closed before RunnerStopped")
	}
	if event.Kind != wire.EventRunnerStopped {
		t.Fatalf("event kind = %q, want %q", event.Kind, wire.EventRunnerStopped)
	}
	if _, ok := <-subscription.Events; ok {
		t.Fatal("subscription remained open after pump exit")
	}
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestRunRejectsSecondStart(t *testing.T) {
	runner := newFakeRunner()
	p := New(runner)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	for !p.started.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := p.Run(ctx); err != ErrAlreadyStarted {
		t.Fatalf("second Run error = %v, want %v", err, ErrAlreadyStarted)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("first Run: %v", err)
	}
}

func TestRejectsNonMonotonicSequence(t *testing.T) {
	runner := newFakeRunner()
	p := New(runner)
	runner.events <- encodeEventBatch(2)
	runner.events <- encodeEventBatch(2)

	result := p.Run(context.Background())
	if result == nil || !strings.Contains(result.Error(), "non-monotonic event batch sequence") {
		t.Fatalf("Run error = %v, want non-monotonic sequence error", result)
	}
}

type lifecycleHandler struct {
	start func(context.Context, *ActorSession, wire.Event) (any, error)
	stop  func(context.Context, *ActorSession, wire.Event, any) error
}

func (h lifecycleHandler) Start(ctx context.Context, session *ActorSession, event wire.Event) (any, error) {
	if h.start == nil {
		return nil, nil
	}
	return h.start(ctx, session, event)
}

func (h lifecycleHandler) Stop(ctx context.Context, session *ActorSession, event wire.Event, state any) error {
	if h.stop == nil {
		return nil
	}
	return h.stop(ctx, session, event, state)
}

func TestActorStopWaitsForCleanupAndPreservesActorOrder(t *testing.T) {
	runner := newFakeRunner()
	cleanupStarted := make(chan struct{})
	cleanupRelease := make(chan struct{})
	handler := lifecycleHandler{
		start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
			return session.AID(), nil
		},
		stop: func(_ context.Context, session *ActorSession, _ wire.Event, state any) error {
			if state != session.AID() {
				return HandlerError{Code: "state_mismatch", Message: "actor-local state changed"}
			}
			close(cleanupStarted)
			<-cleanupRelease
			return nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"counter": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{
		Kind:       wire.EventActorStart,
		AID:        "actor-1",
		Generation: 3,
		Name:       "counter",
	})
	startResult := nextCommand(t, runner)
	if startResult.Kind != wire.CommandActorStartResult || !startResult.OK || startResult.Error != nil {
		t.Fatalf("ActorStartResult = %#v, want success", startResult)
	}

	runner.emit(wire.Event{
		Kind:       wire.EventActorStop,
		AID:        "actor-1",
		Generation: 3,
		Reason:     "destroy",
	})
	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("OnStop did not start")
	}
	select {
	case command := <-runner.submitted:
		t.Fatalf("ActorStopResult arrived before cleanup completed: %#v", command)
	case <-time.After(50 * time.Millisecond):
	}
	close(cleanupRelease)
	stopResult := nextCommand(t, runner)
	if stopResult.Kind != wire.CommandActorStopResult || stopResult.Error != nil {
		t.Fatalf("ActorStopResult = %#v, want success", stopResult)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestActorSaveCompletesWhileOnStartIsRunning(t *testing.T) {
	runner := newFakeRunner()
	handler := lifecycleHandler{
		start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
			return nil, session.Save(context.Background(), []byte(`{"count":7}`))
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"counter": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{
		Kind:       wire.EventActorStart,
		AID:        "actor-save",
		Generation: 1,
		Name:       "counter",
	})
	save := nextCommand(t, runner)
	if save.Kind != wire.CommandSaveState || string(save.State) != `{"count":7}` {
		t.Fatalf("SaveState = %#v", save)
	}
	runner.emit(wire.Event{
		Kind:         wire.EventStatePersisted,
		AID:          "actor-save",
		Generation:   1,
		StateVersion: 1,
	})
	startResult := nextCommand(t, runner)
	if startResult.Kind != wire.CommandActorStartResult || !startResult.OK {
		t.Fatalf("ActorStartResult = %#v, want success", startResult)
	}

	runner.emit(wire.Event{
		Kind:       wire.EventActorStop,
		AID:        "actor-save",
		Generation: 1,
		Reason:     "destroy",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("stop command = %#v", command)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestActorStopWaitsForConcurrentSave(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	sessionReady := make(chan *ActorSession, 1)
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"counter": lifecycleHandler{
			start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
				sessionReady <- session
				return nil, nil
			},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "actor-save-stop", Generation: 1, Name: "counter"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult || !command.OK {
		t.Fatalf("start command kind/status = %q/%t, want successful ActorStartResult", command.Kind, command.OK)
	}
	session := <-sessionReady
	saveResult := make(chan error, 1)
	go func() { saveResult <- session.Save(context.Background(), []byte(`{"count":8}`)) }()
	if command := nextCommand(t, runner); command.Kind != wire.CommandSaveState {
		t.Fatalf("command kind = %q, want SaveState", command.Kind)
	}

	runner.emit(wire.Event{Kind: wire.EventActorStop, AID: "actor-save-stop", Generation: 1, Reason: "destroy"})
	select {
	case command := <-runner.submitted:
		t.Fatalf("ActorStopResult arrived before the concurrent save completed: kind=%q", command.Kind)
	case <-time.After(50 * time.Millisecond):
	}
	runner.emit(wire.Event{Kind: wire.EventStatePersisted, AID: "actor-save-stop", Generation: 1, StateVersion: 1})
	if err := <-saveResult; err != nil {
		t.Fatalf("Save: %v", err)
	}
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("command kind = %q, want ActorStopResult", command.Kind)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestActorSaveWithoutCompletionTimesOutStructured(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"counter": lifecycleHandler{
			start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
				return nil, session.Save(context.Background(), []byte(`{"count":9}`))
			},
		},
	})
	p.saveTimeout = 40 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "actor-save-timeout", Generation: 1, Name: "counter"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandSaveState {
		t.Fatalf("command kind = %q, want SaveState", command.Kind)
	}
	startResult := nextCommand(t, runner)
	if startResult.Kind != wire.CommandActorStartResult || startResult.Error == nil || startResult.Error.Code != "state_persist_timeout" {
		t.Fatalf("start result kind/error = %q/%v, want state_persist_timeout", startResult.Kind, startResult.Error)
	}

	// A completion that races the actor's failed teardown is consumed as the
	// acknowledgement of the poisoned save, not treated as another actor's.
	runner.emit(wire.Event{Kind: wire.EventStatePersisted, AID: "actor-save-timeout", Generation: 1, StateVersion: 1})
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run after timed-out save: %v", err)
	}
}

func TestActorSaveAcceptsNextSaveAfterLateCompletion(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	sessionReady := make(chan *ActorSession, 1)
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"counter": lifecycleHandler{start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
			sessionReady <- session
			return nil, nil
		}},
	})
	ctx, cancelPump := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "actor-save-late", Generation: 1, Name: "counter"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult || !command.OK {
		t.Fatalf("start command kind/status = %q/%t", command.Kind, command.OK)
	}
	session := <-sessionReady

	saveCtx, cancelSave := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() { firstResult <- session.Save(saveCtx, []byte("first")) }()
	if command := nextCommand(t, runner); command.Kind != wire.CommandSaveState {
		t.Fatalf("first command kind = %q, want SaveState", command.Kind)
	}
	cancelSave()
	var canceled HandlerError
	if err := <-firstResult; !errors.As(err, &canceled) || canceled.Code != "state_persist_canceled" {
		t.Fatalf("canceled save error = %v, want state_persist_canceled", err)
	}
	runner.emit(wire.Event{Kind: wire.EventStatePersisted, AID: "actor-save-late", Generation: 1, StateVersion: 1})
	deadline := time.Now().Add(time.Second)
	for {
		session.saveStateMu.Lock()
		poisoned := session.savePoisoned
		session.saveStateMu.Unlock()
		if !poisoned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late save acknowledgement did not unblock the session")
		}
		time.Sleep(time.Millisecond)
	}

	secondResult := make(chan error, 1)
	go func() { secondResult <- session.Save(context.Background(), []byte("second")) }()
	if command := nextCommand(t, runner); command.Kind != wire.CommandSaveState {
		t.Fatalf("second command kind = %q, want SaveState", command.Kind)
	}
	runner.emit(wire.Event{Kind: wire.EventStatePersisted, AID: "actor-save-late", Generation: 1, StateVersion: 2})
	if err := <-secondResult; err != nil {
		t.Fatalf("second Save: %v", err)
	}

	runner.emit(wire.Event{Kind: wire.EventActorStop, AID: "actor-save-late", Generation: 1, Reason: "destroy"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("stop command kind = %q", command.Kind)
	}
	cancelPump()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestShutdownCancelsActorWithPendingSave(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	saveReturned := make(chan error, 1)
	handler := lifecycleHandler{
		start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
			err := session.Save(context.Background(), []byte(`{"count":7}`))
			saveReturned <- err
			return nil, err
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"counter": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{
		Kind:       wire.EventActorStart,
		AID:        "actor-save-shutdown",
		Generation: 1,
		Name:       "counter",
	})
	if save := nextCommand(t, runner); save.Kind != wire.CommandSaveState {
		t.Fatalf("command = %#v, want SaveState", save)
	}
	cancel()
	select {
	case err := <-saveReturned:
		if !errors.Is(err, ErrShuttingDown) {
			t.Fatalf("pending Save error = %v, want %v", err, ErrShuttingDown)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending Save did not return during shutdown")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pump did not finish with an actor Save pending")
	}
}

func TestShutdownCancelsPendingKV(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	kvReturned := make(chan error, 1)
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"kv": lifecycleHandler{start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
			go func() {
				_, _, err := session.KVGet(context.Background(), []byte("pending"))
				kvReturned <- err
			}()
			return nil, nil
		}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "actor-kv-shutdown", Generation: 1, Name: "kv"})
	commands := []wire.Command{nextCommand(t, runner), nextCommand(t, runner)}
	foundKV := false
	for _, command := range commands {
		foundKV = foundKV || command.Kind == wire.CommandKVGet
	}
	if !foundKV {
		t.Fatal("actor did not submit its pending KV request")
	}
	cancel()
	select {
	case err := <-kvReturned:
		if !errors.Is(err, ErrShuttingDown) {
			t.Fatalf("pending KV error = %v, want %v", err, ErrShuttingDown)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending KV did not return during runner shutdown")
	}
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestHandlerPanicIsActorLocalAndStructured(t *testing.T) {
	runner := newFakeRunner()
	healthyStarted := make(chan struct{}, 1)
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"panic": lifecycleHandler{start: func(context.Context, *ActorSession, wire.Event) (any, error) {
			panic("start failed")
		}},
		"healthy": lifecycleHandler{start: func(context.Context, *ActorSession, wire.Event) (any, error) {
			healthyStarted <- struct{}{}
			return nil, nil
		}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(
		wire.Event{Kind: wire.EventActorStart, AID: "actor-panic", Generation: 1, Name: "panic"},
		wire.Event{Kind: wire.EventActorStart, AID: "actor-healthy", Generation: 1, Name: "healthy"},
	)

	commands := map[string]wire.Command{}
	for range 2 {
		command := nextCommand(t, runner)
		commands[command.AID] = command
	}
	panicked := commands["actor-panic"]
	if panicked.OK || panicked.Error == nil || panicked.Error.Code != "handler_panic" {
		t.Fatalf("panic ActorStartResult = %#v", panicked)
	}
	healthy := commands["actor-healthy"]
	if !healthy.OK || healthy.Error != nil {
		t.Fatalf("healthy ActorStartResult = %#v", healthy)
	}
	select {
	case <-healthyStarted:
	default:
		t.Fatal("healthy actor did not run after peer panic")
	}

	runner.emit(wire.Event{
		Kind:       wire.EventActorStop,
		AID:        "actor-healthy",
		Generation: 1,
		Reason:     "destroy",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("stop command = %#v", command)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run after handler panic: %v", err)
	}
}

func TestOnStopPanicIsActorLocalAndStructured(t *testing.T) {
	runner := newFakeRunner()
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"panic-stop": lifecycleHandler{stop: func(context.Context, *ActorSession, wire.Event, any) error {
			panic("stop failed")
		}},
		"healthy": lifecycleHandler{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(
		wire.Event{Kind: wire.EventActorStart, AID: "actor-panic-stop", Generation: 1, Name: "panic-stop"},
		wire.Event{Kind: wire.EventActorStart, AID: "actor-healthy-stop", Generation: 1, Name: "healthy"},
	)
	for range 2 {
		if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult || !command.OK {
			t.Fatalf("start command kind/status = %q/%t", command.Kind, command.OK)
		}
	}

	runner.emit(wire.Event{Kind: wire.EventActorStop, AID: "actor-panic-stop", Generation: 1, Reason: "destroy"})
	panicked := nextCommand(t, runner)
	if panicked.Kind != wire.CommandActorStopResult || panicked.Error == nil || panicked.Error.Code != "handler_panic" {
		t.Fatalf("panic stop result kind/error = %q/%v", panicked.Kind, panicked.Error)
	}
	runner.emit(wire.Event{Kind: wire.EventActorStop, AID: "actor-healthy-stop", Generation: 1, Reason: "destroy"})
	if healthy := nextCommand(t, runner); healthy.Kind != wire.CommandActorStopResult || healthy.Error != nil {
		t.Fatalf("healthy stop result kind/error = %q/%v", healthy.Kind, healthy.Error)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run after OnStop panic: %v", err)
	}
}

func TestSlowActorDoesNotBlockFastActorAndPreservesOrder(t *testing.T) {
	runner := newFakeRunner()
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	slowStopped := make(chan struct{})
	fastStarted := make(chan struct{})
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"slow": lifecycleHandler{
			start: func(context.Context, *ActorSession, wire.Event) (any, error) {
				close(slowStarted)
				<-releaseSlow
				return nil, nil
			},
			stop: func(context.Context, *ActorSession, wire.Event, any) error {
				close(slowStopped)
				return nil
			},
		},
		"fast": lifecycleHandler{start: func(context.Context, *ActorSession, wire.Event) (any, error) {
			close(fastStarted)
			return nil, nil
		}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(
		wire.Event{Kind: wire.EventActorStart, AID: "slow", Generation: 1, Name: "slow"},
		wire.Event{Kind: wire.EventActorStop, AID: "slow", Generation: 1, Reason: "destroy"},
		wire.Event{Kind: wire.EventActorStart, AID: "fast", Generation: 1, Name: "fast"},
	)
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow actor did not start")
	}
	select {
	case <-fastStarted:
	case <-time.After(time.Second):
		t.Fatal("fast actor was blocked by the slow actor")
	}
	fastResult := nextCommand(t, runner)
	if fastResult.AID != "fast" || fastResult.Kind != wire.CommandActorStartResult {
		t.Fatalf("first command aid/kind = %q/%q, want fast ActorStartResult", fastResult.AID, fastResult.Kind)
	}
	select {
	case <-slowStopped:
		t.Fatal("slow actor stopped before its start hook completed")
	default:
	}
	close(releaseSlow)
	if command := nextCommand(t, runner); command.AID != "slow" || command.Kind != wire.CommandActorStartResult {
		t.Fatalf("slow command aid/kind = %q/%q, want ActorStartResult", command.AID, command.Kind)
	}
	if command := nextCommand(t, runner); command.AID != "slow" || command.Kind != wire.CommandActorStopResult {
		t.Fatalf("slow command aid/kind = %q/%q, want ActorStopResult", command.AID, command.Kind)
	}
	select {
	case <-slowStopped:
	case <-time.After(time.Second):
		t.Fatal("slow actor stop did not follow start")
	}
	runner.emit(wire.Event{Kind: wire.EventActorStop, AID: "fast", Generation: 1, Reason: "destroy"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("fast stop command kind = %q", command.Kind)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestKVCorrelationErrorAndPendingDrain(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	kvError := make(chan error, 1)
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"kv": lifecycleHandler{start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
			go func() {
				_, _, err := session.KVGet(context.Background(), []byte("pending"))
				kvError <- err
			}()
			return nil, nil
		}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "actor-kv", Generation: 1, Name: "kv"})
	commands := []wire.Command{nextCommand(t, runner), nextCommand(t, runner)}
	var kvID uint64
	for _, command := range commands {
		if command.Kind == wire.CommandKVGet {
			kvID = command.KVID
		}
	}
	if kvID == 0 {
		t.Fatal("KV allocation emitted kv_id 0")
	}
	runner.emit(wire.Event{
		Kind:  wire.EventKVResult,
		KVID:  kvID,
		Error: &wire.WireError{Code: "kv_read_failed", Message: "storage read failed"},
	})
	select {
	case err := <-kvError:
		var structured wire.WireError
		if !errors.As(err, &structured) || structured.Code != "kv_read_failed" {
			t.Fatalf("KV error = %v, want kv_read_failed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("KV error result did not resolve its waiter")
	}

	pendingError := make(chan error, 1)
	worker := p.actor("actor-kv", 1)
	go func() {
		_, _, err := worker.session.KVGet(context.Background(), []byte("drain"))
		pendingError <- err
	}()
	if command := nextCommand(t, runner); command.Kind != wire.CommandKVGet || command.KVID == kvID || command.KVID == 0 {
		t.Fatalf("second KV command kind/id = %q/%d; first id was %d", command.Kind, command.KVID, kvID)
	}
	runner.emit(wire.Event{Kind: wire.EventActorStop, AID: "actor-kv", Generation: 1, Reason: "destroy"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("stop command kind = %q", command.Kind)
	}
	select {
	case err := <-pendingError:
		if !errors.Is(err, ErrShuttingDown) {
			t.Fatalf("pending KV error = %v, want %v", err, ErrShuttingDown)
		}
	case <-time.After(time.Second):
		t.Fatal("pending KV waiter was not drained on actor stop")
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestKVIDSkipsZeroAndReusesOnlyCompletedID(t *testing.T) {
	runner := newFakeRunner()
	sessionReady := make(chan *ActorSession, 1)
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"kv": lifecycleHandler{start: func(_ context.Context, session *ActorSession, _ wire.Event) (any, error) {
			sessionReady <- session
			return nil, nil
		}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "actor-kv-wrap", Generation: 1, Name: "kv"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult || !command.OK {
		t.Fatalf("start command kind/status = %q/%t", command.Kind, command.OK)
	}
	session := <-sessionReady

	p.nextKVID.Store(^uint64(0))
	firstResult := make(chan error, 1)
	go func() {
		_, _, err := session.KVGet(context.Background(), []byte("first"))
		firstResult <- err
	}()
	first := nextCommand(t, runner)
	if first.Kind != wire.CommandKVGet || first.KVID != 1 {
		t.Fatalf("first KV command kind/id = %q/%d, want kv_get/1", first.Kind, first.KVID)
	}

	// Force another wrap while ID 1 is still pending. Allocation must skip both
	// zero and the live correlation rather than overwriting its waiter.
	p.nextKVID.Store(^uint64(0))
	secondResult := make(chan error, 1)
	go func() {
		_, _, err := session.KVGet(context.Background(), []byte("second"))
		secondResult <- err
	}()
	second := nextCommand(t, runner)
	if second.Kind != wire.CommandKVGet || second.KVID != 2 {
		t.Fatalf("second KV command kind/id = %q/%d, want kv_get/2", second.Kind, second.KVID)
	}
	runner.emit(
		wire.Event{Kind: wire.EventKVResult, KVID: second.KVID, Value: []byte("second")},
		wire.Event{Kind: wire.EventKVResult, KVID: first.KVID, Value: []byte("first")},
	)
	if err := <-secondResult; err != nil {
		t.Fatalf("second KVGet: %v", err)
	}
	if err := <-firstResult; err != nil {
		t.Fatalf("first KVGet: %v", err)
	}

	// Once ID 1 completed, it is safe to reuse after another wrap.
	p.nextKVID.Store(^uint64(0))
	reusedResult := make(chan error, 1)
	go func() {
		_, _, err := session.KVGet(context.Background(), []byte("reused"))
		reusedResult <- err
	}()
	reused := nextCommand(t, runner)
	if reused.Kind != wire.CommandKVGet || reused.KVID != 1 {
		t.Fatalf("reused KV command kind/id = %q/%d, want kv_get/1", reused.Kind, reused.KVID)
	}
	runner.emit(wire.Event{Kind: wire.EventKVResult, KVID: reused.KVID, Value: []byte("reused")})
	if err := <-reusedResult; err != nil {
		t.Fatalf("reused KVGet: %v", err)
	}

	runner.emit(wire.Event{Kind: wire.EventActorStop, AID: "actor-kv-wrap", Generation: 1, Reason: "destroy"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("stop command kind = %q", command.Kind)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func waitPumpStarted(t *testing.T, p *Pump) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !p.started.Load() {
		if time.Now().After(deadline) {
			t.Fatal("pump did not start")
		}
		time.Sleep(time.Millisecond)
	}
}

func nextCommand(t *testing.T, runner *fakeRunner) wire.Command {
	t.Helper()
	select {
	case command := <-runner.submitted:
		return command
	case <-time.After(2 * time.Second):
		t.Fatal("no submitted command")
		return wire.Command{}
	}
}

func encodeEventBatch(seq uint64, events ...wire.Event) []byte {
	data, err := msgpack.Marshal(wire.EventBatch{Seq: seq, Events: events})
	if err != nil {
		panic(err)
	}
	return data
}

func decodeCommandBatch(data []byte, batch *wire.CommandBatch) error {
	return msgpack.Unmarshal(data, batch)
}

func decodeEventBatch(data []byte, batch *wire.EventBatch) error {
	return msgpack.Unmarshal(data, batch)
}
