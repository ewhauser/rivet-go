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
