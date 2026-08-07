package pump

import (
	"context"
	"errors"
	"fmt"
	"io"
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

type retryRunner struct {
	*fakeRunner
	remaining atomic.Int32
	attempts  atomic.Int32
}

type codedTestError struct {
	code string
}

type recordingHooks struct {
	mu       sync.Mutex
	counters map[string]int64
	gauges   map[string]int64
	observed map[string][]time.Duration
}

type reentrantBlockingHooks struct {
	pump      *Pump
	once      sync.Once
	started   chan struct{}
	release   chan struct{}
	reentered chan error
}

func (h *reentrantBlockingHooks) Counter(name string, _ int64) {
	if name != metricCommandsSubmitted {
		return
	}
	h.once.Do(func() {
		close(h.started)
		<-h.release
		h.reentered <- h.pump.Submit(context.Background())
	})
}

func (*reentrantBlockingHooks) Gauge(string, int64)                   {}
func (*reentrantBlockingHooks) ObserveDuration(string, time.Duration) {}

func newRecordingHooks() *recordingHooks {
	return &recordingHooks{
		counters: make(map[string]int64),
		gauges:   make(map[string]int64),
		observed: make(map[string][]time.Duration),
	}
}

func (h *recordingHooks) Counter(name string, delta int64) {
	h.mu.Lock()
	h.counters[name] += delta
	h.mu.Unlock()
}

func (h *recordingHooks) Gauge(name string, value int64) {
	h.mu.Lock()
	h.gauges[name] = value
	h.mu.Unlock()
}

func (h *recordingHooks) ObserveDuration(name string, value time.Duration) {
	h.mu.Lock()
	h.observed[name] = append(h.observed[name], value)
	h.mu.Unlock()
}

func (h *recordingHooks) snapshot() (map[string]int64, map[string]int64, map[string][]time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	counters := make(map[string]int64, len(h.counters))
	for name, value := range h.counters {
		counters[name] = value
	}
	gauges := make(map[string]int64, len(h.gauges))
	for name, value := range h.gauges {
		gauges[name] = value
	}
	observed := make(map[string][]time.Duration, len(h.observed))
	for name, values := range h.observed {
		observed[name] = append([]time.Duration(nil), values...)
	}
	return counters, gauges, observed
}

func (e codedTestError) Error() string     { return e.code }
func (e codedTestError) ErrorCode() string { return e.code }

func (r *retryRunner) Submit(data []byte) error {
	r.attempts.Add(1)
	for {
		remaining := r.remaining.Load()
		if remaining == 0 {
			return r.fakeRunner.Submit(data)
		}
		if r.remaining.CompareAndSwap(remaining, remaining-1) {
			return codedTestError{code: "backpressure"}
		}
	}
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

func TestHooksMayBlockBrieflyAndReenterPump(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	hooks := &reentrantBlockingHooks{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		reentered: make(chan error, 1),
	}
	p := NewWithOptions(runner, nil, Options{Hooks: hooks})
	hooks.pump = p
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	submitResult := make(chan error, 1)
	go func() {
		submitResult <- p.Submit(context.Background(), wire.Command{Kind: "test"})
	}()
	select {
	case <-hooks.started:
	case <-time.After(2 * time.Second):
		t.Fatal("command metric hook was not called")
	}
	select {
	case err := <-submitResult:
		if err != nil {
			t.Fatalf("Submit while hook blocked: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("blocking hook stalled the submit loop")
	}
	close(hooks.release)
	select {
	case err := <-hooks.reentered:
		if err != nil {
			t.Fatalf("hook reentrant Submit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hook reentrant Submit deadlocked")
	}

	cancel()
	if err := <-runResult; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestPollLatencyExcludesIdleTimeouts(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	hooks := newRecordingHooks()
	p := NewWithOptions(runner, nil, Options{Hooks: hooks})
	p.pollTimeout = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	deadline := time.Now().Add(time.Second)
	for runner.nextSeq.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if runner.nextSeq.Load() < 3 {
		t.Fatal("runner did not complete idle poll timeouts")
	}
	_, _, observed := hooks.snapshot()
	if len(observed[metricPollLatency]) != 0 {
		t.Fatalf("idle timeouts recorded as poll latency: %v", observed[metricPollLatency])
	}

	runner.emit(wire.Event{Kind: wire.EventRunnerConnected, RunnerID: "runner"})
	for {
		_, _, observed = hooks.snapshot()
		if len(observed[metricPollLatency]) != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("event-bearing poll latency was not observed")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-runResult; err != nil {
		t.Fatalf("Run: %v", err)
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

func TestNonGracefulDrainReturnsAnError(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	p := New(runner)
	result := make(chan error, 1)
	go func() { result <- p.Run(context.Background()) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{
		Kind: wire.EventRunnerStopped,
		DrainReport: &wire.DrainReport{
			Graceful:        false,
			ActorsRemaining: 2,
		},
	})
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "2 actors remaining") {
			t.Fatalf("non-graceful drain error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pump did not return after non-graceful RunnerStopped")
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

type dispatchHandler struct {
	lifecycleHandler
	action     func(context.Context, *ActorSession, wire.Event, any) ([]byte, error)
	alarm      func(context.Context, *ActorSession, wire.Event, any) error
	fetch      func(context.Context, *ActorSession, wire.Event, any) error
	wsOpen     func(context.Context, *ActorSession, wire.Event, any) error
	wsMessage  func(context.Context, *ActorSession, wire.Event, any) error
	wsClose    func(context.Context, *ActorSession, wire.Event, any) error
	wsCloseAll func(context.Context, *ActorSession, any, string)
}

func (h dispatchHandler) WebSocketOpen(
	ctx context.Context,
	session *ActorSession,
	event wire.Event,
	state any,
) error {
	if h.wsOpen == nil {
		return nil
	}
	return h.wsOpen(ctx, session, event, state)
}

func (h dispatchHandler) WebSocketMessage(
	ctx context.Context,
	session *ActorSession,
	event wire.Event,
	state any,
) error {
	if h.wsMessage == nil {
		return nil
	}
	return h.wsMessage(ctx, session, event, state)
}

func (h dispatchHandler) WebSocketClose(
	ctx context.Context,
	session *ActorSession,
	event wire.Event,
	state any,
) error {
	if h.wsClose == nil {
		return nil
	}
	return h.wsClose(ctx, session, event, state)
}

func (h dispatchHandler) CloseWebSockets(
	ctx context.Context,
	session *ActorSession,
	state any,
	reason string,
) {
	if h.wsCloseAll != nil {
		h.wsCloseAll(ctx, session, state, reason)
	}
}

func (h dispatchHandler) Alarm(
	ctx context.Context,
	session *ActorSession,
	event wire.Event,
	state any,
) error {
	if h.alarm == nil {
		return nil
	}
	return h.alarm(ctx, session, event, state)
}

func (h dispatchHandler) Action(
	ctx context.Context,
	session *ActorSession,
	event wire.Event,
	state any,
) ([]byte, error) {
	return h.action(ctx, session, event, state)
}

func (h dispatchHandler) Fetch(
	ctx context.Context,
	session *ActorSession,
	event wire.Event,
	state any,
) error {
	return h.fetch(ctx, session, event, state)
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

func TestActorAlarmIsOrderedAndAcknowledged(t *testing.T) {
	runner := newFakeRunner()
	actionStarted := make(chan struct{})
	actionRelease := make(chan struct{})
	seen := make(chan string, 1)
	handler := dispatchHandler{
		action: func(context.Context, *ActorSession, wire.Event, any) ([]byte, error) {
			close(actionStarted)
			<-actionRelease
			return []byte("done"), nil
		},
		alarm: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) error {
			seen <- fmt.Sprintf("alarm:%d", event.AlarmTS)
			return nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"scheduled": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{
		Kind: wire.EventActorStart, AID: "scheduled-aid", Generation: 2, Name: "scheduled",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(
		wire.Event{
			Kind: wire.EventActionCall, AID: "scheduled-aid", Generation: 2,
			CallID: 31, Action: "before-alarm", ActionTimeoutMS: 1_000,
		},
		wire.Event{
			Kind: wire.EventActorAlarm, AID: "scheduled-aid", Generation: 2,
			AlarmTS: 1_788_500_000_000,
		},
	)
	select {
	case <-actionStarted:
	case <-time.After(time.Second):
		t.Fatal("long action did not start")
	}
	select {
	case got := <-seen:
		t.Fatalf("alarm ran while action was active: %q", got)
	default:
	}
	close(actionRelease)
	if command := nextCommand(t, runner); command.Kind != wire.CommandActionResult {
		t.Fatalf("action command = %#v", command)
	}
	if got := <-seen; got != "alarm:1788500000000" {
		t.Fatalf("serialized callback = %q, want alarm", got)
	}
	if command := nextCommand(t, runner); command.Kind != wire.CommandAlarmHandled ||
		command.AID != "scheduled-aid" || command.Generation != 2 || command.Error != nil {
		t.Fatalf("alarm command = %#v", command)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestActorWithoutOnAlarmReturnsStructuredError(t *testing.T) {
	runner := newFakeRunner()
	p := NewWithHandlers(runner, map[string]ActorHandler{"no-alarm": lifecycleHandler{}})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{
		Kind: wire.EventActorStart, AID: "no-alarm-aid", Generation: 3, Name: "no-alarm",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(wire.Event{
		Kind: wire.EventActorAlarm, AID: "no-alarm-aid", Generation: 3, AlarmTS: 1_788_500_000_000,
	})
	command := nextCommand(t, runner)
	if command.Kind != wire.CommandAlarmHandled || command.Error == nil ||
		command.Error.Code != "callback_not_found" {
		t.Fatalf("alarm command = %#v, want structured callback_not_found", command)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestActorIntentsAreGenerationFencedAndWaitForCompletion(t *testing.T) {
	runner := newFakeRunner()
	sessions := make(chan *ActorSession, 1)
	handler := lifecycleHandler{start: func(
		_ context.Context,
		session *ActorSession,
		_ wire.Event,
	) (any, error) {
		sessions <- session
		return nil, nil
	}}
	p := NewWithHandlers(runner, map[string]ActorHandler{"intent": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{
		Kind: wire.EventActorStart, AID: "intent-aid", Generation: 9, Name: "intent",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	session := <-sessions
	alarmTS := int64(1_788_500_000_000)
	alarmResult := make(chan error, 1)
	go func() { alarmResult <- session.SetAlarm(&alarmTS) }()
	alarmCommand := nextCommand(t, runner)
	if alarmCommand.Kind != wire.CommandSetAlarm || alarmCommand.AID != "intent-aid" ||
		alarmCommand.Generation != 9 || alarmCommand.OperationID == 0 {
		t.Fatalf("set alarm command = %#v", alarmCommand)
	}
	select {
	case err := <-alarmResult:
		t.Fatalf("SetAlarm returned before native completion: %v", err)
	default:
	}
	runner.emit(wire.Event{
		Kind: wire.EventActorIntentResult, OperationID: alarmCommand.OperationID,
	})
	if err := <-alarmResult; err != nil {
		t.Fatalf("SetAlarm: %v", err)
	}

	sleepResult := make(chan error, 1)
	go func() { sleepResult <- session.Sleep() }()
	sleepCommand := nextCommand(t, runner)
	if sleepCommand.Kind != wire.CommandSleepIntent || sleepCommand.AID != "intent-aid" ||
		sleepCommand.Generation != 9 || sleepCommand.OperationID == 0 {
		t.Fatalf("sleep command = %#v", sleepCommand)
	}
	runner.emit(wire.Event{
		Kind: wire.EventActorIntentResult, OperationID: sleepCommand.OperationID,
		Error: &wire.WireError{Code: "sleep_intent_failed", Message: "core rejected sleep"},
	})
	intentErr := <-sleepResult
	var structured HandlerError
	if !errors.As(intentErr, &structured) || structured.Code != "sleep_intent_failed" ||
		structured.Message != "core rejected sleep" {
		t.Fatalf("Sleep error = %#v, want native structured sleep_intent_failed", intentErr)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestLateActorIntentCompletionCannotResolveAReusedID(t *testing.T) {
	p := New(newFakeRunner())
	p.nextIntentID.Store(^uint64(0))
	abandoned := make(chan wire.Event, 1)
	abandonedID := p.addActorIntentWaiter(abandoned)
	if abandonedID != 1 {
		t.Fatalf("wrapped operation ID = %d, want 1", abandonedID)
	}
	p.abandonActorIntentWaiter(abandonedID)

	p.nextIntentID.Store(^uint64(0))
	live := make(chan wire.Event, 1)
	liveID := p.addActorIntentWaiter(live)
	if liveID != 2 {
		t.Fatalf("operation ID after tombstone = %d, want 2", liveID)
	}
	if err := p.handleInternalEvent(wire.Event{
		Kind: wire.EventActorIntentResult, OperationID: abandonedID,
	}); err != nil {
		t.Fatalf("handle late completion: %v", err)
	}
	select {
	case event := <-live:
		t.Fatalf("late completion resolved live waiter: %#v", event)
	default:
	}
	if err := p.handleInternalEvent(wire.Event{
		Kind: wire.EventActorIntentResult, OperationID: liveID,
	}); err != nil {
		t.Fatalf("handle live completion: %v", err)
	}
	select {
	case event := <-live:
		if event.OperationID != liveID {
			t.Fatalf("live completion = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("live waiter did not receive its completion")
	}
}

func TestRunnerShutdownRacingSleepIntentIsDeterministic(t *testing.T) {
	runner := newFakeRunner()
	sessions := make(chan *ActorSession, 1)
	handler := lifecycleHandler{start: func(
		_ context.Context,
		session *ActorSession,
		_ wire.Event,
	) (any, error) {
		sessions <- session
		return nil, nil
	}}
	p := NewWithHandlers(runner, map[string]ActorHandler{"sleep-race": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{
		Kind: wire.EventActorStart, AID: "sleep-race-aid", Generation: 6, Name: "sleep-race",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	session := <-sessions
	sleepResult := make(chan error, 1)
	go func() { sleepResult <- session.Sleep() }()
	sleepCommand := nextCommand(t, runner)
	if sleepCommand.Kind != wire.CommandSleepIntent || sleepCommand.Generation != 6 {
		t.Fatalf("sleep command = %#v", sleepCommand)
	}

	cancel()
	if err := <-sleepResult; !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("Sleep result = %v, want ErrShuttingDown", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !runner.stoppedPoll.Load() || runner.closeEarly.Load() {
		t.Fatalf("shutdown order: stopped=%v close_early=%v", runner.stoppedPoll.Load(), runner.closeEarly.Load())
	}
}

func TestSleepWaitsForMidflightActionAndHibernatesWebSocket(t *testing.T) {
	runner := newFakeRunner()
	actionStarted := make(chan struct{})
	actionRelease := make(chan struct{})
	closeReason := make(chan string, 1)
	var disconnects atomic.Int32
	handler := dispatchHandler{
		action: func(context.Context, *ActorSession, wire.Event, any) ([]byte, error) {
			close(actionStarted)
			<-actionRelease
			return []byte("completed-before-sleep"), nil
		},
		wsClose: func(context.Context, *ActorSession, wire.Event, any) error {
			disconnects.Add(1)
			return nil
		},
		wsCloseAll: func(_ context.Context, _ *ActorSession, _ any, reason string) {
			closeReason <- reason
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"sleeper": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{
		Kind: wire.EventActorStart, AID: "sleeper-aid", Generation: 4, Name: "sleeper",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(wire.Event{
		Kind: wire.EventWSOpen, AID: "sleeper-aid", WSID: "ws-hibernate",
		Path: "/chat", CanHibernate: true,
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandWSOpenResult {
		t.Fatalf("open command = %#v", command)
	}
	runner.emit(wire.Event{
		Kind: wire.EventActionCall, AID: "sleeper-aid", Generation: 4,
		CallID: 51, Action: "block", ActionTimeoutMS: 5_000,
	})
	select {
	case <-actionStarted:
	case <-time.After(time.Second):
		t.Fatal("action did not start")
	}
	runner.emit(wire.Event{
		Kind: wire.EventActorStop, AID: "sleeper-aid", Generation: 4, Reason: "sleep",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandWSClose ||
		command.WSID != "ws-hibernate" || !command.Hibernate {
		t.Fatalf("hibernate command = %#v", command)
	}
	select {
	case command := <-runner.submitted:
		t.Fatalf("actor work completed before action release: %#v", command)
	default:
	}
	close(actionRelease)
	if command := nextCommand(t, runner); command.Kind != wire.CommandActionResult ||
		string(command.Output) != "completed-before-sleep" {
		t.Fatalf("action command = %#v", command)
	}
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("stop command = %#v", command)
	}
	select {
	case reason := <-closeReason:
		if reason != "sleep" {
			t.Fatalf("close-all reason = %q, want sleep", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("connection generation was not detached")
	}
	if disconnects.Load() != 0 {
		t.Fatalf("OnDisconnect calls = %d, want 0 for hibernation", disconnects.Load())
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
	hooks := newRecordingHooks()
	healthyStarted := make(chan struct{}, 1)
	p := NewWithOptions(runner, map[string]ActorHandler{
		"panic": lifecycleHandler{start: func(context.Context, *ActorSession, wire.Event) (any, error) {
			panic("start failed")
		}},
		"healthy": lifecycleHandler{start: func(context.Context, *ActorSession, wire.Event) (any, error) {
			healthyStarted <- struct{}{}
			return nil, nil
		}},
	}, Options{Hooks: hooks})
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
	counters, gauges, observed := hooks.snapshot()
	if counters[metricActorStarts] != 2 || counters[metricActorStops] != 1 || counters[metricActorPanics] != 1 {
		t.Fatalf("actor metrics = %#v", counters)
	}
	if counters[metricEventsPolled] != 4 || counters[metricCommandsSubmitted] != 3 {
		t.Fatalf("pump metrics = %#v", counters)
	}
	if gauges[metricLiveActors] != 0 || gauges[metricLiveConnections] != 0 {
		t.Fatalf("final gauges = %#v", gauges)
	}
	if len(observed[metricPollLatency]) == 0 {
		t.Fatal("poll latency was not observed")
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

func TestActionsAreActorLocalOrderedAndPanicContained(t *testing.T) {
	runner := newFakeRunner()
	slowRelease := make(chan struct{})
	seenSlow := make(chan string, 2)
	handlers := map[string]ActorHandler{
		"slow": dispatchHandler{
			action: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) ([]byte, error) {
				seenSlow <- event.Action
				if event.Action == "first" {
					<-slowRelease
				}
				return []byte(event.Action), nil
			},
		},
		"fast": dispatchHandler{
			action: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) ([]byte, error) {
				return []byte(event.Action), nil
			},
		},
		"panic": dispatchHandler{
			action: func(context.Context, *ActorSession, wire.Event, any) ([]byte, error) {
				panic("action boom")
			},
		},
	}
	p := NewWithHandlers(runner, handlers)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(
		wire.Event{Kind: wire.EventActorStart, AID: "slow-aid", Generation: 1, Name: "slow"},
		wire.Event{Kind: wire.EventActorStart, AID: "fast-aid", Generation: 1, Name: "fast"},
		wire.Event{Kind: wire.EventActorStart, AID: "panic-aid", Generation: 1, Name: "panic"},
	)
	for range 3 {
		if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
			t.Fatalf("start command kind = %q", command.Kind)
		}
	}
	runner.emit(
		wire.Event{Kind: wire.EventActionCall, AID: "slow-aid", Generation: 1, CallID: 1, Action: "first", ActionTimeoutMS: 1_000},
		wire.Event{Kind: wire.EventActionCall, AID: "slow-aid", Generation: 1, CallID: 2, Action: "second", ActionTimeoutMS: 1_000},
		wire.Event{Kind: wire.EventActionCall, AID: "fast-aid", Generation: 1, CallID: 3, Action: "fast", ActionTimeoutMS: 1_000},
		wire.Event{Kind: wire.EventActionCall, AID: "panic-aid", Generation: 1, CallID: 4, Action: "panic", ActionTimeoutMS: 1_000},
	)

	first := nextCommand(t, runner)
	second := nextCommand(t, runner)
	commands := map[uint64]wire.Command{first.CallID: first, second.CallID: second}
	if command := commands[3]; command.Kind != wire.CommandActionResult || string(command.Output) != "fast" {
		t.Fatalf("fast action result = %#v", command)
	}
	if command := commands[4]; command.Error == nil || command.Error.Code != "handler_panic" {
		t.Fatalf("panic action result = %#v", command)
	}
	select {
	case action := <-seenSlow:
		if action != "first" {
			t.Fatalf("first slow action = %q", action)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow action did not start")
	}
	select {
	case action := <-seenSlow:
		t.Fatalf("second slow action ran before first completed: %q", action)
	default:
	}
	close(slowRelease)
	if command := nextCommand(t, runner); command.CallID != 1 {
		t.Fatalf("first slow result = %#v", command)
	}
	if action := <-seenSlow; action != "second" {
		t.Fatalf("second slow action = %q", action)
	}
	if command := nextCommand(t, runner); command.CallID != 2 {
		t.Fatalf("second slow result = %#v", command)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestActionDeadlineIsPropagatedAndActorRecovers(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	handler := dispatchHandler{
		action: func(ctx context.Context, _ *ActorSession, event wire.Event, _ any) ([]byte, error) {
			if event.Action == "slow" {
				if _, ok := ctx.Deadline(); !ok {
					return nil, errors.New("action context has no deadline")
				}
				<-ctx.Done()
				return nil, HandlerError{Code: "action_timed_out", Message: ctx.Err().Error()}
			}
			return []byte("healthy"), nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"deadline": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "deadline-aid", Generation: 1, Name: "deadline"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(wire.Event{Kind: wire.EventActionCall, AID: "deadline-aid", Generation: 1, CallID: 40, Action: "slow", ActionTimeoutMS: 40})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActionResult ||
		command.Error == nil || command.Error.Code != "action_timed_out" {
		t.Fatalf("timed-out action result = %#v", command)
	}
	runner.emit(wire.Event{Kind: wire.EventActionCall, AID: "deadline-aid", Generation: 1, CallID: 41, Action: "after", ActionTimeoutMS: 1_000})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActionResult ||
		command.CallID != 41 || string(command.Output) != "healthy" {
		t.Fatalf("post-timeout action result = %#v", command)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestWebSocketEventsAreActorOrderedAndCommandsDoNotUsePoller(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	seen := make(chan string, 8)
	handler := dispatchHandler{
		wsOpen: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) error {
			seen <- "open:" + event.WSID
			return nil
		},
		wsMessage: func(ctx context.Context, session *ActorSession, event wire.Event, _ any) error {
			seen <- "message:" + event.WSID + ":" + string(event.Data)
			if err := session.Broadcast(ctx, "updated", []byte{0x81, 0x01}, nil); err != nil {
				return err
			}
			return session.SendWebSocket(ctx, "ws-b", []byte("targeted"), false)
		},
		wsClose: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) error {
			seen <- "close:" + event.WSID
			return nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"socket": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "socket-aid", Generation: 1, Name: "socket"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(
		wire.Event{Kind: wire.EventWSOpen, AID: "socket-aid", WSID: "ws-a", Path: "/chat", CanHibernate: true},
		wire.Event{Kind: wire.EventWSOpen, AID: "socket-aid", WSID: "ws-b", Path: "/chat", CanHibernate: true},
	)
	for _, wsID := range []string{"ws-a", "ws-b"} {
		if got := <-seen; got != "open:"+wsID {
			t.Fatalf("open order = %q, want open:%s", got, wsID)
		}
		if command := nextCommand(t, runner); command.Kind != wire.CommandWSOpenResult ||
			command.WSID != wsID || !command.Accept {
			t.Fatalf("open result = %#v", command)
		}
	}

	runner.emit(wire.Event{
		Kind:         wire.EventWSMessage,
		WSID:         "ws-a",
		Data:         []byte("hello"),
		MessageIndex: 7,
	})
	if got := <-seen; got != "message:ws-a:hello" {
		t.Fatalf("message observation = %q", got)
	}
	commands := []wire.Command{nextCommand(t, runner), nextCommand(t, runner), nextCommand(t, runner)}
	if commands[0].Kind != wire.CommandBroadcast || commands[0].Event != "updated" {
		t.Fatalf("broadcast command = %#v", commands[0])
	}
	if commands[1].Kind != wire.CommandWSSend || commands[1].WSID != "ws-b" {
		t.Fatalf("targeted send command = %#v", commands[1])
	}
	if commands[2].Kind != wire.CommandWSMessageAck || commands[2].MessageIndex != 7 {
		t.Fatalf("message acknowledgement = %#v", commands[2])
	}
	if runner.maxPolls.Load() != 1 {
		t.Fatalf("maximum concurrent native polls = %d, want 1", runner.maxPolls.Load())
	}

	reason := "client done"
	code := uint16(1000)
	runner.emit(wire.Event{Kind: wire.EventWSClose, WSID: "ws-a", CloseCode: &code, Reason: reason})
	if got := <-seen; got != "close:ws-a" {
		t.Fatalf("close observation = %q", got)
	}
	runner.emit(wire.Event{Kind: wire.EventActorStop, AID: "socket-aid", Generation: 1, Reason: "destroy"})
	if got := <-seen; got != "close:ws-b" {
		t.Fatalf("actor-stop close observation = %q", got)
	}
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStopResult {
		t.Fatalf("stop command = %#v", command)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestNonHibernatingWebSocketMessageSkipsBoundaryAcknowledgement(t *testing.T) {
	runner := newFakeRunner()
	handled := make(chan struct{}, 1)
	handler := dispatchHandler{
		wsMessage: func(context.Context, *ActorSession, wire.Event, any) error {
			handled <- struct{}{}
			return nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"socket": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{
		Kind: wire.EventActorStart, AID: "default-aid", Generation: 1, Name: "socket",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(wire.Event{
		Kind: wire.EventWSOpen, AID: "default-aid", WSID: "default-ws", Path: "/chat",
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandWSOpenResult {
		t.Fatalf("open command = %#v", command)
	}
	runner.emit(wire.Event{
		Kind: wire.EventWSMessage, WSID: "default-ws", Data: []byte("message"), MessageIndex: 9,
	})
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("default WebSocket message was not handled")
	}
	select {
	case command := <-runner.submitted:
		t.Fatalf("default WebSocket submitted an unnecessary command: %#v", command)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestWebSocketHandlerPanicRequestsActorErrorStopAndPeerSurvives(t *testing.T) {
	runner := newFakeRunner()
	handlers := map[string]ActorHandler{
		"panic-socket": dispatchHandler{
			wsMessage: func(context.Context, *ActorSession, wire.Event, any) error {
				panic("socket boom")
			},
		},
		"peer": dispatchHandler{
			action: func(context.Context, *ActorSession, wire.Event, any) ([]byte, error) {
				return []byte("healthy"), nil
			},
		},
	}
	p := NewWithHandlers(runner, handlers)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(
		wire.Event{Kind: wire.EventActorStart, AID: "panic-aid", Generation: 1, Name: "panic-socket"},
		wire.Event{Kind: wire.EventActorStart, AID: "peer-aid", Generation: 1, Name: "peer"},
	)
	nextCommand(t, runner)
	nextCommand(t, runner)
	runner.emit(wire.Event{Kind: wire.EventWSOpen, AID: "panic-aid", WSID: "ws-panic", Path: "/", CanHibernate: true})
	nextCommand(t, runner)
	runner.emit(wire.Event{Kind: wire.EventWSMessage, WSID: "ws-panic", Data: []byte("boom"), MessageIndex: 1})
	first := nextCommand(t, runner)
	second := nextCommand(t, runner)
	kinds := map[wire.CommandKind]wire.Command{first.Kind: first, second.Kind: second}
	if ack := kinds[wire.CommandWSMessageAck]; ack.WSID != "ws-panic" {
		t.Fatalf("panic message acknowledgement = %#v", ack)
	}
	if stop := kinds[wire.CommandStopIntent]; stop.AID != "panic-aid" {
		t.Fatalf("panic stop intent = %#v", stop)
	}
	runner.emit(wire.Event{Kind: wire.EventActionCall, AID: "peer-aid", Generation: 1, CallID: 9, Action: "health", ActionTimeoutMS: 1_000})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActionResult || string(command.Output) != "healthy" {
		t.Fatalf("peer action after WebSocket panic = %#v", command)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestWebSocketIDCanBeReusedAfterCloseWithoutStaleCorrelation(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	seen := make(chan string, 8)
	handler := dispatchHandler{
		wsOpen: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) error {
			seen <- "open:" + event.WSID
			return nil
		},
		wsMessage: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) error {
			seen <- "message:" + string(event.Data)
			return nil
		},
		wsClose: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) error {
			seen <- "close:" + event.WSID
			return nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"socket": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "socket-aid", Generation: 1, Name: "socket"})
	nextCommand(t, runner)
	code := uint16(1000)
	for attempt := range 2 {
		runner.emit(wire.Event{Kind: wire.EventWSOpen, AID: "socket-aid", WSID: "reused", Path: "/", CanHibernate: true})
		if got := <-seen; got != "open:reused" {
			t.Fatalf("attempt %d open observation = %q", attempt, got)
		}
		if command := nextCommand(t, runner); command.Kind != wire.CommandWSOpenResult || !command.Accept {
			t.Fatalf("attempt %d open result = %#v", attempt, command)
		}
		if attempt == 1 {
			runner.emit(wire.Event{
				Kind: wire.EventWSMessage, WSID: "reused", Data: []byte("fresh"), MessageIndex: 2,
			})
			if got := <-seen; got != "message:fresh" {
				t.Fatalf("reused connection message observation = %q", got)
			}
			if command := nextCommand(t, runner); command.Kind != wire.CommandWSMessageAck || command.MessageIndex != 2 {
				t.Fatalf("reused connection acknowledgement = %#v", command)
			}
		}
		runner.emit(wire.Event{Kind: wire.EventWSClose, WSID: "reused", CloseCode: &code, Reason: "done"})
		if got := <-seen; got != "close:reused" {
			t.Fatalf("attempt %d close observation = %q", attempt, got)
		}
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestHTTPRequestChunksFeedBodyAndResponsesSubmitFromHandler(t *testing.T) {
	runner := newFakeRunner()
	requestObserved := make(chan string, 1)
	handler := dispatchHandler{
		fetch: func(_ context.Context, session *ActorSession, event wire.Event, _ any) error {
			request, err := session.HTTPRequest(event)
			if err != nil {
				return err
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return err
			}
			requestObserved <- request.Method + " " + request.Path + " " + string(body)
			if err := session.StartHTTPResponse(
				request.Context,
				event.RequestID,
				201,
				map[string]string{"content-type": "text/plain"},
				nil,
				true,
			); err != nil {
				return err
			}
			if err := session.WriteHTTPResponseChunk(request.Context, event.RequestID, []byte("one"), false); err != nil {
				return err
			}
			return session.WriteHTTPResponseChunk(request.Context, event.RequestID, []byte("two"), true)
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"fetch": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "fetch-aid", Generation: 1, Name: "fetch"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command kind = %q", command.Kind)
	}
	runner.emit(
		wire.Event{
			Kind: wire.EventHTTPRequest, AID: "fetch-aid", Generation: 1,
			RequestID: 8, Method: "POST", Path: "/upload?part=1", Body: []byte("first-"), Stream: true,
		},
		wire.Event{Kind: wire.EventHTTPRequestChunk, RequestID: 8, Body: []byte("second"), Finish: true},
	)
	select {
	case got := <-requestObserved:
		if got != "POST /upload?part=1 first-second" {
			t.Fatalf("request observation = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP handler did not receive request chunks")
	}
	wantKinds := []wire.CommandKind{
		wire.CommandHTTPResponseStart,
		wire.CommandHTTPResponseChunk,
		wire.CommandHTTPResponseChunk,
	}
	for _, want := range wantKinds {
		if command := nextCommand(t, runner); command.Kind != want {
			t.Fatalf("HTTP response command kind = %q, want %q", command.Kind, want)
		}
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestHTTPResponseRetriesNativeBackpressure(t *testing.T) {
	base := newFakeRunner()
	runner := &retryRunner{fakeRunner: base}
	hooks := newRecordingHooks()
	handler := dispatchHandler{
		fetch: func(ctx context.Context, session *ActorSession, event wire.Event, _ any) error {
			if err := session.StartHTTPResponse(ctx, event.RequestID, 200, nil, nil, true); err != nil {
				return err
			}
			return session.WriteHTTPResponseChunk(ctx, event.RequestID, []byte("done"), true)
		},
	}
	p := NewWithOptions(runner, map[string]ActorHandler{"fetch": handler}, Options{Hooks: hooks})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	base.emit(wire.Event{Kind: wire.EventActorStart, AID: "fetch-retry", Generation: 1, Name: "fetch"})
	if command := nextCommand(t, base); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.remaining.Store(2)
	base.emit(wire.Event{
		Kind: wire.EventHTTPRequest, AID: "fetch-retry", Generation: 1,
		RequestID: 21, Method: "GET", Path: "/retry",
	})
	if command := nextCommand(t, base); command.Kind != wire.CommandHTTPResponseStart {
		t.Fatalf("HTTP response start = %#v", command)
	}
	if command := nextCommand(t, base); command.Kind != wire.CommandHTTPResponseChunk || !command.Finish {
		t.Fatalf("HTTP response finish = %#v", command)
	}
	if attempts := runner.attempts.Load(); attempts < 5 {
		t.Fatalf("native submit attempts = %d, want the start plus two retries and two accepted response commands", attempts)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
	counters, _, _ := hooks.snapshot()
	if counters[metricBackpressureHits] != 2 {
		t.Fatalf("backpressure metric = %d, want 2", counters[metricBackpressureHits])
	}
}

func TestHTTPResponseBackpressureRetryIsBounded(t *testing.T) {
	defer goleak.VerifyNone(t)
	base := newFakeRunner()
	runner := &retryRunner{fakeRunner: base}
	runner.remaining.Store(1<<31 - 1)
	p := New(runner)
	p.httpSubmitLimit = 40 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	session := &ActorSession{pump: p, done: make(chan struct{})}
	started := time.Now()
	err := session.StartHTTPResponse(context.Background(), 31, 200, nil, nil, true)
	var structured HandlerError
	if !errors.As(err, &structured) || structured.Code != "http_response_backpressure_timeout" {
		t.Fatalf("bounded backpressure error = %v, want http_response_backpressure_timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("bounded backpressure returned after %s", elapsed)
	}
	if attempts := runner.attempts.Load(); attempts < 2 {
		t.Fatalf("native submit attempts = %d, want at least one retry", attempts)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestHTTPRequestAbortCancelsHandlerContext(t *testing.T) {
	runner := newFakeRunner()
	handlerStarted := make(chan struct{})
	type abortObservation struct {
		contextCause error
		bodyError    error
	}
	observed := make(chan abortObservation, 1)
	handler := dispatchHandler{
		fetch: func(_ context.Context, session *ActorSession, event wire.Event, _ any) error {
			request, err := session.HTTPRequest(event)
			if err != nil {
				return err
			}
			close(handlerStarted)
			_, bodyError := io.ReadAll(request.Body)
			observed <- abortObservation{
				contextCause: context.Cause(request.Context),
				bodyError:    bodyError,
			}
			return nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"fetch": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "fetch-abort", Generation: 1, Name: "fetch"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(wire.Event{
		Kind: wire.EventHTTPRequest, AID: "fetch-abort", Generation: 1,
		RequestID: 22, Method: "POST", Path: "/abort", Body: []byte("partial"), Stream: true,
	})
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}
	runner.emit(wire.Event{Kind: wire.EventHTTPRequestAbort, RequestID: 22})
	select {
	case observation := <-observed:
		if observation.contextCause == nil || !strings.Contains(observation.contextCause.Error(), "aborted by engine") {
			t.Fatalf("request cancellation cause = %v", observation.contextCause)
		}
		if observation.bodyError == nil || !strings.Contains(observation.bodyError.Error(), "aborted by engine") {
			t.Fatalf("request body error = %v", observation.bodyError)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP abort did not cancel the handler context")
	}
	deadline := time.Now().Add(time.Second)
	for {
		p.httpMu.Lock()
		pending := len(p.httpPending)
		p.httpMu.Unlock()
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("HTTP correlations remaining after abort = %d", pending)
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestShutdownCancelsInFlightHTTPRequestAndCleansCorrelation(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	handlerStarted := make(chan struct{})
	observed := make(chan error, 1)
	handler := dispatchHandler{
		fetch: func(_ context.Context, session *ActorSession, event wire.Event, _ any) error {
			request, err := session.HTTPRequest(event)
			if err != nil {
				return err
			}
			close(handlerStarted)
			<-request.Context.Done()
			observed <- context.Cause(request.Context)
			return nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"fetch": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "fetch-shutdown", Generation: 1, Name: "fetch"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(wire.Event{
		Kind: wire.EventHTTPRequest, AID: "fetch-shutdown", Generation: 1,
		RequestID: 32, Method: "GET", Path: "/shutdown", Stream: true,
	})
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}
	cancel()
	select {
	case cause := <-observed:
		if !errors.Is(cause, context.Canceled) {
			t.Fatalf("request cancellation cause = %v, want context cancellation", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("runner shutdown did not cancel the HTTP request")
	}
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.httpMu.Lock()
	pending := len(p.httpPending)
	p.httpMu.Unlock()
	if pending != 0 {
		t.Fatalf("HTTP correlations remaining after shutdown = %d", pending)
	}
}

func TestFetchPanicReturnsStructuredErrorWithoutStoppingPeerActor(t *testing.T) {
	runner := newFakeRunner()
	p := NewWithHandlers(runner, map[string]ActorHandler{
		"panic-fetch": dispatchHandler{
			fetch: func(context.Context, *ActorSession, wire.Event, any) error {
				panic("fetch boom")
			},
		},
		"healthy": dispatchHandler{
			action: func(_ context.Context, _ *ActorSession, event wire.Event, _ any) ([]byte, error) {
				return []byte(event.Action), nil
			},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)
	runner.emit(
		wire.Event{Kind: wire.EventActorStart, AID: "panic-fetch-aid", Generation: 1, Name: "panic-fetch"},
		wire.Event{Kind: wire.EventActorStart, AID: "healthy-aid", Generation: 1, Name: "healthy"},
	)
	for range 2 {
		if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult || !command.OK {
			t.Fatalf("start command = %#v", command)
		}
	}

	runner.emit(wire.Event{
		Kind: wire.EventHTTPRequest, AID: "panic-fetch-aid", Generation: 1,
		RequestID: 19, Method: "GET", Path: "/panic",
	})
	failed := nextCommand(t, runner)
	if failed.Kind != wire.CommandHTTPResponseStart || failed.RequestID != 19 ||
		failed.Error == nil || failed.Error.Code != "handler_panic" {
		t.Fatalf("panicking fetch result = %#v", failed)
	}

	runner.emit(wire.Event{
		Kind: wire.EventActionCall, AID: "healthy-aid", Generation: 1,
		CallID: 20, Action: "still-healthy", ActionTimeoutMS: 1_000,
	})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActionResult ||
		command.CallID != 20 || string(command.Output) != "still-healthy" {
		t.Fatalf("peer action result = %#v", command)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run after fetch panic: %v", err)
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

// Regression for the poller deadlock class: the poll goroutine must never
// block delivering events to a busy actor. Here the actor's first message
// handler parks inside KVGet — a wait only the poll goroutine can complete —
// while the engine delivers far more events for the same actor than the old
// bounded poller→worker queue (64) would hold. With a bounded hand-off this
// deadlocked: the poller blocked on delivery, so the KVResult that would
// unpark the handler could never be polled.
func TestPollerNeverBlocksOnBusyActorMailbox(t *testing.T) {
	defer goleak.VerifyNone(t)
	runner := newFakeRunner()
	const messages = 200
	processed := make(chan string, messages)
	kvDone := make(chan error, 1)
	handler := dispatchHandler{
		wsMessage: func(_ context.Context, session *ActorSession, event wire.Event, _ any) error {
			if string(event.Data) == "message-000" {
				_, _, err := session.KVGet(context.Background(), []byte("parked"))
				kvDone <- err
			}
			processed <- string(event.Data)
			return nil
		},
	}
	p := NewWithHandlers(runner, map[string]ActorHandler{"socket": handler})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	waitPumpStarted(t, p)

	runner.emit(wire.Event{Kind: wire.EventActorStart, AID: "busy-aid", Generation: 1, Name: "socket"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandActorStartResult {
		t.Fatalf("start command = %#v", command)
	}
	runner.emit(wire.Event{Kind: wire.EventWSOpen, AID: "busy-aid", WSID: "ws-busy", Path: "/chat"})
	if command := nextCommand(t, runner); command.Kind != wire.CommandWSOpenResult || !command.Accept {
		t.Fatalf("open result = %#v", command)
	}
	// One batch delivers every message: the first parks the handler in KVGet
	// and the rest accumulate behind it in the actor's mailbox.
	events := make([]wire.Event, 0, messages)
	for index := range messages {
		events = append(events, wire.Event{
			Kind: wire.EventWSMessage, WSID: "ws-busy",
			Data: []byte(fmt.Sprintf("message-%03d", index)),
		})
	}
	runner.emit(events...)
	// The KVGet command must reach the runner while the actor's mailbox holds
	// the backlog — proof the poller is still live — and its completion must
	// unpark the handler.
	command := nextCommand(t, runner)
	if command.Kind != wire.CommandKVGet || command.KVID == 0 {
		t.Fatalf("kv command = %#v", command)
	}
	runner.emit(wire.Event{Kind: wire.EventKVResult, KVID: command.KVID, Value: []byte("value")})
	if err := <-kvDone; err != nil {
		t.Fatalf("KVGet: %v", err)
	}
	for index := range messages {
		want := fmt.Sprintf("message-%03d", index)
		select {
		case got := <-processed:
			if got != want {
				t.Fatalf("message order: got %q want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("message %q was never processed", want)
		}
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
