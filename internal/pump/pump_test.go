package pump

import (
	"context"
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
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		events: make(chan []byte, 128),
		closed: make(chan struct{}),
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
