package conformance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

type queueJob struct {
	ID        string `json:"id"`
	Text      string `json:"text"`
	RetryOnce bool   `json:"retryOnce"`
}

type queueState struct {
	Attempts  map[string]int `json:"attempts"`
	Processed []string       `json:"processed"`
}

type queueCompletion struct {
	Attempt int    `json:"attempt"`
	Text    string `json:"text"`
}

type runFailureState struct{}

func (*runFailureState) MarshalBinary() ([]byte, error) { return []byte{1}, nil }
func (*runFailureState) UnmarshalBinary([]byte) error   { return nil }

func TestDurableQueuesAndManagedWorkAcrossGenerations(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)
	starts := make(chan uint64, 16)
	waitUntilDone := make(chan struct{}, 1)
	shutdownTaskCanceled := make(chan struct{}, 1)
	runFailureStarts := make(chan uint64, 4)
	runFailureStops := make(chan uint64, 4)
	var failedRun atomic.Bool

	newRegistry := func() *rivet.Registry {
		registry := rivet.NewRegistry()
		if registerErr := rivet.Register(registry, "queue-worker", rivet.Actor[queueState]{
			OnStart: func(actor *rivet.Context[queueState]) error {
				if actor.State().Attempts == nil {
					actor.State().Attempts = make(map[string]int)
				}
				starts <- actor.Generation()
				return nil
			},
			Run: func(ctx context.Context, actor *rivet.RunContext[queueState]) error {
				for {
					message, nextErr := actor.Queue().Next(ctx, rivet.QueueNextOptions{
						Names: []string{"jobs"}, Completable: true,
					})
					if nextErr != nil {
						if errors.Is(nextErr, context.Canceled) || errors.Is(nextErr, rivet.ErrActorAborted) {
							return nil
						}
						return nextErr
					}
					var job queueJob
					if decodeErr := message.DecodeBody(&job); decodeErr != nil {
						return decodeErr
					}
					actor.State().Attempts[job.ID]++
					attempt := actor.State().Attempts[job.ID]
					if saveErr := actor.Save(ctx); saveErr != nil {
						return saveErr
					}
					if job.RetryOnce && attempt == 1 {
						if retryErr := message.Retry(ctx); retryErr != nil {
							return retryErr
						}
						continue
					}
					actor.State().Processed = append(actor.State().Processed, job.Text)
					if saveErr := actor.Save(ctx); saveErr != nil {
						return saveErr
					}
					if completeErr := message.Complete(ctx, queueCompletion{Attempt: attempt, Text: job.Text}); completeErr != nil {
						return completeErr
					}
				}
			},
			Actions: rivet.Actions[queueState]{
				"state": rivet.Action(func(actor *rivet.Context[queueState], _ struct{}) (queueState, error) {
					return *actor.State(), nil
				}),
				"sleep": rivet.Action(func(actor *rivet.Context[queueState], _ struct{}) (bool, error) {
					return true, actor.Sleep()
				}),
				"managed": rivet.ActionWithContext(func(
					ctx context.Context,
					actor *rivet.Context[queueState],
					_ struct{},
				) (bool, error) {
					if keepErr := actor.KeepAwake(ctx, func(workCtx context.Context) error {
						return workCtx.Err()
					}); keepErr != nil {
						return false, keepErr
					}
					if waitErr := actor.WaitUntil(ctx, func(workCtx context.Context) error {
						select {
						case waitUntilDone <- struct{}{}:
						case <-workCtx.Done():
							return workCtx.Err()
						}
						return nil
					}); waitErr != nil {
						return false, waitErr
					}
					return true, nil
				}),
				"roundTrip": rivet.ActionWithContext(func(
					ctx context.Context,
					actor *rivet.Context[queueState],
					job queueJob,
				) (queueCompletion, error) {
					response, queueErr := actor.Queue().SendAndWait(ctx, "jobs", job, rivet.QueueWaitOptions{
						Timeout: 5 * time.Second,
					})
					if queueErr != nil {
						return queueCompletion{}, queueErr
					}
					var completion queueCompletion
					if decodeErr := response.Decode(&completion); decodeErr != nil {
						return queueCompletion{}, decodeErr
					}
					return completion, nil
				}),
				"waitForShutdown": rivet.ActionWithContext(func(
					ctx context.Context,
					actor *rivet.Context[queueState],
					_ struct{},
				) (bool, error) {
					err := actor.WaitUntil(ctx, func(workCtx context.Context) error {
						<-workCtx.Done()
						shutdownTaskCanceled <- struct{}{}
						return workCtx.Err()
					})
					return err == nil, err
				}),
			},
		}); registerErr != nil {
			t.Fatalf("register queue worker: %v", registerErr)
		}
		if registerErr := rivet.Register(registry, "queue-poller", rivet.Actor[struct{}]{
			Actions: rivet.Actions[struct{}]{
				"cancelPoll": rivet.ActionWithContext(func(
					ctx context.Context,
					actor *rivet.Context[struct{}],
					_ struct{},
				) (bool, error) {
					pollCtx, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
					defer cancel()
					_, pollErr := actor.Queue().Next(pollCtx, rivet.QueueNextOptions{
						Names: []string{"missing"}, Timeout: 5 * time.Second,
					})
					return errors.Is(pollErr, context.DeadlineExceeded), nil
				}),
			},
		}); registerErr != nil {
			t.Fatalf("register queue poller: %v", registerErr)
		}
		if registerErr := rivet.Register(registry, "queue-buffer", rivet.Actor[struct{}]{}); registerErr != nil {
			t.Fatalf("register queue buffer: %v", registerErr)
		}
		if registerErr := rivet.Register(registry, "queue-run-failure", rivet.Actor[runFailureState]{
			OnStart: func(actor *rivet.Context[runFailureState]) error {
				runFailureStarts <- actor.Generation()
				return nil
			},
			OnStop: func(actor *rivet.Context[runFailureState]) error {
				runFailureStops <- actor.Generation()
				return nil
			},
			Run: func(ctx context.Context, actor *rivet.RunContext[runFailureState]) error {
				if failedRun.CompareAndSwap(false, true) {
					return errors.New("intentional Run failure")
				}
				_, runErr := actor.Queue().Next(ctx, rivet.QueueNextOptions{Names: []string{"never"}})
				return runErr
			},
			Actions: rivet.Actions[runFailureState]{
				"generation": rivet.Action(func(actor *rivet.Context[runFailureState], _ struct{}) (uint64, error) {
					return actor.Generation(), nil
				}),
				"sleep": rivet.Action(func(actor *rivet.Context[runFailureState], _ struct{}) (bool, error) {
					return true, actor.Sleep()
				}),
			},
		}); registerErr != nil {
			t.Fatalf("register Run failure actor: %v", registerErr)
		}
		return registry
	}

	runnerName := fmt.Sprintf("rivet-go-queues-%d", time.Now().UnixNano())
	firstRunner := startRegistry(t, engine, runnerName, newRegistry())
	client, err := rivet.NewClient(rivet.ClientConfig{
		Endpoint: engine.endpoint, Namespace: "default", RunnerName: runnerName, Token: "dev",
	})
	if err != nil {
		t.Fatal(err)
	}
	failureRecord := createActor(t, engine.endpoint, "queue-run-failure", runnerName, "destroy", nil, nil)
	firstFailureGeneration := <-runFailureStarts
	eventually(t, 5*time.Second, func() (bool, error) { return failedRun.Load(), nil })
	activeGeneration := decodeActionOutput[uint64](t, gatewayAction(
		t, engine.endpoint, failureRecord.ActorID, "generation", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	if activeGeneration != firstFailureGeneration {
		t.Fatalf("generation after Run failure = %d, want %d", activeGeneration, firstFailureGeneration)
	}
	decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, failureRecord.ActorID, "sleep", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	select {
	case stoppedGeneration := <-runFailureStops:
		if stoppedGeneration != firstFailureGeneration {
			t.Fatalf("sleep cleanup generation = %d, want %d", stoppedGeneration, firstFailureGeneration)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("sleep after Run failure did not invoke OnStop")
	}
	waitForActor(t, engine.endpoint, failureRecord.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	failureActor, err := client.Get(context.Background(), failureRecord.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	if err := failureActor.Queue().Send(context.Background(), "never", "wake"); err != nil {
		t.Fatalf("wake after Run failure: %v", err)
	}
	select {
	case wakeGeneration := <-runFailureStarts:
		if wakeGeneration <= firstFailureGeneration {
			t.Fatalf("wake generation = %d, first = %d", wakeGeneration, firstFailureGeneration)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run was not relaunched in the next actor generation")
	}
	deleteActor(t, engine.endpoint, failureRecord.ActorID)

	workerRecord := createActor(t, engine.endpoint, "queue-worker", runnerName, "restart", nil, nil)
	initialGeneration := <-starts
	worker, err := client.Get(context.Background(), workerRecord.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := worker.Queue().SendAndWait(context.Background(), "jobs", queueJob{
		ID: "retry", Text: "first", RetryOnce: true,
	}, 15*time.Second)
	if err != nil {
		t.Fatalf("send retry job: %v", err)
	}
	if completion.Status != rivet.ActorQueueCompleted {
		t.Fatalf("retry job status = %q", completion.Status)
	}
	var reply queueCompletion
	if err := completion.DecodeResponse(&reply); err != nil {
		t.Fatalf("decode queue response: %v", err)
	}
	if reply.Attempt != 2 || reply.Text != "first" {
		t.Fatalf("retry completion = %#v", reply)
	}
	roundTrip := decodeActionOutput[queueCompletion](t, gatewayAction(
		t, engine.endpoint, worker.ID(), "roundTrip", []any{queueJob{ID: "self", Text: "from action"}},
		10*time.Second,
	), http.StatusOK)
	if roundTrip.Attempt != 1 || roundTrip.Text != "from action" {
		t.Fatalf("actor queue round trip = %#v", roundTrip)
	}

	if err := worker.Queue().Send(context.Background(), "jobs", queueJob{ID: "plain", Text: "second"}); err != nil {
		t.Fatalf("send plain job: %v", err)
	}
	eventually(t, 10*time.Second, func() (bool, error) {
		state := decodeActionOutput[queueState](t, gatewayAction(
			t, engine.endpoint, worker.ID(), "state", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		return len(state.Processed) == 3 && state.Attempts["plain"] == 1, nil
	})
	timedOut, err := worker.Queue().SendAndWait(context.Background(), "unhandled", queueJob{ID: "timeout"}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("timed queue wait: %v", err)
	}
	if timedOut.Status != rivet.ActorQueueTimedOut {
		t.Fatalf("timed queue status = %q", timedOut.Status)
	}

	managed := decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, worker.ID(), "managed", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	if !managed {
		t.Fatal("managed work action returned false")
	}
	select {
	case <-waitUntilDone:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitUntil work did not finish")
	}

	poller := createActor(t, engine.endpoint, "queue-poller", runnerName, "destroy", nil, nil)
	cancelled := decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, poller.ActorID, "cancelPoll", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	if !cancelled {
		t.Fatal("cancelled queue Next did not preserve context deadline error")
	}
	deleteActor(t, engine.endpoint, poller.ActorID)

	bufferRecord := createActor(t, engine.endpoint, "queue-buffer", runnerName, "destroy", nil, nil)
	buffer, err := client.Get(context.Background(), bufferRecord.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	const (
		queueCapacity = 1_000
		senders       = 20
	)
	fillErrors := make(chan error, queueCapacity)
	var fill sync.WaitGroup
	for sender := 0; sender < senders; sender++ {
		fill.Add(1)
		go func(sender int) {
			defer fill.Done()
			for index := sender; index < queueCapacity; index += senders {
				if sendErr := buffer.Queue().Send(context.Background(), "buffered", index); sendErr != nil {
					fillErrors <- sendErr
				}
			}
		}(sender)
	}
	fill.Wait()
	close(fillErrors)
	for fillErr := range fillErrors {
		t.Fatalf("fill durable queue: %v", fillErr)
	}
	if err := buffer.Queue().Send(context.Background(), "buffered", "overflow"); !errors.Is(err, rivet.ErrQueueFull) {
		t.Fatalf("queue overflow error = %v, want ErrQueueFull", err)
	}
	deleteActor(t, engine.endpoint, buffer.ID())

	decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, worker.ID(), "sleep", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	waitForActor(t, engine.endpoint, worker.ID(), false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	if err := worker.Queue().Send(context.Background(), "jobs", queueJob{ID: "wake", Text: "third"}); err != nil {
		t.Fatalf("send wake job: %v", err)
	}
	var wakeGeneration uint64
	select {
	case wakeGeneration = <-starts:
	case <-time.After(30 * time.Second):
		t.Fatal("queue send did not wake actor")
	}
	if wakeGeneration <= initialGeneration {
		t.Fatalf("wake generation = %d, initial = %d", wakeGeneration, initialGeneration)
	}
	eventually(t, 10*time.Second, func() (bool, error) {
		state := decodeActionOutput[queueState](t, gatewayAction(
			t, engine.endpoint, worker.ID(), "state", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		return len(state.Processed) == 4 && state.Attempts["wake"] == 1, nil
	})

	registeredShutdownTask := decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, worker.ID(), "waitForShutdown", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	if !registeredShutdownTask {
		t.Fatal("shutdown WaitUntil task was not registered")
	}
	shutdownStarted := time.Now()
	firstRunner.stop(t)
	if elapsed := time.Since(shutdownStarted); elapsed > 10*time.Second {
		t.Fatalf("managed-work runner shutdown took %s", elapsed)
	}
	select {
	case <-shutdownTaskCanceled:
	default:
		t.Fatal("runner shutdown did not cancel lingering WaitUntil work")
	}
	startRegistry(t, engine, runnerName, newRegistry())
	if err := worker.Queue().Send(context.Background(), "jobs", queueJob{ID: "restart", Text: "fourth"}); err != nil {
		t.Fatalf("send after runner restart: %v", err)
	}
	select {
	case restartGeneration := <-starts:
		if restartGeneration <= wakeGeneration {
			t.Fatalf("restart generation = %d, wake = %d", restartGeneration, wakeGeneration)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("queue worker did not restart on replacement runner")
	}
	eventually(t, 15*time.Second, func() (bool, error) {
		state := decodeActionOutput[queueState](t, gatewayAction(
			t, engine.endpoint, worker.ID(), "state", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		return len(state.Processed) == 5 && state.Attempts["restart"] == 1, nil
	})
	deleteActor(t, engine.endpoint, worker.ID())
}
