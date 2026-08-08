package conformance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/internal/devengine"
	"github.com/ewhauser/rivet-go/internal/ffi"
	"github.com/ewhauser/rivet-go/internal/wire"
	"github.com/ewhauser/rivet-go/rivet"
	"github.com/fxamacker/cbor/v2"
)

const (
	engineTag    = devengine.Tag
	startupBound = 13 * time.Second
	// rivet-envoy-client v2.3.10 declares a 20-second ping-health threshold;
	// the adapter samples that status every 250 milliseconds.
	disconnectLivenessWindow = 22 * time.Second
	rehydrateWindow          = 45 * time.Second
)

type runningEngine struct {
	process  *devengine.Engine
	endpoint string
	storage  string
}

type envoyListResponse struct {
	Envoys []envoyRecord `json:"envoys"`
}

type envoyRecord struct {
	EnvoyKey   string `json:"envoy_key"`
	PoolName   string `json:"pool_name"`
	StopTS     *int64 `json:"stop_ts"`
	LastPingTS int64  `json:"last_ping_ts"`
	Version    int    `json:"version"`
}

type actorListResponse struct {
	Actors []actorRecord `json:"actors"`
}

type actorCreateResponse struct {
	Actor actorRecord `json:"actor"`
}

type actorRecord struct {
	ActorID       string          `json:"actor_id"`
	Name          string          `json:"name"`
	Key           *string         `json:"key"`
	CreateTS      int64           `json:"create_ts"`
	ConnectableTS *int64          `json:"connectable_ts"`
	SleepTS       *int64          `json:"sleep_ts"`
	DestroyTS     *int64          `json:"destroy_ts"`
	Error         json.RawMessage `json:"error"`
}

type gatewayResponse struct {
	response *http.Response
	body     []byte
	err      error
}

type persistentCounterState struct {
	Count int
}

func (s *persistentCounterState) MarshalBinary() ([]byte, error) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(s.Count))
	return data, nil
}

func (s *persistentCounterState) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("counter state length = %d, want 8", len(data))
	}
	s.Count = int(binary.BigEndian.Uint64(data))
	return nil
}

func TestRunnerRegistersWithEngine(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	runnerName := fmt.Sprintf("rivet-go-conformance-%d", time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	registry := rivet.NewRegistry()
	go func() {
		serveResult <- registry.Serve(ctx, rivet.Config{
			Endpoint:   engine.endpoint,
			Namespace:  "default",
			RunnerName: runnerName,
			Version:    1,
			TotalSlots: 4,
			LogLevel:   "info",
		})
	}()

	var registered envoyRecord
	eventually(t, 20*time.Second, func() (bool, error) {
		select {
		case err := <-serveResult:
			if err == nil {
				return false, errors.New("runner Serve exited before registration without an error")
			}
			return false, fmt.Errorf("runner Serve exited before registration: %w", err)
		default:
		}
		envoys, err := listEnvoys(engine.endpoint, runnerName)
		if err != nil {
			return false, err
		}
		for _, envoy := range envoys {
			if envoy.PoolName == runnerName && envoy.StopTS == nil {
				registered = envoy
				return true, nil
			}
		}
		return false, nil
	})
	if registered.EnvoyKey == "" {
		t.Fatal("engine management API returned a registration with an empty envoy_key")
	}
	if registered.Version != 1 {
		t.Fatalf("engine-visible runner version = %d, want 1", registered.Version)
	}

	cancel()
	select {
	case err := <-serveResult:
		if err != nil {
			t.Fatalf("runner Serve shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runner did not finish graceful shutdown")
	}

	// /envoys is an active-only list at this pin, so graceful shutdown is
	// observable as removal from the management API.
	eventually(t, 10*time.Second, func() (bool, error) {
		envoys, err := listEnvoys(engine.endpoint, runnerName)
		if err != nil {
			return false, err
		}
		return len(envoys) == 0, nil
	})
}

func TestRunnerNewFailuresAreStructuredAndBounded(t *testing.T) {
	tests := []struct {
		name     string
		endpoint func(*testing.T) string
	}{
		{name: "dead endpoint", endpoint: silentEndpoint},
		{name: "wrong port", endpoint: closedEndpoint},
		{name: "non-engine HTTP server", endpoint: nonEngineEndpoint},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			started := time.Now()
			payload := expectRunnerNewError(t, test.endpoint(t))
			if payload.Code == "" {
				t.Fatal("runner_new returned an error without a structured code")
			}
			if elapsed := time.Since(started); elapsed > startupBound {
				t.Fatalf("runner_new took %s, want at most %s", elapsed, startupBound)
			}
		})
	}
}

func TestNativeBoundaryConcurrencyAndLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	runner := startNativeRunner(t, engine.endpoint, "rivet-go-boundary")
	connectedBatch, connected := waitForNativeEvent(t, runner, wire.EventRunnerConnected, 5*time.Second)
	if connected.RunnerID == "" {
		t.Fatal("RunnerConnected has an empty runner_id")
	}

	firstPoll := make(chan struct {
		batch wire.EventBatch
		err   error
	}, 1)
	go func() {
		data, pollErr := runner.Poll(2 * time.Second)
		if pollErr != nil {
			firstPoll <- struct {
				batch wire.EventBatch
				err   error
			}{err: pollErr}
			return
		}
		batch, decodeErr := wire.DecodeEventBatch(data)
		firstPoll <- struct {
			batch wire.EventBatch
			err   error
		}{batch: batch, err: decodeErr}
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := runner.Poll(10 * time.Millisecond); nativeErrorCode(err) != "poll_in_progress" {
		t.Fatalf("second concurrent Poll error = %v, want poll_in_progress", err)
	}
	first := <-firstPoll
	if first.err != nil {
		t.Fatalf("first concurrent Poll: %v", first.err)
	}
	if first.batch.Seq <= connectedBatch.Seq {
		t.Fatalf("poll sequence = %d after %d, want strictly increasing", first.batch.Seq, connectedBatch.Seq)
	}

	emptyBatch, err := wire.EncodeCommandBatch(wire.CommandBatch{Commands: []wire.Command{}})
	if err != nil {
		t.Fatalf("encode empty command batch: %v", err)
	}
	const (
		submitters = 16
		submits    = 32
	)
	var submitWG sync.WaitGroup
	submitErrors := make(chan error, submitters)
	submitWG.Add(submitters)
	for range submitters {
		go func() {
			defer submitWG.Done()
			for range submits {
				if err := runner.Submit(emptyBatch); err != nil {
					submitErrors <- err
					return
				}
			}
		}()
	}
	submitWG.Wait()
	close(submitErrors)
	for err := range submitErrors {
		t.Fatalf("concurrent native Submit: %v", err)
	}

	if err := runner.Shutdown(3 * time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	stoppedBatch, stopped := waitForNativeEvent(t, runner, wire.EventRunnerStopped, 5*time.Second)
	if stoppedBatch.Seq <= first.batch.Seq {
		t.Fatalf("shutdown sequence = %d after %d, want strictly increasing", stoppedBatch.Seq, first.batch.Seq)
	}
	if stopped.DrainReport == nil || !stopped.DrainReport.Graceful {
		t.Fatalf("RunnerStopped drain report = %#v, want graceful", stopped.DrainReport)
	}
	runner.Close()
	runner.Close() // The owning Go handle must make a duplicate free harmless.

	forced := startNativeRunner(t, engine.endpoint, "rivet-go-forced-free")
	waitForNativeEvent(t, forced, wire.EventRunnerConnected, 5*time.Second)
	started := time.Now()
	forced.Close() // Free without a preceding shutdown or poll drain.
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("free without shutdown took %s", elapsed)
	}
	forced.Close()
}

func TestRunnerReportsEngineDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)
	runner := startNativeRunner(t, engine.endpoint, "rivet-go-disconnect")
	defer runner.Close()
	waitForNativeEvent(t, runner, wire.EventRunnerConnected, 5*time.Second)

	started := time.Now()
	engine.kill(t)
	_, disconnected := waitForNativeEvent(t, runner, wire.EventRunnerDisconnected, disconnectLivenessWindow)
	if disconnected.Reason == "" {
		t.Fatal("RunnerDisconnected has an empty reason")
	}
	if elapsed := time.Since(started); elapsed > disconnectLivenessWindow {
		t.Fatalf("RunnerDisconnected arrived after %s, liveness window is %s", elapsed, disconnectLivenessWindow)
	}
	if err := runner.Shutdown(2 * time.Second); err != nil {
		t.Fatalf("Shutdown after disconnect: %v", err)
	}
	waitForNativeEvent(t, runner, wire.EventRunnerStopped, 5*time.Second)
}

func TestNativeKVListCrossesPollBatchBoundaryAndReportsErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)
	runnerName := fmt.Sprintf("rivet-go-kv-%d", time.Now().UnixNano())
	runner := startNativeRunnerWithActors(t, engine.endpoint, runnerName, []string{"kv-list"})
	waitForNativeEvent(t, runner, wire.EventRunnerConnected, 5*time.Second)
	actor := createActor(t, engine.endpoint, "kv-list", runnerName, "destroy", nil, nil)
	_, started := waitForNativeEvent(t, runner, wire.EventActorStart, 10*time.Second)
	if started.AID != actor.ActorID {
		t.Fatalf("ActorStart actor ID = %s, want %s", started.AID, actor.ActorID)
	}
	submitNativeCommands(t, runner, wire.Command{
		Kind:       wire.CommandActorStartResult,
		AID:        started.AID,
		Generation: started.Generation,
		OK:         true,
	})
	listedActor := waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	if listedActor.Name != "kv-list" {
		t.Fatalf("engine-listed actor name = %q, want kv-list", listedActor.Name)
	}

	const entryCount = 65
	puts := make([]wire.Command, 0, entryCount)
	for i := uint64(1); i <= entryCount; i++ {
		puts = append(puts, wire.Command{
			Kind:  wire.CommandKVPut,
			KVID:  i,
			AID:   actor.ActorID,
			Key:   []byte(fmt.Sprintf("entry/%03d", i)),
			Value: []byte(fmt.Sprintf("value-%03d", i)),
		})
	}
	submitNativeCommands(t, runner, puts...)
	seen := make(map[uint64]struct{}, entryCount)
	deadline := time.Now().Add(20 * time.Second)
	for len(seen) < entryCount {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("received %d/%d KV put results", len(seen), entryCount)
		}
		data, pollErr := runner.Poll(min(remaining, time.Second))
		if pollErr != nil {
			t.Fatalf("poll KV put results: %v", pollErr)
		}
		batch, decodeErr := wire.DecodeEventBatch(data)
		if decodeErr != nil {
			t.Fatalf("decode KV put result batch: %v", decodeErr)
		}
		for _, event := range batch.Events {
			if event.Kind != wire.EventKVResult || event.KVID > entryCount {
				continue
			}
			if event.Error != nil {
				t.Fatalf("KV put %d failed with code %s", event.KVID, event.Error.Code)
			}
			seen[event.KVID] = struct{}{}
		}
	}

	limit := uint32(entryCount)
	const listID = 1_000
	submitNativeCommands(t, runner, wire.Command{
		Kind:   wire.CommandKVList,
		KVID:   listID,
		AID:    actor.ActorID,
		Prefix: []byte("entry/"),
		Limit:  &limit,
	})
	listed := waitForNativeKVResult(t, runner, listID, 10*time.Second)
	if listed.Error != nil {
		t.Fatalf("KV list failed with code %s", listed.Error.Code)
	}
	if len(listed.Entries) != entryCount {
		t.Fatalf("KV list entry count = %d, want %d", len(listed.Entries), entryCount)
	}

	const errorID = 1_001
	submitNativeCommands(t, runner, wire.Command{
		Kind: wire.CommandKVGet,
		KVID: errorID,
		AID:  "actor-that-is-not-active",
		Key:  []byte("missing"),
	})
	failed := waitForNativeKVResult(t, runner, errorID, 10*time.Second)
	if failed.Error == nil || failed.Error.Code != "actor_not_found" {
		t.Fatalf("KV error code = %v, want actor_not_found", failed.Error)
	}

	deleteActor(t, engine.endpoint, actor.ActorID)
	_, stopped := waitForNativeEvent(t, runner, wire.EventActorStop, 10*time.Second)
	submitNativeCommands(t, runner, wire.Command{
		Kind:       wire.CommandActorStopResult,
		AID:        stopped.AID,
		Generation: stopped.Generation,
	})
	waitForActor(t, engine.endpoint, actor.ActorID, true, func(actor actorRecord) bool {
		return actor.DestroyTS != nil
	})
	if err := runner.Shutdown(3 * time.Second); err != nil {
		t.Fatalf("shutdown KV runner: %v", err)
	}
	waitForNativeEvent(t, runner, wire.EventRunnerStopped, 5*time.Second)
}

func TestPublicActorIdentityAndKV(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type identity struct {
		ActorID string
		Name    string
		Key     string
	}
	type entry struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	started := make(chan identity, 1)
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "public-kv", rivet.Actor[struct{}]{
		OnStart: func(ctx *rivet.Context[struct{}]) error {
			started <- identity{ActorID: ctx.ActorID(), Name: ctx.Name(), Key: ctx.Key()}
			return nil
		},
		Actions: rivet.Actions[struct{}]{
			"put": rivet.Action(func(ctx *rivet.Context[struct{}], input entry) (bool, error) {
				return true, ctx.KV().Put(context.Background(), []byte(input.Key), []byte(input.Value))
			}),
			"get": rivet.Action(func(ctx *rivet.Context[struct{}], key string) (entry, error) {
				value, found, err := ctx.KV().Get(context.Background(), []byte(key))
				if err != nil || !found {
					return entry{}, err
				}
				return entry{Key: key, Value: string(value)}, nil
			}),
			"list": rivet.Action(func(ctx *rivet.Context[struct{}], prefix string) ([]entry, error) {
				entries, err := ctx.KV().List(context.Background(), rivet.KVListOptions{
					Prefix:  []byte(prefix),
					Reverse: true,
					Limit:   2,
				})
				if err != nil {
					return nil, err
				}
				result := make([]entry, len(entries))
				for index, item := range entries {
					result[index] = entry{Key: string(item.Key), Value: string(item.Value)}
				}
				return result, nil
			}),
			"delete": rivet.Action(func(ctx *rivet.Context[struct{}], key string) (bool, error) {
				return true, ctx.KV().Delete(context.Background(), []byte(key))
			}),
		},
	}); err != nil {
		t.Fatalf("register public KV actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-public-kv-%d", time.Now().UnixNano())
	served := startRegistry(t, engine, runnerName, registry)
	key := "tenant/player"
	actor := createActor(t, engine.endpoint, "public-kv", runnerName, "destroy", &key, nil)
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})

	select {
	case observed := <-started:
		if observed.ActorID != actor.ActorID || observed.Name != "public-kv" || observed.Key != key {
			t.Fatalf("public actor identity = %#v, actor = %#v", observed, actor)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("public KV actor OnStart did not run")
	}

	for _, item := range []entry{
		{Key: "items/a", Value: "one"},
		{Key: "items/b", Value: "two"},
		{Key: "items/c", Value: "three"},
	} {
		stored := decodeActionOutput[bool](t, gatewayAction(
			t, engine.endpoint, actor.ActorID, "put", []any{item}, 10*time.Second,
		), http.StatusOK)
		if !stored {
			t.Fatalf("put %q returned false", item.Key)
		}
	}
	got := decodeActionOutput[entry](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "get", []any{"items/b"}, 10*time.Second,
	), http.StatusOK)
	if got != (entry{Key: "items/b", Value: "two"}) {
		t.Fatalf("public KV get = %#v", got)
	}
	listed := decodeActionOutput[[]entry](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "list", []any{"items/"}, 10*time.Second,
	), http.StatusOK)
	if len(listed) != 2 || listed[0].Key != "items/c" || listed[1].Key != "items/b" {
		t.Fatalf("public KV reverse limited list = %#v", listed)
	}
	deleted := decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "delete", []any{"items/b"}, 10*time.Second,
	), http.StatusOK)
	if !deleted {
		t.Fatal("delete returned false")
	}
	listed = decodeActionOutput[[]entry](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "list", []any{"items/"}, 10*time.Second,
	), http.StatusOK)
	if len(listed) != 2 || listed[0].Key != "items/c" || listed[1].Key != "items/a" {
		t.Fatalf("public KV list after delete = %#v", listed)
	}

	deleteActor(t, engine.endpoint, actor.ActorID)
	served.stop(t)
	engine.stop()
}

func TestCounterStatePersistsAcrossEngineRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type observation struct {
		actorID    string
		generation uint64
		loaded     int
		current    int
		input      string
	}
	observations := make(chan observation, 8)
	stopped := make(chan observation, 2)
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "counter", rivet.Actor[persistentCounterState]{
		OnStart: func(ctx *rivet.Context[persistentCounterState]) error {
			loaded := ctx.State().Count
			if loaded == 0 {
				ctx.State().Count = 41
				saveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := ctx.Save(saveCtx); err != nil {
					return err
				}
			}
			observations <- observation{
				actorID:    ctx.ActorID(),
				generation: ctx.Generation(),
				loaded:     loaded,
				current:    ctx.State().Count,
				input:      string(ctx.Input()),
			}
			return nil
		},
		OnStop: func(ctx *rivet.Context[persistentCounterState]) error {
			stopped <- observation{
				actorID:    ctx.ActorID(),
				generation: ctx.Generation(),
				current:    ctx.State().Count,
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("register counter: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-counter-%d", time.Now().UnixNano())
	served := startRegistry(t, engine, runnerName, registry)
	key := "persistent-counter"
	actor := createActor(t, engine.endpoint, "counter", runnerName, "restart", &key, []byte("seed"))

	var first observation
	select {
	case first = <-observations:
	case <-time.After(30 * time.Second):
		t.Fatal("counter OnStart did not run before restart")
	}
	if first.actorID != actor.ActorID || first.loaded != 0 || first.current != 41 || first.input != "seed" {
		t.Fatalf("first counter observation = %#v, actor = %#v", first, actor)
	}
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})

	// Kill the engine while the actor is live and leave the runner transport
	// reconnecting. Before restarting the engine, require the lost-connection
	// path to stop the original Go actor worker. A later observation can then
	// only come from a new ActorStart and a newly decoded state value, not from
	// the first worker's in-process state.
	engine.kill(t)
	var firstStop observation
	select {
	case firstStop = <-stopped:
	case err := <-served.result:
		served.stopOnce.Do(func() {
			served.cancel()
			served.stopErr = err
		})
		t.Fatalf("runner exited while the engine was stopped: %v", err)
	case <-time.After(disconnectLivenessWindow + 10*time.Second):
		t.Fatal("original actor worker did not stop after the engine process was killed")
	}
	if firstStop.actorID != actor.ActorID || firstStop.generation != first.generation || firstStop.current != 41 {
		t.Fatalf("pre-restart stop observation = %#v, first start = %#v", firstStop, first)
	}

	// start reuses runningEngine.storage, so this launches a distinct OS
	// process against the exact database directory used by the killed process.
	engine.start(t)
	waitForActor(t, engine.endpoint, actor.ActorID, true, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && len(actor.Error) != 0
	})
	// A crashed actor with restart policy sleeps after the engine detects its
	// lost envoy. The gateway request is the public wake path; the management
	// reschedule endpoint deliberately does not wake sleeping actors.
	wakeResult := wakeActor(engine.endpoint, actor.ActorID, rehydrateWindow)
	var rehydrated observation
	select {
	case rehydrated = <-observations:
	case err := <-served.result:
		served.stopOnce.Do(func() {
			served.cancel()
			served.stopErr = err
		})
		t.Fatalf("runner exited before post-restart ActorStart: %v", err)
	case <-time.After(rehydrateWindow):
		actorAfterRestart, actorErr := getActor(engine.endpoint, actor.ActorID, true)
		envoysAfterRestart, envoyErr := listEnvoys(engine.endpoint, runnerName)
		var wakeErr error
		select {
		case wakeErr = <-wakeResult:
		default:
			wakeErr = errors.New("gateway wake request still pending")
		}
		t.Fatalf(
			"counter was not rehydrated after engine restart; actor_id=%s connectable=%t destroyed=%t has_error=%t actor_error=%v envoys=%d envoy_error=%v wake_error=%v",
			actorAfterRestart.ActorID,
			actorAfterRestart.ConnectableTS != nil,
			actorAfterRestart.DestroyTS != nil,
			len(actorAfterRestart.Error) != 0,
			actorErr,
			len(envoysAfterRestart),
			envoyErr,
			wakeErr,
		)
	}
	if rehydrated.actorID != actor.ActorID || rehydrated.loaded != 41 || rehydrated.current != 41 || rehydrated.input != "seed" {
		t.Fatalf("rehydrated counter observation = %#v, first = %#v", rehydrated, first)
	}
	if rehydrated.generation <= first.generation {
		t.Fatalf("rehydrated generation = %d, want greater than prior generation %d", rehydrated.generation, first.generation)
	}
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	deleteActor(t, engine.endpoint, actor.ActorID)
}

func TestTwoActorsOnOneRunnerHaveIsolatedState(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type isolatedState struct {
		Value string `json:"value"`
	}
	type observation struct {
		actorID string
		value   string
	}
	observations := make(chan observation, 4)
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "isolated", rivet.Actor[isolatedState]{
		OnStart: func(ctx *rivet.Context[isolatedState]) error {
			if ctx.State().Value == "" {
				ctx.State().Value = string(ctx.Input())
				saveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := ctx.Save(saveCtx); err != nil {
					return err
				}
			}
			observations <- observation{actorID: ctx.ActorID(), value: ctx.State().Value}
			return nil
		},
	}); err != nil {
		t.Fatalf("register isolated actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-isolation-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	firstActor := createActor(t, engine.endpoint, "isolated", runnerName, "restart", nil, []byte("first"))
	secondActor := createActor(t, engine.endpoint, "isolated", runnerName, "restart", nil, []byte("second"))

	seen := make(map[string]string)
	for len(seen) < 2 {
		select {
		case observation := <-observations:
			seen[observation.actorID] = observation.value
		case <-time.After(30 * time.Second):
			t.Fatalf("received isolated actor observations %#v", seen)
		}
	}
	if firstActor.ActorID == secondActor.ActorID {
		t.Fatal("engine returned the same ID for two actors")
	}
	if seen[firstActor.ActorID] != "first" || seen[secondActor.ActorID] != "second" {
		t.Fatalf("isolated state observations = %#v", seen)
	}
	deleteActor(t, engine.endpoint, firstActor.ActorID)
	deleteActor(t, engine.endpoint, secondActor.ActorID)
}

func TestActionsAndHTTPTunnelRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type counterState struct {
		Count int `json:"count"`
	}
	type slowArgument struct {
		Amount  int `json:"amount"`
		DelayMS int `json:"delay_ms"`
	}
	slowStarted := make(chan string, 1)
	deadlineFinished := make(chan struct{}, 1)
	lateWriteRelease := make(chan struct{})
	lateWriteResult := make(chan error, 1)
	streamBody := bytes.Repeat([]byte("rivet-go-stream\n"), 150_000)
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m3-counter", rivet.Actor[counterState]{
		Actions: rivet.Actions[counterState]{
			"increment": rivet.Action(func(ctx *rivet.Context[counterState], amount int) (int, error) {
				ctx.State().Count += amount
				return ctx.State().Count, nil
			}),
			"get": rivet.Action(func(ctx *rivet.Context[counterState], _ struct{}) (int, error) {
				return ctx.State().Count, nil
			}),
			"raw": rivet.RawAction(func(_ *rivet.Context[counterState], encoded []byte) ([]byte, error) {
				return cbor.Marshal(map[string]any{"argument_bytes": len(encoded)})
			}),
			"encode_failure": rivet.Action(func(*rivet.Context[counterState], struct{}) (chan int, error) {
				return make(chan int), nil
			}),
			"deadline": rivet.ActionWithContext(func(ctx context.Context, _ *rivet.Context[counterState], _ struct{}) (int, error) {
				<-ctx.Done()
				deadlineFinished <- struct{}{}
				return 0, ctx.Err()
			}),
			"slow_increment": rivet.Action(func(ctx *rivet.Context[counterState], input slowArgument) (int, error) {
				slowStarted <- ctx.ActorID()
				time.Sleep(time.Duration(input.DelayMS) * time.Millisecond)
				ctx.State().Count += input.Amount
				return ctx.State().Count, nil
			}),
			"panic": rivet.Action(func(*rivet.Context[counterState], struct{}) (int, error) {
				panic("intentional M3 action panic")
			}),
			"reject": rivet.Action(func(*rivet.Context[counterState], struct{}) (int, error) {
				return 0, rivet.ActionError{Code: "counter_rejected", Message: "counter rejected the action"}
			}),
		},
		OnFetch: func(_ *rivet.Context[counterState], writer http.ResponseWriter, request *http.Request) {
			switch request.URL.Path {
			case "/panic":
				panic("intentional M3 fetch panic")
			case "/inspect":
				body, err := io.ReadAll(request.Body)
				if err != nil {
					panic(fmt.Sprintf("read inspected request body: %v", err))
				}
				digest := sha256.Sum256(body)
				writer.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(writer).Encode(map[string]any{
					"method":          request.Method,
					"path":            request.URL.Path,
					"query":           request.URL.RawQuery,
					"host":            request.Host,
					"repeated_header": request.Header.Values("X-Repeated"),
					"cookie_length":   len(request.Header.Get("Cookie")),
					"body_length":     len(body),
					"body_sha256":     fmt.Sprintf("%x", digest),
				}); err != nil {
					panic(fmt.Sprintf("write inspected response: %v", err))
				}
				return
			case "/content-length":
				writer.Header().Set("Content-Length", "5")
				_, _ = writer.Write([]byte("hello"))
				return
			case "/bad-content-length":
				writer.Header().Set("Content-Length", "4")
				_, _ = writer.Write([]byte("hello"))
				return
			case "/too-many-headers":
				for index := range 257 {
					writer.Header().Set(fmt.Sprintf("X-Limit-%03d", index), "value")
				}
				writer.WriteHeader(http.StatusOK)
				return
			case "/repeated-cookie":
				writer.Header().Add("Set-Cookie", "first=1")
				writer.Header().Add("Set-Cookie", "second=2")
				writer.WriteHeader(http.StatusOK)
				return
			case "/oversized-header":
				writer.Header().Set("X-Large", strings.Repeat("v", (1<<20)+1))
				writer.WriteHeader(http.StatusOK)
				return
			case "/late-write":
				go func() {
					<-lateWriteRelease
					_, err := writer.Write([]byte("late"))
					lateWriteResult <- err
				}()
				return
			case "/concurrent-writes":
				var writes sync.WaitGroup
				writeErrors := make(chan error, 32)
				for range 32 {
					writes.Add(1)
					go func() {
						defer writes.Done()
						if _, err := writer.Write([]byte("chunk\n")); err != nil {
							writeErrors <- err
						}
					}()
				}
				writes.Wait()
				close(writeErrors)
				for err := range writeErrors {
					panic(fmt.Sprintf("concurrent response write: %v", err))
				}
				return
			}
			writer.Header().Set("Content-Type", "text/plain")
			writer.Header().Set("X-Rivet-Go-M3", "streamed")
			writer.WriteHeader(http.StatusCreated)
			first := len(streamBody)/2 + 137
			if _, err := writer.Write(streamBody[:first]); err != nil {
				panic(fmt.Sprintf("write first response segment: %v", err))
			}
			if _, err := writer.Write(streamBody[first:]); err != nil {
				panic(fmt.Sprintf("write second response segment: %v", err))
			}
		},
	}); err != nil {
		t.Fatalf("register M3 counter: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m3-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	primary := createActor(t, engine.endpoint, "m3-counter", runnerName, "destroy", nil, nil)
	fast := createActor(t, engine.endpoint, "m3-counter", runnerName, "destroy", nil, nil)
	slow := createActor(t, engine.endpoint, "m3-counter", runnerName, "destroy", nil, nil)
	deadlineActor := createActor(t, engine.endpoint, "m3-counter", runnerName, "destroy", nil, nil)
	for _, actor := range []actorRecord{primary, fast, slow, deadlineActor} {
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})
	}

	increment := gatewayAction(t, engine.endpoint, primary.ActorID, "increment", []any{3}, 10*time.Second)
	assertActionOutput(t, increment, http.StatusOK, 3)
	observed := gatewayAction(t, engine.endpoint, primary.ActorID, "get", []any{struct{}{}}, 10*time.Second)
	assertActionOutput(t, observed, http.StatusOK, 3)
	assertGatewayError(
		t,
		gatewayAction(t, engine.endpoint, primary.ActorID, "increment", []any{}, 10*time.Second),
		http.StatusInternalServerError,
		"actor",
		"action_decode_failed",
	)
	assertGatewayError(
		t,
		gatewayAction(t, engine.endpoint, primary.ActorID, "increment", []any{"wrong-type"}, 10*time.Second),
		http.StatusInternalServerError,
		"actor",
		"action_decode_failed",
	)
	assertGatewayError(
		t,
		gatewayAction(t, engine.endpoint, primary.ActorID, "encode_failure", []any{struct{}{}}, 10*time.Second),
		http.StatusInternalServerError,
		"actor",
		"action_encode_failed",
	)
	rawAction := gatewayAction(t, engine.endpoint, primary.ActorID, "raw", []any{"kept-raw"}, 10*time.Second)
	if rawAction.err != nil || rawAction.response == nil || rawAction.response.StatusCode != http.StatusOK {
		t.Fatalf("raw action response: status=%s err=%v body=%s", responseStatus(rawAction.response), rawAction.err, rawAction.body)
	}
	var rawOutput struct {
		Output struct {
			ArgumentBytes int `json:"argument_bytes"`
		} `json:"output"`
	}
	if err := json.Unmarshal(rawAction.body, &rawOutput); err != nil || rawOutput.Output.ArgumentBytes == 0 {
		t.Fatalf("raw action output = %#v, decode error = %v", rawOutput, err)
	}

	deadlineResult := make(chan gatewayResponse, 1)
	go func() {
		deadlineResult <- gatewayAction(
			t,
			engine.endpoint,
			deadlineActor.ActorID,
			"deadline",
			[]any{struct{}{}},
			70*time.Second,
		)
	}()

	slowResult := make(chan gatewayResponse, 1)
	go func() {
		response, body, requestErr := gatewayRequest(
			engine.endpoint,
			slow.ActorID,
			"/action/slow_increment",
			map[string]any{"args": []any{slowArgument{Amount: 4, DelayMS: 1_200}}},
			5*time.Second,
		)
		slowResult <- gatewayResponse{response: response, body: body, err: requestErr}
	}()
	select {
	case actorID := <-slowStarted:
		if actorID != slow.ActorID {
			t.Fatalf("slow action actor ID = %s, want %s", actorID, slow.ActorID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("slow action did not reach the Go handler")
	}
	secondSlowResult := make(chan gatewayResponse, 1)
	go func() {
		secondSlowResult <- gatewayAction(t, engine.endpoint, slow.ActorID, "increment", []any{2}, 5*time.Second)
	}()
	fastStarted := time.Now()
	fastResponse := gatewayAction(t, engine.endpoint, fast.ActorID, "increment", []any{9}, 5*time.Second)
	assertActionOutput(t, fastResponse, http.StatusOK, 9)
	if elapsed := time.Since(fastStarted); elapsed > 750*time.Millisecond {
		t.Fatalf("fast actor action took %s while peer was slow", elapsed)
	}
	select {
	case result := <-slowResult:
		t.Fatalf("slow action completed before isolation assertion: status=%v err=%v body=%s", responseStatus(result.response), result.err, result.body)
	default:
	}
	select {
	case result := <-secondSlowResult:
		t.Fatalf("second same-actor action completed before the first: status=%v err=%v body=%s", responseStatus(result.response), result.err, result.body)
	default:
	}
	result := <-slowResult
	if result.err != nil {
		t.Fatalf("slow action request: %v", result.err)
	}
	assertActionOutput(t, result, http.StatusOK, 4)
	assertActionOutput(t, <-secondSlowResult, http.StatusOK, 6)

	rawResponse, rawBody, err := gatewayRequest(
		engine.endpoint,
		primary.ActorID,
		"/request/stream",
		nil,
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("raw HTTP gateway request: %v", err)
	}
	if rawResponse.StatusCode != http.StatusCreated {
		t.Fatalf("raw HTTP status = %s, want 201", rawResponse.Status)
	}
	if rawResponse.Header.Get("X-Rivet-Go-M3") != "streamed" {
		t.Fatalf("raw HTTP response header = %q", rawResponse.Header.Get("X-Rivet-Go-M3"))
	}
	if !bytes.Equal(rawBody, streamBody) || len(rawBody) <= 2*(1<<20) {
		t.Fatalf("raw HTTP response length = %d, want %d bytes across multiple chunks", len(rawBody), len(streamBody))
	}
	if rawResponse.ContentLength >= 0 && rawResponse.ContentLength != int64(len(streamBody)) {
		t.Fatalf("raw HTTP Content-Length = %d, body length = %d", rawResponse.ContentLength, len(streamBody))
	}

	requestBody := bytes.Repeat([]byte("request-chunk\n"), 170_000)
	requestDigest := sha256.Sum256(requestBody)
	requestHeaders := make(http.Header)
	requestHeaders.Add("X-Repeated", "first")
	requestHeaders.Add("X-Repeated", "second")
	requestHeaders.Set("Cookie", "session="+strings.Repeat("c", 16<<10))
	inspectResponse, inspectBody, err := gatewayHTTPRequest(
		engine.endpoint,
		primary.ActorID,
		"/request/inspect?part=one&part=two",
		http.MethodPatch,
		requestBody,
		requestHeaders,
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("inspect HTTP gateway request: %v", err)
	}
	if inspectResponse.StatusCode != http.StatusOK {
		t.Fatalf("inspect HTTP status = %s; body=%s", inspectResponse.Status, inspectBody)
	}
	var inspected struct {
		Method         string   `json:"method"`
		Path           string   `json:"path"`
		Query          string   `json:"query"`
		Host           string   `json:"host"`
		RepeatedHeader []string `json:"repeated_header"`
		CookieLength   int      `json:"cookie_length"`
		BodyLength     int      `json:"body_length"`
		BodySHA256     string   `json:"body_sha256"`
	}
	if err := json.Unmarshal(inspectBody, &inspected); err != nil {
		t.Fatalf("decode inspected request: %v; body=%s", err, inspectBody)
	}
	if inspected.Method != http.MethodPatch || inspected.Path != "/inspect" ||
		inspected.Query != "part=one&part=two" || inspected.Host == "" ||
		inspected.BodyLength != len(requestBody) ||
		inspected.BodySHA256 != fmt.Sprintf("%x", requestDigest) ||
		inspected.CookieLength != len(requestHeaders.Get("Cookie")) {
		t.Fatalf("inspected request = %#v", inspected)
	}
	if len(inspected.RepeatedHeader) != 1 || inspected.RepeatedHeader[0] != "second" {
		t.Fatalf("pinned-core repeated request header = %#v, want the last value", inspected.RepeatedHeader)
	}
	tooManyRequestHeaders := make(http.Header)
	for index := range 257 {
		tooManyRequestHeaders.Set(fmt.Sprintf("X-Request-Limit-%03d", index), "value")
	}
	tooManyResponse, tooManyBody, err := gatewayHTTPRequest(
		engine.endpoint,
		primary.ActorID,
		"/request/inspect",
		http.MethodGet,
		nil,
		tooManyRequestHeaders,
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("request header-limit gateway call: %v", err)
	}
	if tooManyResponse.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("request header-limit status = %s, want 431; body=%s", tooManyResponse.Status, tooManyBody)
	}

	lengthResponse, lengthBody, err := gatewayRequest(
		engine.endpoint,
		primary.ActorID,
		"/request/content-length",
		nil,
		10*time.Second,
	)
	if err != nil || string(lengthBody) != "hello" || lengthResponse.ContentLength != int64(len(lengthBody)) {
		t.Fatalf("content-length response: status=%s length=%d body=%q err=%v", responseStatus(lengthResponse), responseContentLength(lengthResponse), lengthBody, err)
	}
	badLengthResponse, badLengthBody, err := gatewayRequest(
		engine.endpoint,
		primary.ActorID,
		"/request/bad-content-length",
		nil,
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("bad content-length request: %v", err)
	}
	assertGatewayError(
		t,
		gatewayResponse{response: badLengthResponse, body: badLengthBody},
		http.StatusInternalServerError,
		"actor",
		"http_response_content_length_mismatch",
	)

	for path, code := range map[string]string{
		"/request/too-many-headers": "http_response_headers_too_large",
		"/request/repeated-cookie":  "http_response_repeated_header_unsupported",
		"/request/oversized-header": "http_response_header_too_large",
	} {
		response, body, requestErr := gatewayRequest(engine.endpoint, primary.ActorID, path, nil, 10*time.Second)
		if requestErr != nil {
			t.Fatalf("%s: %v", path, requestErr)
		}
		assertGatewayError(
			t,
			gatewayResponse{response: response, body: body},
			http.StatusInternalServerError,
			"actor",
			code,
		)
	}

	concurrentResponse, concurrentBody, err := gatewayRequest(
		engine.endpoint,
		primary.ActorID,
		"/request/concurrent-writes",
		nil,
		10*time.Second,
	)
	if err != nil || concurrentResponse.StatusCode != http.StatusOK ||
		len(concurrentBody) != 32*len("chunk\n") || bytes.Count(concurrentBody, []byte("chunk\n")) != 32 {
		t.Fatalf("concurrent response: status=%s bytes=%d chunks=%d err=%v", responseStatus(concurrentResponse), len(concurrentBody), bytes.Count(concurrentBody, []byte("chunk\n")), err)
	}
	lateResponse, lateBody, err := gatewayRequest(
		engine.endpoint,
		primary.ActorID,
		"/request/late-write",
		nil,
		10*time.Second,
	)
	if err != nil || lateResponse.StatusCode != http.StatusOK || len(lateBody) != 0 {
		t.Fatalf("late-write initial response: status=%s body=%q err=%v", responseStatus(lateResponse), lateBody, err)
	}
	close(lateWriteRelease)
	select {
	case lateErr := <-lateWriteResult:
		if lateErr == nil || !strings.Contains(lateErr.Error(), "after OnFetch returned") {
			t.Fatalf("late response write error = %v", lateErr)
		}
	case <-time.After(time.Second):
		t.Fatal("late response writer did not reject the write")
	}

	missing := gatewayAction(t, engine.endpoint, primary.ActorID, "missing", []any{}, 10*time.Second)
	assertGatewayError(t, missing, http.StatusNotFound, "actor", "action_not_found")
	rejected := gatewayAction(t, engine.endpoint, primary.ActorID, "reject", []any{struct{}{}}, 10*time.Second)
	assertGatewayError(t, rejected, http.StatusInternalServerError, "actor", "counter_rejected")

	panicActionActor := createActor(t, engine.endpoint, "m3-counter", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, panicActionActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil
	})
	panickedAction := gatewayAction(t, engine.endpoint, panicActionActor.ActorID, "panic", []any{struct{}{}}, 10*time.Second)
	assertGatewayError(t, panickedAction, http.StatusInternalServerError, "actor", "handler_panic")
	waitForActor(t, engine.endpoint, panicActionActor.ActorID, true, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil || actor.DestroyTS != nil || (len(actor.Error) != 0 && string(actor.Error) != "null")
	})
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, fast.ActorID, "get", []any{struct{}{}}, 10*time.Second),
		http.StatusOK,
		9,
	)

	panicFetchActor := createActor(t, engine.endpoint, "m3-counter", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, panicFetchActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil
	})
	panicFetchResponse, panicFetchBody, err := gatewayRequest(
		engine.endpoint,
		panicFetchActor.ActorID,
		"/request/panic",
		nil,
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("panicking fetch request: %v", err)
	}
	assertGatewayError(t, gatewayResponse{response: panicFetchResponse, body: panicFetchBody}, http.StatusInternalServerError, "actor", "handler_panic")
	waitForActor(t, engine.endpoint, panicFetchActor.ActorID, true, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil || actor.DestroyTS != nil || (len(actor.Error) != 0 && string(actor.Error) != "null")
	})
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, fast.ActorID, "get", []any{struct{}{}}, 10*time.Second),
		http.StatusOK,
		9,
	)

	select {
	case timedOut := <-deadlineResult:
		assertGatewayError(t, timedOut, http.StatusRequestTimeout, "actor", "action_timed_out")
	case <-time.After(75 * time.Second):
		t.Fatal("core action deadline did not return a gateway response")
	}
	select {
	case <-deadlineFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("Go action context did not observe the pinned core deadline")
	}
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, deadlineActor.ActorID, "get", []any{struct{}{}}, 10*time.Second),
		http.StatusOK,
		0,
	)
}

func TestActionImplicitSavePersistsAcrossEngineRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type counterState struct {
		Count int `json:"count"`
	}
	type observation struct {
		actorID    string
		generation uint64
		count      int
	}
	started := make(chan observation, 4)
	stopped := make(chan observation, 2)
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m3-persistent-action", rivet.Actor[counterState]{
		OnStart: func(ctx *rivet.Context[counterState]) error {
			started <- observation{actorID: ctx.ActorID(), generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
		OnStop: func(ctx *rivet.Context[counterState]) error {
			stopped <- observation{actorID: ctx.ActorID(), generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
		Actions: rivet.Actions[counterState]{
			"increment": rivet.Action(func(ctx *rivet.Context[counterState], amount int) (int, error) {
				ctx.State().Count += amount
				return ctx.State().Count, nil
			}),
			"get": rivet.Action(func(ctx *rivet.Context[counterState], _ struct{}) (int, error) {
				return ctx.State().Count, nil
			}),
		},
	}); err != nil {
		t.Fatalf("register persistent M3 action actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m3-persistence-%d", time.Now().UnixNano())
	served := startRegistry(t, engine, runnerName, registry)
	key := "m3-action-persistence"
	actor := createActor(t, engine.endpoint, "m3-persistent-action", runnerName, "restart", &key, nil)

	var first observation
	select {
	case first = <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("persistent action actor did not start")
	}
	if first.actorID != actor.ActorID || first.count != 0 {
		t.Fatalf("first action actor observation = %#v", first)
	}
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, actor.ActorID, "increment", []any{17}, 10*time.Second),
		http.StatusOK,
		17,
	)

	engine.kill(t)
	select {
	case stoppedActor := <-stopped:
		if stoppedActor.actorID != actor.ActorID || stoppedActor.generation != first.generation || stoppedActor.count != 17 {
			t.Fatalf("pre-restart action stop observation = %#v", stoppedActor)
		}
	case err := <-served.result:
		served.stopOnce.Do(func() {
			served.cancel()
			served.stopErr = err
		})
		t.Fatalf("runner exited while the engine was stopped: %v", err)
	case <-time.After(disconnectLivenessWindow + 10*time.Second):
		t.Fatal("action actor worker did not stop after engine loss")
	}

	engine.start(t)
	waitForActor(t, engine.endpoint, actor.ActorID, true, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && len(actor.Error) != 0
	})
	wakeResult := wakeActor(engine.endpoint, actor.ActorID, rehydrateWindow)
	var rehydrated observation
	select {
	case rehydrated = <-started:
	case err := <-served.result:
		served.stopOnce.Do(func() {
			served.cancel()
			served.stopErr = err
		})
		t.Fatalf("runner exited before action actor rehydration: %v", err)
	case <-time.After(rehydrateWindow):
		t.Fatal("action actor was not rehydrated after engine restart")
	}
	if rehydrated.actorID != actor.ActorID || rehydrated.count != 17 || rehydrated.generation <= first.generation {
		t.Fatalf("rehydrated action actor = %#v, first = %#v", rehydrated, first)
	}
	select {
	case wakeErr := <-wakeResult:
		if wakeErr != nil {
			t.Fatalf("gateway wake request: %v", wakeErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("gateway wake request did not complete")
	}
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, actor.ActorID, "get", []any{struct{}{}}, 10*time.Second),
		http.StatusOK,
		17,
	)
	deleteActor(t, engine.endpoint, actor.ActorID)
}

func TestActorDestroyRunsGoCleanupHook(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type cleanupState struct {
		Started bool `json:"started"`
		Stopped bool `json:"stopped"`
	}
	type cleanupObservation struct {
		actorID string
		err     error
	}
	started := make(chan string, 1)
	cleaned := make(chan cleanupObservation, 1)
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "cleanup", rivet.Actor[cleanupState]{
		OnStart: func(ctx *rivet.Context[cleanupState]) error {
			ctx.State().Started = true
			saveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := ctx.Save(saveCtx); err != nil {
				return err
			}
			started <- ctx.ActorID()
			return nil
		},
		OnStop: func(ctx *rivet.Context[cleanupState]) error {
			ctx.State().Stopped = true
			saveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := ctx.Save(saveCtx)
			cleaned <- cleanupObservation{actorID: ctx.ActorID(), err: err}
			return err
		},
	}); err != nil {
		t.Fatalf("register cleanup actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-cleanup-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "cleanup", runnerName, "destroy", nil, nil)
	select {
	case actorID := <-started:
		if actorID != actor.ActorID {
			t.Fatalf("OnStart actor ID = %s, want %s", actorID, actor.ActorID)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("cleanup actor did not start")
	}

	deleteActor(t, engine.endpoint, actor.ActorID)
	select {
	case observation := <-cleaned:
		if observation.actorID != actor.ActorID || observation.err != nil {
			t.Fatalf("cleanup observation = %#v", observation)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("DELETE /actors did not run Go OnStop cleanup")
	}
	waitForActor(t, engine.endpoint, actor.ActorID, true, func(actor actorRecord) bool {
		return actor.DestroyTS != nil
	})
}

func TestPanickingActorDoesNotKillRunner(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "panics", rivet.Actor[struct{}]{
		OnStart: func(*rivet.Context[struct{}]) error {
			panic("intentional lifecycle panic")
		},
	}); err != nil {
		t.Fatalf("register panic actor: %v", err)
	}
	if err := rivet.Register(registry, "panics-stop", rivet.Actor[struct{}]{
		OnStop: func(*rivet.Context[struct{}]) error {
			panic("intentional stop panic")
		},
	}); err != nil {
		t.Fatalf("register stop-panic actor: %v", err)
	}
	healthyStarted := make(chan string, 1)
	if err := rivet.Register(registry, "healthy", rivet.Actor[struct{}]{
		OnStart: func(ctx *rivet.Context[struct{}]) error {
			healthyStarted <- ctx.ActorID()
			return nil
		},
	}); err != nil {
		t.Fatalf("register healthy actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-panic-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	panicked := createActor(t, engine.endpoint, "panics", runnerName, "destroy", nil, nil)
	failed := waitForActor(t, engine.endpoint, panicked.ActorID, true, func(actor actorRecord) bool {
		return len(actor.Error) > 0 && string(actor.Error) != "null"
	})
	if len(failed.Error) == 0 || string(failed.Error) == "null" {
		t.Fatalf("panicking actor has no structured engine error: %#v", failed)
	}
	if failed.ConnectableTS != nil {
		t.Fatalf("panicking actor became connectable: %#v", failed)
	}

	panicsOnStop := createActor(t, engine.endpoint, "panics-stop", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, panicsOnStop.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	deleteActor(t, engine.endpoint, panicsOnStop.ActorID)
	stoppedAfterPanic := waitForActor(t, engine.endpoint, panicsOnStop.ActorID, true, func(actor actorRecord) bool {
		return actor.DestroyTS != nil
	})
	if stoppedAfterPanic.DestroyTS == nil {
		t.Fatalf("OnStop panic actor did not reach an engine-visible stopped state: %#v", stoppedAfterPanic)
	}

	healthy := createActor(t, engine.endpoint, "healthy", runnerName, "destroy", nil, nil)
	select {
	case actorID := <-healthyStarted:
		if actorID != healthy.ActorID {
			t.Fatalf("healthy OnStart actor ID = %s, want %s", actorID, healthy.ActorID)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("healthy actor did not start after peer panic")
	}
	deleteActor(t, engine.endpoint, healthy.ActorID)
}

func acquireEngine(ctx context.Context) (string, error) {
	return devengine.Acquire(ctx)
}

func startEngine(t *testing.T, binary string) *runningEngine {
	t.Helper()
	port, err := devengine.ReservePortRange()
	if err != nil {
		t.Fatalf("reserve engine ports: %v", err)
	}
	process, err := devengine.New(binary, t.TempDir(), port)
	if err != nil {
		t.Fatalf("configure engine: %v", err)
	}
	engine := &runningEngine{process: process, endpoint: process.Endpoint, storage: process.StorageDir}
	engine.start(t)
	t.Cleanup(func() { engine.stop() })
	return engine
}

func (e *runningEngine) start(t *testing.T) {
	t.Helper()
	if err := e.process.Start(context.Background()); err != nil {
		t.Fatalf("start engine %s: %v", e.process.Binary, err)
	}
}

func (e *runningEngine) kill(t *testing.T) {
	t.Helper()
	if err := e.process.Kill(); err != nil {
		t.Fatalf("kill engine: %v", err)
	}
}

func (e *runningEngine) restart(t *testing.T) {
	t.Helper()
	e.kill(t)
	e.start(t)
}

func (e *runningEngine) stop() {
	_ = e.process.Kill()
}

func listEnvoys(endpoint, runnerName string) ([]envoyRecord, error) {
	request, err := http.NewRequest(http.MethodGet, endpoint+"/envoys?namespace=default&name="+runnerName, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer dev")
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return nil, fmt.Errorf("GET /envoys returned %s: %s", response.Status, body)
	}
	var list envoyListResponse
	if err := json.NewDecoder(response.Body).Decode(&list); err != nil {
		return nil, err
	}
	return list.Envoys, nil
}

type servedRegistry struct {
	cancel   context.CancelFunc
	result   chan error
	stopOnce sync.Once
	stopErr  error
}

func startRegistry(t *testing.T, engine *runningEngine, runnerName string, registry *rivet.Registry) *servedRegistry {
	return startRegistryWithConfig(t, engine, runnerName, registry, rivet.Config{})
}

func startRegistryWithConfig(
	t *testing.T,
	engine *runningEngine,
	runnerName string,
	registry *rivet.Registry,
	config rivet.Config,
) *servedRegistry {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	served := &servedRegistry{cancel: cancel, result: make(chan error, 1)}
	config.Endpoint = engine.endpoint
	config.Namespace = "default"
	config.RunnerName = runnerName
	config.Version = 1
	config.TotalSlots = 16
	config.LogLevel = "error"
	go func() {
		served.result <- registry.Serve(ctx, config)
	}()
	t.Cleanup(func() { served.stop(t) })

	eventually(t, 20*time.Second, func() (bool, error) {
		select {
		case err := <-served.result:
			served.stopOnce.Do(func() {
				served.cancel()
				served.stopErr = err
			})
			if err == nil {
				return false, errors.New("runner Serve exited before registration")
			}
			return false, fmt.Errorf("runner Serve exited before registration: %w", err)
		default:
		}
		envoys, err := listEnvoys(engine.endpoint, runnerName)
		if err != nil {
			return false, err
		}
		for _, envoy := range envoys {
			if envoy.PoolName == runnerName && envoy.StopTS == nil {
				return true, nil
			}
		}
		return false, nil
	})
	return served
}

func (s *servedRegistry) stop(t *testing.T) {
	t.Helper()
	s.stopOnce.Do(func() {
		s.cancel()
		select {
		case s.stopErr = <-s.result:
		case <-time.After(15 * time.Second):
			s.stopErr = errors.New("runner did not finish graceful shutdown")
		}
	})
	if s.stopErr != nil {
		t.Fatalf("runner Serve shutdown: %v", s.stopErr)
	}
}

func gatewayAction(
	t *testing.T,
	endpoint, actorID, action string,
	args []any,
	timeout time.Duration,
) gatewayResponse {
	t.Helper()
	response, body, err := gatewayRequest(
		endpoint,
		actorID,
		"/action/"+url.PathEscape(action),
		map[string]any{"args": args},
		timeout,
	)
	return gatewayResponse{response: response, body: body, err: err}
}

func gatewayRequest(
	endpoint, actorID, path string,
	payload any,
	timeout time.Duration,
) (*http.Response, []byte, error) {
	method := http.MethodGet
	var body []byte
	headers := make(http.Header)
	if payload != nil {
		method = http.MethodPost
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, nil, fmt.Errorf("encode gateway request: %w", err)
		}
		body = encoded
		headers.Set("Content-Type", "application/json")
	}
	return gatewayHTTPRequest(endpoint, actorID, path, method, body, headers, timeout)
}

func gatewayHTTPRequest(
	endpoint, actorID, path, method string,
	body []byte,
	headers http.Header,
	timeout time.Duration,
) (*http.Response, []byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	request, err := http.NewRequest(
		method,
		endpoint+"/gateway/"+url.PathEscape(actorID)+path,
		bodyReader,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("build gateway request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer dev")
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	response, err := (&http.Client{Timeout: timeout}).Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return response, nil, fmt.Errorf("read gateway response: %w", err)
	}
	return response, responseBody, nil
}

func assertActionOutput(t *testing.T, result gatewayResponse, status, output int) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("action request: %v", result.err)
	}
	if result.response == nil || result.response.StatusCode != status {
		t.Fatalf("action status = %s, want %d; body=%s", responseStatus(result.response), status, result.body)
	}
	var response struct {
		Output int `json:"output"`
	}
	if err := json.Unmarshal(result.body, &response); err != nil {
		t.Fatalf("decode action response: %v; body=%s", err, result.body)
	}
	if response.Output != output {
		t.Fatalf("action output = %d, want %d", response.Output, output)
	}
}

func assertGatewayError(
	t *testing.T,
	result gatewayResponse,
	status int,
	group, code string,
) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("gateway error request: %v", result.err)
	}
	if result.response == nil || result.response.StatusCode != status {
		t.Fatalf("gateway error status = %s, want %d; body=%s", responseStatus(result.response), status, result.body)
	}
	var response struct {
		Group   string `json:"group"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(result.body, &response); err != nil {
		t.Fatalf("decode gateway error: %v; body=%s", err, result.body)
	}
	if response.Group != group || response.Code != code || response.Message == "" {
		t.Fatalf("gateway structured error = %#v, want group=%q code=%q", response, group, code)
	}
}

func responseStatus(response *http.Response) string {
	if response == nil {
		return "<no response>"
	}
	return response.Status
}

func responseContentLength(response *http.Response) int64 {
	if response == nil {
		return -1
	}
	return response.ContentLength
}

func createActor(
	t *testing.T,
	endpoint, name, runnerName, crashPolicy string,
	key *string,
	input []byte,
) actorRecord {
	t.Helper()
	payload := struct {
		Name               string  `json:"name"`
		RunnerNameSelector string  `json:"runner_name_selector"`
		CrashPolicy        string  `json:"crash_policy"`
		Key                *string `json:"key,omitempty"`
		Input              *string `json:"input,omitempty"`
	}{
		Name:               name,
		RunnerNameSelector: runnerName,
		CrashPolicy:        crashPolicy,
		Key:                key,
	}
	if input != nil {
		encoded := base64.StdEncoding.EncodeToString(input)
		payload.Input = &encoded
	}
	var response actorCreateResponse
	requestJSON(t, http.MethodPost, endpoint+"/actors?namespace=default", payload, &response)
	if response.Actor.ActorID == "" {
		t.Fatal("POST /actors returned an empty actor_id")
	}
	return response.Actor
}

func deleteActor(t *testing.T, endpoint, actorID string) {
	t.Helper()
	requestJSON(
		t,
		http.MethodDelete,
		endpoint+"/actors/"+url.PathEscape(actorID)+"?namespace=default",
		nil,
		&struct{}{},
	)
}

func wakeActor(endpoint, actorID string, timeout time.Duration) <-chan error {
	result := make(chan error, 1)
	go func() {
		request, err := http.NewRequest(
			http.MethodGet,
			endpoint+"/gateway/"+url.PathEscape(actorID)+"/m2-rehydrate",
			nil,
		)
		if err != nil {
			result <- fmt.Errorf("build gateway wake request: %w", err)
			return
		}
		request.Header.Set("Authorization", "Bearer dev")
		response, err := (&http.Client{Timeout: timeout}).Do(request)
		if err != nil {
			result <- fmt.Errorf("gateway wake request: %w", err)
			return
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
		result <- nil
	}()
	return result
}

func getActor(endpoint, actorID string, includeDestroyed bool) (actorRecord, error) {
	query := url.Values{}
	query.Set("namespace", "default")
	query.Add("actor_id", actorID)
	if includeDestroyed {
		query.Set("include_destroyed", "true")
	}
	request, err := http.NewRequest(http.MethodGet, endpoint+"/actors?"+query.Encode(), nil)
	if err != nil {
		return actorRecord{}, err
	}
	request.Header.Set("Authorization", "Bearer dev")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return actorRecord{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return actorRecord{}, fmt.Errorf("GET /actors returned %s: %s", response.Status, body)
	}
	var list actorListResponse
	if err := json.NewDecoder(response.Body).Decode(&list); err != nil {
		return actorRecord{}, err
	}
	for _, actor := range list.Actors {
		if actor.ActorID == actorID {
			return actor, nil
		}
	}
	return actorRecord{}, fmt.Errorf("actor %s is absent from GET /actors", actorID)
}

func requestJSON(t *testing.T, method, endpoint string, input, output any) {
	t.Helper()
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatalf("encode %s request: %v", method, err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		t.Fatalf("build %s request: %v", method, err)
	}
	request.Header.Set("Authorization", "Bearer dev")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := (&http.Client{Timeout: 20 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, endpoint, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
		t.Fatalf("%s %s returned %s: %s", method, endpoint, response.Status, responseBody)
	}
	if output != nil {
		if err := json.NewDecoder(response.Body).Decode(output); err != nil {
			t.Fatalf("decode %s %s response: %v", method, endpoint, err)
		}
	}
}

func waitForActor(t *testing.T, endpoint, actorID string, includeDestroyed bool, check func(actorRecord) bool) actorRecord {
	t.Helper()
	var actor actorRecord
	eventually(t, 30*time.Second, func() (bool, error) {
		current, err := getActor(endpoint, actorID, includeDestroyed)
		if err != nil {
			return false, err
		}
		actor = current
		return check(current), nil
	})
	return actor
}

func eventually(t *testing.T, timeout time.Duration, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if ok {
			return
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("condition did not become true within %s: %v", timeout, lastErr)
	}
	t.Fatalf("condition did not become true within %s", timeout)
}

func engineRemediation() string {
	return "Set RIVET_GO_ENGINE_BIN to a v2.3.10 rivet-engine binary, or install git + Rust and retry. " +
		"The automatic fallback clones tag v2.3.10 and runs `cargo build -p rivet-engine --release`; see conformance/README.md."
}

// silentEndpoint returns a URL whose listener accepts connections but never
// responds, forcing rk_runner_new to hit its startup deadline.
func silentEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	})
	return "http://" + listener.Addr().String()
}

// closedEndpoint returns a URL for a port that was just released, so
// connections are refused.
func closedEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	endpoint := "http://" + listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return endpoint
}

// nonEngineEndpoint returns a live HTTP server that is not a Rivet engine.
func nonEngineEndpoint(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not a rivet engine", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func expectRunnerNewError(t *testing.T, endpoint string) ffi.ErrorPayload {
	t.Helper()
	config, err := wire.EncodeRunnerConfig(wire.RunnerConfig{
		EngineEndpoint:           endpoint,
		Namespace:                "default",
		RunnerName:               "rivet-go-error-probe",
		Version:                  1,
		TotalSlots:               1,
		ActorNames:               []string{},
		ActorActions:             map[string][]string{},
		ActorHibernateWebSockets: map[string]bool{},
		ActorDatabases:           map[string]bool{},
		LogLevel:                 "error",
	})
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	result, err := ffi.NewRunner(config)
	if err != nil {
		t.Fatalf("invoke rk_runner_new: %v", err)
	}
	if result.Runner != nil {
		result.Runner.Close()
		t.Fatalf("rk_runner_new against %s returned a runner, want error", endpoint)
	}
	if result.Error == nil {
		t.Fatal("rk_runner_new returned neither runner nor error")
	}
	defer result.Error.Close()
	payload, err := result.Error.Payload()
	if err != nil {
		t.Fatalf("decode rk_runner_new error: %v", err)
	}
	return payload
}

func startNativeRunner(t *testing.T, endpoint, name string) *ffi.Runner {
	return startNativeRunnerWithActors(t, endpoint, name, []string{})
}

func startNativeRunnerWithActors(t *testing.T, endpoint, name string, actorNames []string) *ffi.Runner {
	t.Helper()
	config, err := wire.EncodeRunnerConfig(wire.RunnerConfig{
		EngineEndpoint:           endpoint,
		Namespace:                "default",
		RunnerName:               name,
		Version:                  1,
		TotalSlots:               1,
		ActorNames:               actorNames,
		ActorActions:             map[string][]string{},
		ActorHibernateWebSockets: map[string]bool{},
		ActorDatabases:           map[string]bool{},
		LogLevel:                 "error",
	})
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	result, err := ffi.NewRunner(config)
	if err != nil {
		t.Fatalf("invoke rk_runner_new: %v", err)
	}
	if result.Error != nil {
		defer result.Error.Close()
		payload, decodeErr := result.Error.Payload()
		if decodeErr != nil {
			t.Fatalf("start native runner: decode error: %v", decodeErr)
		}
		t.Fatalf("start native runner: %v", payload)
	}
	if result.Runner == nil {
		t.Fatal("rk_runner_new returned neither runner nor error")
	}
	t.Cleanup(result.Runner.Close)
	return result.Runner
}

func submitNativeCommands(t *testing.T, runner *ffi.Runner, commands ...wire.Command) {
	t.Helper()
	batch, err := wire.EncodeCommandBatch(wire.CommandBatch{Commands: commands})
	if err != nil {
		t.Fatalf("encode native command batch: %v", err)
	}
	if err := runner.Submit(batch); err != nil {
		t.Fatalf("submit native command batch: %v", err)
	}
}

func waitForNativeKVResult(t *testing.T, runner *ffi.Runner, kvID uint64, timeout time.Duration) wire.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for KV result %d", kvID)
		}
		data, err := runner.Poll(min(remaining, time.Second))
		if err != nil {
			t.Fatalf("poll KV result %d: %v", kvID, err)
		}
		batch, err := wire.DecodeEventBatch(data)
		if err != nil {
			t.Fatalf("decode KV result %d: %v", kvID, err)
		}
		for _, event := range batch.Events {
			if event.Kind == wire.EventKVResult && event.KVID == kvID {
				return event
			}
		}
	}
}

// waitForNativeEvent polls until an event of the wanted kind arrives and
// returns it with its enclosing batch.
func waitForNativeEvent(t *testing.T, runner *ffi.Runner, kind wire.EventKind, timeout time.Duration) (wire.EventBatch, wire.Event) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("no %s event within %s", kind, timeout)
		}
		pollTimeout := remaining
		if pollTimeout > 500*time.Millisecond {
			pollTimeout = 500 * time.Millisecond
		}
		data, err := runner.Poll(pollTimeout)
		if err != nil {
			t.Fatalf("poll while waiting for %s: %v", kind, err)
		}
		if len(data) == 0 {
			continue
		}
		batch, err := wire.DecodeEventBatch(data)
		if err != nil {
			t.Fatalf("decode poll batch: %v", err)
		}
		for _, event := range batch.Events {
			if event.Kind == kind {
				return batch, event
			}
		}
	}
}

func nativeErrorCode(err error) string {
	var payload ffi.ErrorPayload
	if errors.As(err, &payload) {
		return payload.Code
	}
	return ""
}
