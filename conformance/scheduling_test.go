package conformance

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
	"github.com/gorilla/websocket"
)

const (
	alarmConformanceWindow = 45 * time.Second
	alarmClearGrace        = 5 * time.Second
)

type schedulingState struct {
	Value       string `json:"value"`
	AlarmCount  int    `json:"alarm_count"`
	FirstDueMS  int64  `json:"first_due_ms"`
	LatestDueMS int64  `json:"latest_due_ms"`
}

type schedulingInput struct {
	Value   string `json:"value"`
	DelayMS int64  `json:"delay_ms"`
}

type schedulingObservation struct {
	kind       string
	actorID    string
	generation uint64
	state      schedulingState
}

type schedulingObservations struct {
	mu     sync.Mutex
	events []schedulingObservation
}

func (o *schedulingObservations) add(observation schedulingObservation) {
	o.mu.Lock()
	o.events = append(o.events, observation)
	o.mu.Unlock()
}

func (o *schedulingObservations) take(
	t *testing.T,
	actorID, kind string,
	timeout time.Duration,
) schedulingObservation {
	t.Helper()
	var found schedulingObservation
	eventually(t, timeout, func() (bool, error) {
		o.mu.Lock()
		defer o.mu.Unlock()
		for index, event := range o.events {
			if event.actorID != actorID || event.kind != kind {
				continue
			}
			found = event
			o.events = append(o.events[:index], o.events[index+1:]...)
			return true, nil
		}
		return false, nil
	})
	return found
}

func (o *schedulingObservations) count(actorID, kind string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	count := 0
	for _, event := range o.events {
		if event.actorID == actorID && event.kind == kind {
			count++
		}
	}
	return count
}

func (o *schedulingObservations) order(actorID string, kinds ...string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	next := 0
	for _, event := range o.events {
		if event.actorID != actorID || next == len(kinds) {
			continue
		}
		if event.kind == kinds[next] {
			next++
		}
	}
	return next == len(kinds)
}

func TestSchedulingSleepAndMidflightPolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)
	observations := &schedulingObservations{}
	midflightStarted := make(chan struct{}, 1)
	midflightRelease := make(chan struct{})

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m5-scheduling", rivet.Actor[schedulingState]{
		OnStart: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "start", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(),
			})
			return nil
		},
		OnAlarm: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "alarm", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(),
			})
			ctx.State().AlarmCount++
			return nil
		},
		OnStop: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "stop", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(),
			})
			return nil
		},
		Actions: rivet.Actions[schedulingState]{
			"getAlarmCount": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (int, error) {
				return ctx.State().AlarmCount, nil
			}),
			"sleep": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (bool, error) {
				return true, ctx.Sleep()
			}),
			"scheduleSleep": rivet.Action(func(ctx *rivet.Context[schedulingState], input schedulingInput) (bool, error) {
				ctx.State().Value = input.Value
				ctx.State().LatestDueMS = time.Now().Add(time.Duration(input.DelayMS) * time.Millisecond).UnixMilli()
				if err := ctx.Schedule(time.UnixMilli(ctx.State().LatestDueMS)); err != nil {
					return false, err
				}
				return true, ctx.Sleep()
			}),
			"clearSleep": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (bool, error) {
				ctx.State().Value = "cleared"
				ctx.State().FirstDueMS = time.Now().Add(4 * time.Second).UnixMilli()
				if err := ctx.Schedule(time.UnixMilli(ctx.State().FirstDueMS)); err != nil {
					return false, err
				}
				if err := ctx.ClearSchedule(); err != nil {
					return false, err
				}
				return true, ctx.Sleep()
			}),
			"overwriteSleep": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (bool, error) {
				now := time.Now()
				ctx.State().Value = "latest"
				ctx.State().FirstDueMS = now.Add(3 * time.Second).UnixMilli()
				ctx.State().LatestDueMS = now.Add(10 * time.Second).UnixMilli()
				if err := ctx.Schedule(time.UnixMilli(ctx.State().FirstDueMS)); err != nil {
					return false, err
				}
				if err := ctx.Schedule(time.UnixMilli(ctx.State().LatestDueMS)); err != nil {
					return false, err
				}
				return true, ctx.Sleep()
			}),
			"midflightSleep": rivet.ActionWithContext(func(
				ctx context.Context,
				actor *rivet.Context[schedulingState],
				_ struct{},
			) (string, error) {
				actor.State().Value = "midflight-completed"
				if err := actor.Sleep(); err != nil {
					return "", err
				}
				midflightStarted <- struct{}{}
				select {
				case <-midflightRelease:
				case <-ctx.Done():
					return "", ctx.Err()
				}
				observations.add(schedulingObservation{
					kind: "action-complete", actorID: actor.ActorID(), generation: actor.Generation(), state: *actor.State(),
				})
				return actor.State().Value, nil
			}),
		},
	}); err != nil {
		t.Fatalf("register M5 scheduling actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m5-scheduling-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)

	alarmActor := createActor(t, engine.endpoint, "m5-scheduling", runnerName, "restart", nil, nil)
	firstStart := observations.take(t, alarmActor.ActorID, "start", 30*time.Second)
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, alarmActor.ActorID, "scheduleSleep", []any{
		schedulingInput{Value: "persisted-before-sleep", DelayMS: 5_000},
	}, websocketTestTimeout))
	observations.take(t, alarmActor.ActorID, "stop", 20*time.Second)
	waitForActor(t, engine.endpoint, alarmActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	alarm := observations.take(t, alarmActor.ActorID, "alarm", alarmConformanceWindow)
	if alarm.generation <= firstStart.generation || alarm.state.Value != "persisted-before-sleep" || alarm.state.AlarmCount != 0 {
		t.Fatalf("post-sleep alarm observation = %#v, first start = %#v", alarm, firstStart)
	}
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, alarmActor.ActorID, "getAlarmCount", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		1,
	)
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, alarmActor.ActorID, "sleep", []any{struct{}{}}, websocketTestTimeout))
	observations.take(t, alarmActor.ActorID, "stop", 20*time.Second)
	deleteActor(t, engine.endpoint, alarmActor.ActorID)

	clearedActor := createActor(t, engine.endpoint, "m5-scheduling", runnerName, "restart", nil, nil)
	observations.take(t, clearedActor.ActorID, "start", 30*time.Second)
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, clearedActor.ActorID, "clearSleep", []any{struct{}{}}, websocketTestTimeout))
	clearedStop := observations.take(t, clearedActor.ActorID, "stop", 20*time.Second)
	waitForActor(t, engine.endpoint, clearedActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	clearCheckAt := time.UnixMilli(clearedStop.state.FirstDueMS).Add(alarmClearGrace)
	eventually(t, alarmConformanceWindow, func() (bool, error) {
		if observations.count(clearedActor.ActorID, "alarm") != 0 {
			return false, fmt.Errorf("cleared alarm fired")
		}
		actor, err := getActor(engine.endpoint, clearedActor.ActorID, false)
		if err != nil {
			return false, err
		}
		return !time.Now().Before(clearCheckAt) && actor.ConnectableTS == nil && actor.SleepTS != nil, nil
	})
	deleteActor(t, engine.endpoint, clearedActor.ActorID)

	overwriteActor := createActor(t, engine.endpoint, "m5-scheduling", runnerName, "restart", nil, nil)
	overwriteStart := observations.take(t, overwriteActor.ActorID, "start", 30*time.Second)
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, overwriteActor.ActorID, "overwriteSleep", []any{struct{}{}}, websocketTestTimeout))
	overwriteStop := observations.take(t, overwriteActor.ActorID, "stop", 20*time.Second)
	waitForActor(t, engine.endpoint, overwriteActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	firstAlarmGrace := time.UnixMilli(overwriteStop.state.FirstDueMS).Add(3 * time.Second)
	eventually(t, alarmConformanceWindow, func() (bool, error) {
		if observations.count(overwriteActor.ActorID, "alarm") != 0 {
			return false, fmt.Errorf("superseded alarm fired")
		}
		actor, err := getActor(engine.endpoint, overwriteActor.ActorID, false)
		if err != nil {
			return false, err
		}
		return !time.Now().Before(firstAlarmGrace) && actor.ConnectableTS == nil && actor.SleepTS != nil, nil
	})
	latestAlarm := observations.take(t, overwriteActor.ActorID, "alarm", alarmConformanceWindow)
	if latestAlarm.generation <= overwriteStart.generation || latestAlarm.state.Value != "latest" {
		t.Fatalf("latest alarm observation = %#v, first start = %#v", latestAlarm, overwriteStart)
	}
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, overwriteActor.ActorID, "getAlarmCount", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		1,
	)
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, overwriteActor.ActorID, "sleep", []any{struct{}{}}, websocketTestTimeout))
	observations.take(t, overwriteActor.ActorID, "stop", 20*time.Second)
	deleteActor(t, engine.endpoint, overwriteActor.ActorID)

	midflightActor := createActor(t, engine.endpoint, "m5-scheduling", runnerName, "restart", nil, nil)
	observations.take(t, midflightActor.ActorID, "start", 30*time.Second)
	actionResult := make(chan gatewayResponse, 1)
	go func() {
		actionResult <- gatewayAction(t, engine.endpoint, midflightActor.ActorID, "midflightSleep", []any{struct{}{}}, 30*time.Second)
	}()
	select {
	case <-midflightStarted:
	case <-time.After(websocketTestTimeout):
		t.Fatal("mid-flight action did not enter its handler")
	}
	if observations.count(midflightActor.ActorID, "stop") != 0 {
		t.Fatal("sleep stop overtook the active action")
	}
	close(midflightRelease)
	assertSuccessfulAction(t, <-actionResult)
	eventually(t, 20*time.Second, func() (bool, error) {
		return observations.order(midflightActor.ActorID, "action-complete", "stop"), nil
	})
	observations.take(t, midflightActor.ActorID, "stop", time.Second)
	waitForActor(t, engine.endpoint, midflightActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	deleteActor(t, engine.endpoint, midflightActor.ActorID)
}

func TestAlarmSurvivesEngineRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)
	observations := &schedulingObservations{}
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m5-restart-alarm", rivet.Actor[schedulingState]{
		OnStart: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "start", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(),
			})
			return nil
		},
		OnStop: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "stop", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(),
			})
			return nil
		},
		OnAlarm: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "alarm", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(),
			})
			ctx.State().AlarmCount++
			return nil
		},
		Actions: rivet.Actions[schedulingState]{
			"getAlarmCount": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (int, error) {
				return ctx.State().AlarmCount, nil
			}),
			"sleep": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (bool, error) {
				return true, ctx.Sleep()
			}),
			"schedule": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (bool, error) {
				ctx.State().Value = "durable-across-engine-restart"
				if err := ctx.ScheduleAfter(35 * time.Second); err != nil {
					return false, err
				}
				return true, ctx.Sleep()
			}),
		},
	}); err != nil {
		t.Fatalf("register restart alarm actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m5-alarm-restart-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "m5-restart-alarm", runnerName, "restart", nil, nil)
	firstStart := observations.take(t, actor.ActorID, "start", 30*time.Second)
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, actor.ActorID, "schedule", []any{struct{}{}}, websocketTestTimeout))
	observations.take(t, actor.ActorID, "stop", 20*time.Second)
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})

	engine.restart(t)
	eventually(t, disconnectLivenessWindow+20*time.Second, func() (bool, error) {
		envoys, err := listEnvoys(engine.endpoint, runnerName)
		if err != nil {
			return false, err
		}
		return len(envoys) == 1, nil
	})
	alarm := observations.take(t, actor.ActorID, "alarm", alarmConformanceWindow)
	if alarm.generation <= firstStart.generation || alarm.state.Value != "durable-across-engine-restart" {
		t.Fatalf("post-restart alarm = %#v, first start = %#v", alarm, firstStart)
	}
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, actor.ActorID, "getAlarmCount", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		1,
	)
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, actor.ActorID, "sleep", []any{struct{}{}}, websocketTestTimeout))
	observations.take(t, actor.ActorID, "stop", 20*time.Second)
	deleteActor(t, engine.endpoint, actor.ActorID)
}

func TestHibernatingWebSocketSurvivesSleep(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type websocketSleepState struct {
		Count int `json:"count"`
	}
	type lifecycleObservation struct {
		generation uint64
		count      int
	}
	started := make(chan lifecycleObservation, 8)
	stopped := make(chan lifecycleObservation, 8)
	alarmed := make(chan lifecycleObservation, 4)
	disconnected := make(chan struct{}, 8)
	handled := make(chan string, 16)
	handlerErrors := make(chan error, 16)
	var connectCount atomic.Int32
	var connects sync.Once
	connectObserved := make(chan struct{})

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m5-hibernating-websocket", rivet.Actor[websocketSleepState]{
		OnStart: func(ctx *rivet.Context[websocketSleepState]) error {
			started <- lifecycleObservation{generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
		OnStop: func(ctx *rivet.Context[websocketSleepState]) error {
			stopped <- lifecycleObservation{generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
		OnAlarm: func(ctx *rivet.Context[websocketSleepState]) error {
			alarmed <- lifecycleObservation{generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
		OnConnect: func(_ *rivet.Context[websocketSleepState], connection *rivet.Connection) error {
			if !connection.CanHibernate() {
				return fmt.Errorf("gateway connection is not hibernatable")
			}
			connectCount.Add(1)
			connects.Do(func() { close(connectObserved) })
			return nil
		},
		OnDisconnect: func(*rivet.Context[websocketSleepState], *rivet.Connection) {
			disconnected <- struct{}{}
		},
		OnMessage: func(ctx *rivet.Context[websocketSleepState], connection *rivet.Connection, message rivet.Message) {
			text := string(message.Data)
			handled <- text
			switch text {
			case "sleep":
				ctx.State().Count = 41
				if err := ctx.ScheduleAfter(8 * time.Second); err != nil {
					handlerErrors <- fmt.Errorf("schedule hibernation wake: %w", err)
					return
				}
				if err := ctx.Save(context.Background()); err != nil {
					handlerErrors <- fmt.Errorf("save before sleep: %w", err)
					return
				}
				if err := connection.SendText("sleep-accepted"); err != nil {
					handlerErrors <- fmt.Errorf("send before sleep: %w", err)
					return
				}
				if err := ctx.Sleep(); err != nil {
					handlerErrors <- fmt.Errorf("sleep actor: %w", err)
				}
			case "wake":
				if ctx.State().Count != 41 {
					handlerErrors <- fmt.Errorf("rehydrated state count = %d, want 41", ctx.State().Count)
					return
				}
				ctx.State().Count++
				if err := ctx.Save(context.Background()); err != nil {
					handlerErrors <- fmt.Errorf("save after wake: %w", err)
					return
				}
				if err := connection.SendText("wake:42"); err != nil {
					handlerErrors <- fmt.Errorf("send after wake: %w", err)
					return
				}
				if err := ctx.Broadcast("afterWake", "server-to-same-client"); err != nil {
					handlerErrors <- fmt.Errorf("broadcast after wake: %w", err)
				}
			case "after":
				if err := connection.SendText(fmt.Sprintf("after:%d", ctx.State().Count)); err != nil {
					handlerErrors <- fmt.Errorf("send follow-up: %w", err)
				}
			default:
				handlerErrors <- fmt.Errorf("unexpected WebSocket message %q", text)
			}
		},
		Actions: rivet.Actions[websocketSleepState]{
			"get": rivet.Action(func(ctx *rivet.Context[websocketSleepState], _ struct{}) (int, error) {
				return ctx.State().Count, nil
			}),
			"sleep": rivet.Action(func(ctx *rivet.Context[websocketSleepState], _ struct{}) (bool, error) {
				return true, ctx.Sleep()
			}),
		},
	}); err != nil {
		t.Fatalf("register hibernating WebSocket actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m5-hibernating-ws-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "m5-hibernating-websocket", runnerName, "restart", nil, nil)
	var first lifecycleObservation
	select {
	case first = <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("hibernating WebSocket actor did not start")
	}
	client := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "m5-hibernate", true)
	select {
	case <-connectObserved:
	case <-time.After(websocketTestTimeout):
		t.Fatal("OnConnect did not observe the hibernatable connection")
	}
	client.write(t, websocket.TextMessage, []byte("sleep"))
	waitTextFrame(t, client, "sleep-accepted")
	select {
	case observation := <-stopped:
		if observation.generation != first.generation || observation.count != 41 {
			t.Fatalf("sleep stop observation = %#v, first start = %#v", observation, first)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("actor did not stop for WebSocket hibernation")
	}
	sleepingActor := waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	select {
	case <-disconnected:
		t.Fatal("OnDisconnect ran for hibernation")
	default:
	}
	settledAt := time.UnixMilli(*sleepingActor.SleepTS).Add(2 * time.Second)
	eventually(t, 10*time.Second, func() (bool, error) {
		actor, err := getActor(engine.endpoint, actor.ActorID, false)
		if err != nil {
			return false, err
		}
		return !time.Now().Before(settledAt) && actor.ConnectableTS == nil && actor.SleepTS != nil, nil
	})

	client.write(t, websocket.TextMessage, []byte("wake"))
	var rehydrated lifecycleObservation
	select {
	case rehydrated = <-started:
	case <-time.After(alarmConformanceWindow):
		t.Fatal("hibernated WebSocket message did not rehydrate the actor")
	}
	if rehydrated.generation <= first.generation || rehydrated.count != 41 {
		t.Fatalf("rehydrated WebSocket actor = %#v, first = %#v", rehydrated, first)
	}
	select {
	case alarm := <-alarmed:
		if alarm.generation != rehydrated.generation || (alarm.count != 41 && alarm.count != 42) {
			t.Fatalf("hibernation wake alarm = %#v, rehydrated = %#v", alarm, rehydrated)
		}
	case <-time.After(alarmConformanceWindow):
		t.Fatal("scheduled alarm did not wake the hibernating WebSocket actor")
	}
	waitTextFrame(t, client, "wake:42")
	waitActorEvent(t, client, "afterWake", "server-to-same-client")
	if connectCount.Load() != 1 {
		t.Fatalf("OnConnect calls = %d, want 1 across hibernation", connectCount.Load())
	}
	client.write(t, websocket.TextMessage, []byte("after"))
	waitTextFrame(t, client, "after:42")
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, actor.ActorID, "get", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		42,
	)
	for _, want := range []string{"sleep", "wake", "after"} {
		select {
		case got := <-handled:
			if got != want {
				t.Fatalf("WebSocket handler order = %q, want %q", got, want)
			}
		default:
			t.Fatalf("WebSocket handler did not record %q", want)
		}
	}
	assertNoHandlerError(t, handlerErrors)
	client.closeWithCode(t, websocket.CloseNormalClosure, "real disconnect")
	select {
	case <-disconnected:
	case <-time.After(websocketTestTimeout):
		t.Fatal("OnDisconnect did not run for a real awake connection close")
	}
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, actor.ActorID, "sleep", []any{struct{}{}}, websocketTestTimeout))
	select {
	case <-stopped:
	case <-time.After(20 * time.Second):
		t.Fatal("actor did not sleep before cleanup")
	}
	deleteActor(t, engine.endpoint, actor.ActorID)
}
