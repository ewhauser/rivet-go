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
	alarmConformanceWindow = 90 * time.Second
	pinnedWorkflowPollTick = 16 * time.Second
	alarmDeliveryMargin    = 5 * time.Second
	alarmNegativeWindow    = pinnedWorkflowPollTick + alarmDeliveryMargin
	alarmSleepDelay        = 20 * time.Second
	alarmOverwriteFirst    = 12 * time.Second
	alarmOverwriteLatest   = 45 * time.Second
	alarmRestartDelay      = 60 * time.Second
	alarmHibernationDelay  = 35 * time.Second
	alarmTransitionMargin  = 10 * time.Second
	hibernationWakeBound   = 20 * time.Second
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
	observedAt time.Time
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

func assertAlarmTransitionMargin(t *testing.T, actorID string, dueMS int64, margin time.Duration) {
	t.Helper()
	remaining := time.Until(time.UnixMilli(dueMS))
	if remaining < margin {
		t.Fatalf(
			"actor %s alarm has %s remaining after its engine transition, want at least %s",
			actorID,
			remaining.Round(time.Millisecond),
			margin,
		)
	}
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
	httpMidflightStarted := make(chan struct{}, 1)
	httpMidflightRelease := make(chan struct{})

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m5-scheduling", rivet.Actor[schedulingState]{
		OnStart: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "start", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(), observedAt: time.Now(),
			})
			if ctx.State().Value == "schedule-on-rehydrated-start" && ctx.State().LatestDueMS == 0 {
				ctx.State().LatestDueMS = time.Now().Add(alarmSleepDelay).UnixMilli()
				if err := ctx.Schedule(time.UnixMilli(ctx.State().LatestDueMS)); err != nil {
					return err
				}
				if err := ctx.Save(context.Background()); err != nil {
					return err
				}
			}
			return nil
		},
		OnAlarm: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "alarm", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(), observedAt: time.Now(),
			})
			ctx.State().AlarmCount++
			if ctx.State().Value == "sleep-from-alarm" {
				return ctx.Sleep()
			}
			return nil
		},
		OnStop: func(ctx *rivet.Context[schedulingState]) error {
			observations.add(schedulingObservation{
				kind: "stop", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(), observedAt: time.Now(),
			})
			return nil
		},
		OnFetch: func(ctx *rivet.Context[schedulingState], response http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/midflight-sleep" {
				http.NotFound(response, request)
				return
			}
			ctx.State().Value = "http-midflight-completed"
			if err := ctx.Sleep(); err != nil {
				http.Error(response, err.Error(), http.StatusInternalServerError)
				return
			}
			httpMidflightStarted <- struct{}{}
			<-httpMidflightRelease
			observations.add(schedulingObservation{
				kind: "http-complete", actorID: ctx.ActorID(), generation: ctx.Generation(), state: *ctx.State(), observedAt: time.Now(),
			})
			response.WriteHeader(http.StatusAccepted)
			_, _ = response.Write([]byte("http-completed-before-sleep"))
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
				ctx.State().FirstDueMS = time.Now().Add(12 * time.Second).UnixMilli()
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
				ctx.State().FirstDueMS = now.Add(alarmOverwriteFirst).UnixMilli()
				ctx.State().LatestDueMS = now.Add(alarmOverwriteLatest).UnixMilli()
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
			"scheduleAwakeThenSleepFromAlarm": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (bool, error) {
				ctx.State().Value = "sleep-from-alarm"
				ctx.State().LatestDueMS = time.Now().Add(alarmSleepDelay).UnixMilli()
				return true, ctx.Schedule(time.UnixMilli(ctx.State().LatestDueMS))
			}),
			"prepareOnStartSchedule": rivet.Action(func(ctx *rivet.Context[schedulingState], _ struct{}) (bool, error) {
				ctx.State().Value = "schedule-on-rehydrated-start"
				ctx.State().LatestDueMS = 0
				return true, ctx.Sleep()
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
		schedulingInput{Value: "persisted-before-sleep", DelayMS: alarmSleepDelay.Milliseconds()},
	}, websocketTestTimeout))
	alarmStop := observations.take(t, alarmActor.ActorID, "stop", 20*time.Second)
	waitForActor(t, engine.endpoint, alarmActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	assertAlarmTransitionMargin(t, alarmActor.ActorID, alarmStop.state.LatestDueMS, alarmTransitionMargin)
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
	clearCheckAt := time.UnixMilli(clearedStop.state.FirstDueMS).Add(alarmNegativeWindow)
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
	assertAlarmTransitionMargin(t, overwriteActor.ActorID, overwriteStop.state.LatestDueMS, alarmTransitionMargin)
	firstAlarmGrace := time.UnixMilli(overwriteStop.state.FirstDueMS).Add(alarmNegativeWindow)
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
	if latestAlarm.observedAt.Before(time.UnixMilli(overwriteStop.state.LatestDueMS)) {
		t.Fatalf("latest alarm observed at %s before its requested timestamp %s", latestAlarm.observedAt, time.UnixMilli(overwriteStop.state.LatestDueMS))
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

	httpActor := createActor(t, engine.endpoint, "m5-scheduling", runnerName, "restart", nil, nil)
	observations.take(t, httpActor.ActorID, "start", 30*time.Second)
	type httpResult struct {
		response *http.Response
		body     []byte
		err      error
	}
	httpDone := make(chan httpResult, 1)
	go func() {
		response, body, requestErr := gatewayRequest(
			engine.endpoint, httpActor.ActorID, "/request/midflight-sleep", nil, 30*time.Second,
		)
		httpDone <- httpResult{response: response, body: body, err: requestErr}
	}()
	select {
	case <-httpMidflightStarted:
	case <-time.After(websocketTestTimeout):
		t.Fatal("mid-flight HTTP handler did not submit its sleep intent")
	}
	select {
	case result := <-httpDone:
		t.Fatalf("HTTP response completed before handler release: status=%s body=%q err=%v", responseStatus(result.response), result.body, result.err)
	default:
	}
	if observations.count(httpActor.ActorID, "stop") != 0 {
		t.Fatal("sleep stop overtook the active HTTP handler")
	}
	close(httpMidflightRelease)
	httpResponse := <-httpDone
	if httpResponse.err != nil || httpResponse.response.StatusCode != http.StatusAccepted ||
		string(httpResponse.body) != "http-completed-before-sleep" {
		t.Fatalf("mid-flight HTTP response: status=%s body=%q err=%v", responseStatus(httpResponse.response), httpResponse.body, httpResponse.err)
	}
	eventually(t, 20*time.Second, func() (bool, error) {
		return observations.order(httpActor.ActorID, "http-complete", "stop"), nil
	})
	observations.take(t, httpActor.ActorID, "stop", time.Second)
	waitForActor(t, engine.endpoint, httpActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	deleteActor(t, engine.endpoint, httpActor.ActorID)

	alarmSleepActor := createActor(t, engine.endpoint, "m5-scheduling", runnerName, "restart", nil, nil)
	alarmSleepStart := observations.take(t, alarmSleepActor.ActorID, "start", 30*time.Second)
	assertSuccessfulAction(t, gatewayAction(
		t, engine.endpoint, alarmSleepActor.ActorID, "scheduleAwakeThenSleepFromAlarm",
		[]any{struct{}{}}, websocketTestTimeout,
	))
	alarmWhileAwake := observations.take(t, alarmSleepActor.ActorID, "alarm", alarmConformanceWindow)
	if alarmWhileAwake.generation != alarmSleepStart.generation || alarmWhileAwake.state.AlarmCount != 0 {
		t.Fatalf("awake alarm = %#v, initial start = %#v", alarmWhileAwake, alarmSleepStart)
	}
	alarmSleepStop := observations.take(t, alarmSleepActor.ActorID, "stop", 20*time.Second)
	if alarmSleepStop.generation != alarmSleepStart.generation || alarmSleepStop.state.AlarmCount != 1 {
		t.Fatalf("sleep-from-alarm stop = %#v, initial start = %#v", alarmSleepStop, alarmSleepStart)
	}
	waitForActor(t, engine.endpoint, alarmSleepActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	oneShotGuardAt := alarmWhileAwake.observedAt.Add(alarmNegativeWindow)
	eventually(t, alarmNegativeWindow+alarmDeliveryMargin, func() (bool, error) {
		if observations.count(alarmSleepActor.ActorID, "alarm") != 0 {
			return false, fmt.Errorf("one-shot awake alarm fired more than once")
		}
		actor, err := getActor(engine.endpoint, alarmSleepActor.ActorID, false)
		if err != nil {
			return false, err
		}
		return !time.Now().Before(oneShotGuardAt) && actor.ConnectableTS == nil && actor.SleepTS != nil, nil
	})
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, alarmSleepActor.ActorID, "getAlarmCount", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		1,
	)
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, alarmSleepActor.ActorID, "sleep", []any{struct{}{}}, websocketTestTimeout))
	observations.take(t, alarmSleepActor.ActorID, "stop", 20*time.Second)
	deleteActor(t, engine.endpoint, alarmSleepActor.ActorID)

	onStartActor := createActor(t, engine.endpoint, "m5-scheduling", runnerName, "restart", nil, nil)
	firstOnStart := observations.take(t, onStartActor.ActorID, "start", 30*time.Second)
	assertSuccessfulAction(t, gatewayAction(
		t, engine.endpoint, onStartActor.ActorID, "prepareOnStartSchedule", []any{struct{}{}}, websocketTestTimeout,
	))
	observations.take(t, onStartActor.ActorID, "stop", 20*time.Second)
	waitForActor(t, engine.endpoint, onStartActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, onStartActor.ActorID, "sleep", []any{struct{}{}}, websocketTestTimeout))
	rehydratedStart := observations.take(t, onStartActor.ActorID, "start", 30*time.Second)
	if rehydratedStart.generation <= firstOnStart.generation || rehydratedStart.state.LatestDueMS != 0 {
		t.Fatalf("rehydrated OnStart entry = %#v, initial start = %#v", rehydratedStart, firstOnStart)
	}
	onStartStop := observations.take(t, onStartActor.ActorID, "stop", 20*time.Second)
	waitForActor(t, engine.endpoint, onStartActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	assertAlarmTransitionMargin(t, onStartActor.ActorID, onStartStop.state.LatestDueMS, alarmTransitionMargin)
	onStartAlarm := observations.take(t, onStartActor.ActorID, "alarm", alarmConformanceWindow)
	if onStartAlarm.generation <= rehydratedStart.generation || onStartAlarm.state.AlarmCount != 0 ||
		onStartAlarm.state.LatestDueMS != onStartStop.state.LatestDueMS {
		t.Fatalf("OnStart-scheduled alarm = %#v, rehydrated start = %#v, stop = %#v", onStartAlarm, rehydratedStart, onStartStop)
	}
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, onStartActor.ActorID, "sleep", []any{struct{}{}}, websocketTestTimeout))
	observations.take(t, onStartActor.ActorID, "stop", 20*time.Second)
	deleteActor(t, engine.endpoint, onStartActor.ActorID)
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
				ctx.State().LatestDueMS = time.Now().Add(alarmRestartDelay).UnixMilli()
				if err := ctx.ScheduleAfter(alarmRestartDelay); err != nil {
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
	initialStop := observations.take(t, actor.ActorID, "stop", 20*time.Second)
	sleepingActor := waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	settledAt := time.UnixMilli(*sleepingActor.SleepTS).Add(3 * time.Second)
	eventually(t, 10*time.Second, func() (bool, error) {
		record, err := getActor(engine.endpoint, actor.ActorID, false)
		if err != nil {
			return false, err
		}
		return !time.Now().Before(settledAt) && record.ConnectableTS == nil && record.SleepTS != nil, nil
	})

	restartStarted := time.Now().UnixMilli()
	engine.restart(t)
	eventually(t, disconnectLivenessWindow+20*time.Second, func() (bool, error) {
		envoys, err := listEnvoys(engine.endpoint, runnerName)
		if err != nil {
			return false, err
		}
		return len(envoys) == 1 && envoys[0].StopTS == nil && envoys[0].LastPingTS >= restartStarted, nil
	})
	gatewayReadyAt := time.UnixMilli(restartStarted).Add(disconnectLivenessWindow + 5*time.Second)
	eventually(t, disconnectLivenessWindow+10*time.Second, func() (bool, error) {
		return !time.Now().Before(gatewayReadyAt), nil
	})
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, actor.ActorID, "getAlarmCount", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		0,
	)
	restartStart := observations.take(t, actor.ActorID, "start", 30*time.Second)
	if restartStart.generation <= firstStart.generation || restartStart.state.Value != "durable-across-engine-restart" {
		t.Fatalf("post-restart rehydration = %#v, first start = %#v", restartStart, firstStart)
	}
	assertSuccessfulAction(t, gatewayAction(t, engine.endpoint, actor.ActorID, "sleep", []any{struct{}{}}, websocketTestTimeout))
	postRestartStop := observations.take(t, actor.ActorID, "stop", 20*time.Second)
	if postRestartStop.state.LatestDueMS != initialStop.state.LatestDueMS {
		t.Fatalf("post-restart alarm timestamp = %d, original = %d", postRestartStop.state.LatestDueMS, initialStop.state.LatestDueMS)
	}
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	assertAlarmTransitionMargin(t, actor.ActorID, postRestartStop.state.LatestDueMS, alarmTransitionMargin)
	alarm := observations.take(t, actor.ActorID, "alarm", alarmConformanceWindow)
	if alarm.generation <= restartStart.generation || alarm.state.Value != "durable-across-engine-restart" ||
		alarm.state.LatestDueMS != initialStop.state.LatestDueMS {
		t.Fatalf("post-restart alarm = %#v, restart start = %#v", alarm, restartStart)
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
		Count     int   `json:"count"`
		WakeDueMS int64 `json:"wake_due_ms"`
	}
	type lifecycleObservation struct {
		generation uint64
		count      int
		wakeDueMS  int64
		observedAt time.Time
	}
	type messageObservation struct {
		text       string
		generation uint64
		count      int
	}
	started := make(chan lifecycleObservation, 8)
	stopped := make(chan lifecycleObservation, 8)
	alarmed := make(chan lifecycleObservation, 4)
	disconnected := make(chan struct{}, 8)
	handled := make(chan messageObservation, 16)
	handlerErrors := make(chan error, 16)
	sleepHandlerEntered := make(chan struct{})
	sleepHandlerRelease := make(chan struct{})
	var connectCount atomic.Int32
	var connects sync.Once
	connectObserved := make(chan struct{})

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m5-hibernating-websocket", rivet.Actor[websocketSleepState]{
		OnStart: func(ctx *rivet.Context[websocketSleepState]) error {
			started <- lifecycleObservation{
				generation: ctx.Generation(), count: ctx.State().Count,
				wakeDueMS: ctx.State().WakeDueMS, observedAt: time.Now(),
			}
			return nil
		},
		OnStop: func(ctx *rivet.Context[websocketSleepState]) error {
			stopped <- lifecycleObservation{
				generation: ctx.Generation(), count: ctx.State().Count,
				wakeDueMS: ctx.State().WakeDueMS, observedAt: time.Now(),
			}
			return nil
		},
		OnAlarm: func(ctx *rivet.Context[websocketSleepState]) error {
			alarmed <- lifecycleObservation{
				generation: ctx.Generation(), count: ctx.State().Count,
				wakeDueMS: ctx.State().WakeDueMS, observedAt: time.Now(),
			}
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
			handled <- messageObservation{text: text, generation: ctx.Generation(), count: ctx.State().Count}
			switch text {
			case "sleep":
				ctx.State().Count = 41
				ctx.State().WakeDueMS = time.Now().Add(alarmHibernationDelay).UnixMilli()
				if err := ctx.Schedule(time.UnixMilli(ctx.State().WakeDueMS)); err != nil {
					handlerErrors <- fmt.Errorf("schedule hibernation wake: %w", err)
					return
				}
				if err := ctx.Save(context.Background()); err != nil {
					handlerErrors <- fmt.Errorf("save before sleep: %w", err)
					return
				}
				// This mutation is deliberately not saved. The stopped generation
				// observes it, while the rehydrated generation must reload 41.
				ctx.State().Count = 99
				if err := ctx.Sleep(); err != nil {
					handlerErrors <- fmt.Errorf("sleep actor: %w", err)
					return
				}
				close(sleepHandlerEntered)
				<-sleepHandlerRelease
				if err := connection.SendText("sleep-accepted"); err != nil {
					handlerErrors <- fmt.Errorf("send before sleep: %w", err)
					return
				}
			case "during-window-1", "during-window-2":
				if ctx.State().Count != 99 {
					handlerErrors <- fmt.Errorf("pre-eviction state count = %d, want 99", ctx.State().Count)
					return
				}
				if err := connection.SendText("handled-old:" + text); err != nil {
					handlerErrors <- fmt.Errorf("send pre-eviction acknowledgement: %w", err)
				}
			case "during-sleep-1", "during-sleep-2":
				if ctx.State().Count != 41 {
					handlerErrors <- fmt.Errorf("rehydrated state count = %d, want 41", ctx.State().Count)
					return
				}
				if err := connection.SendText("handled-new:" + text); err != nil {
					handlerErrors <- fmt.Errorf("send replay acknowledgement: %w", err)
				}
			case "after":
				ctx.State().Count++
				if err := ctx.Save(context.Background()); err != nil {
					handlerErrors <- fmt.Errorf("save after wake: %w", err)
					return
				}
				if err := connection.SendText(fmt.Sprintf("after:%d", ctx.State().Count)); err != nil {
					handlerErrors <- fmt.Errorf("send follow-up: %w", err)
				}
				if err := ctx.Broadcast("afterWake", "server-to-same-client"); err != nil {
					handlerErrors <- fmt.Errorf("broadcast after wake: %w", err)
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
	select {
	case <-sleepHandlerEntered:
	case <-time.After(websocketTestTimeout):
		t.Fatal("WebSocket sleep handler did not submit its sleep intent")
	}
	client.write(t, websocket.TextMessage, []byte("during-window-1"))
	client.write(t, websocket.TextMessage, []byte("during-window-2"))
	select {
	case observation := <-stopped:
		t.Fatalf("sleep overtook its active WebSocket handler: %#v", observation)
	default:
	}
	assertNoWebSocketFrame(t, client, 250*time.Millisecond)
	close(sleepHandlerRelease)
	waitTextFrame(t, client, "sleep-accepted")
	for index, want := range []messageObservation{
		{text: "sleep", generation: first.generation, count: 0},
		{text: "during-window-1", generation: first.generation, count: 99},
		{text: "during-window-2", generation: first.generation, count: 99},
	} {
		select {
		case got := <-handled:
			if got != want {
				t.Fatalf("pre-eviction WebSocket handler %d = %#v, want %#v", index, got, want)
			}
		case <-time.After(websocketTestTimeout):
			t.Fatalf("pre-eviction WebSocket handler %d did not run", index)
		}
	}
	waitTextFrame(t, client, "handled-old:during-window-1")
	waitTextFrame(t, client, "handled-old:during-window-2")
	var sleepStop lifecycleObservation
	select {
	case sleepStop = <-stopped:
		if sleepStop.generation != first.generation || sleepStop.count != 99 {
			t.Fatalf("sleep stop observation = %#v, first start = %#v", sleepStop, first)
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

	client.write(t, websocket.TextMessage, []byte("during-sleep-1"))
	client.write(t, websocket.TextMessage, []byte("during-sleep-2"))
	var rehydrated lifecycleObservation
	select {
	case rehydrated = <-started:
	case <-time.After(hibernationWakeBound):
		t.Fatal("sleep-time WebSocket message did not rehydrate the actor")
	}
	if rehydrated.generation <= first.generation || rehydrated.count != 41 {
		t.Fatalf("rehydrated WebSocket actor = %#v, first = %#v", rehydrated, first)
	}
	if !rehydrated.observedAt.Before(time.UnixMilli(rehydrated.wakeDueMS)) {
		t.Fatalf("message-driven rehydration at %s did not precede alarm %s", rehydrated.observedAt, time.UnixMilli(rehydrated.wakeDueMS))
	}
	for _, message := range []string{"during-sleep-1", "during-sleep-2"} {
		waitTextFrame(t, client, "handled-new:"+message)
		select {
		case got := <-handled:
			want := messageObservation{text: message, generation: rehydrated.generation, count: 41}
			if got != want {
				t.Fatalf("post-eviction WebSocket handler = %#v, want %#v", got, want)
			}
		case <-time.After(websocketTestTimeout):
			t.Fatalf("post-eviction WebSocket handler did not record %q", message)
		}
	}
	select {
	case alarm := <-alarmed:
		if alarm.generation != rehydrated.generation || alarm.count != 41 {
			t.Fatalf("hibernation wake alarm = %#v, rehydrated = %#v", alarm, rehydrated)
		}
	case <-time.After(alarmConformanceWindow):
		t.Fatal("scheduled alarm did not wake the hibernating WebSocket actor")
	}
	if connectCount.Load() != 1 {
		t.Fatalf("OnConnect calls = %d, want 1 across hibernation", connectCount.Load())
	}
	client.write(t, websocket.TextMessage, []byte("after"))
	waitTextFrame(t, client, "after:42")
	waitActorEvent(t, client, "afterWake", "server-to-same-client")
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, actor.ActorID, "get", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		42,
	)
	select {
	case got := <-handled:
		want := messageObservation{text: "after", generation: rehydrated.generation, count: 41}
		if got != want {
			t.Fatalf("post-wake WebSocket handler = %#v, want %#v", got, want)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("post-wake WebSocket handler did not record after")
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
