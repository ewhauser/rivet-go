package conformance

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

type durableSchedulePayload struct {
	Label string `json:"label"`
}

type durableScheduleState struct {
	Fired []string `json:"fired"`
}

type durableScheduleRecord struct {
	ID     string `json:"id"`
	Action string `json:"action"`
	Label  string `json:"label"`
	RunAt  int64  `json:"runAt"`
}

type durableScheduleExecution struct {
	Label      string
	Generation uint64
	Failed     bool
}

func TestDurableActionSchedulesAcrossSleepAndRunnerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)
	executions := make(chan durableScheduleExecution, 8)
	starts := make(chan uint64, 8)

	newRegistry := func() *rivet.Registry {
		registry := rivet.NewRegistry()
		err := rivet.Register(registry, "action-schedules", rivet.Actor[durableScheduleState]{
			OnStart: func(actor *rivet.Context[durableScheduleState]) error {
				starts <- actor.Generation()
				return nil
			},
			Actions: rivet.Actions[durableScheduleState]{
				"scheduleBatch": rivet.ActionWithContext(func(
					ctx context.Context,
					actor *rivet.Context[durableScheduleState],
					_ struct{},
				) ([]durableScheduleRecord, error) {
					base := time.Now()
					requests := []struct {
						label  string
						action string
						delay  time.Duration
						at     bool
					}{
						{label: "first", action: "recordScheduled", delay: 60 * time.Second},
						{label: "cancelled", action: "recordScheduled", delay: 70 * time.Second, at: true},
						{label: "fails", action: "failScheduled", delay: 80 * time.Second},
						{label: "last", action: "recordScheduled", delay: 90 * time.Second, at: true},
					}
					records := make([]durableScheduleRecord, 0, len(requests))
					for _, request := range requests {
						runAt := base.Add(request.delay)
						scheduleContext, cancelScheduleContext := context.WithTimeout(ctx, 3*time.Second)
						var id string
						var scheduleErr error
						if request.at {
							id, scheduleErr = actor.Schedules().At(
								scheduleContext, runAt, request.action, durableSchedulePayload{Label: request.label},
							)
						} else {
							id, scheduleErr = actor.Schedules().After(
								scheduleContext, request.delay, request.action, durableSchedulePayload{Label: request.label},
							)
						}
						cancelScheduleContext()
						if scheduleErr != nil {
							return nil, scheduleErr
						}
						records = append(records, durableScheduleRecord{
							ID: id, Action: request.action, Label: request.label, RunAt: runAt.UnixMilli(),
						})
					}
					return records, nil
				}),
				"cancelSchedule": rivet.ActionWithContext(func(
					ctx context.Context,
					actor *rivet.Context[durableScheduleState],
					id string,
				) (bool, error) {
					cancelContext, cancel := context.WithTimeout(ctx, 3*time.Second)
					defer cancel()
					return actor.Schedules().Cancel(cancelContext, id)
				}),
				"inspectSchedules": rivet.ActionWithContext(func(
					ctx context.Context,
					actor *rivet.Context[durableScheduleState],
					_ struct{},
				) ([]durableScheduleRecord, error) {
					schedules, err := actor.Schedules().List(ctx)
					if err != nil {
						return nil, err
					}
					records := make([]durableScheduleRecord, len(schedules))
					for index, schedule := range schedules {
						var payload durableSchedulePayload
						if err := schedule.DecodeArgument(&payload); err != nil {
							return nil, err
						}
						records[index] = durableScheduleRecord{
							ID: schedule.ID, Action: schedule.Action,
							Label: payload.Label, RunAt: schedule.RunAt.UnixMilli(),
						}
					}
					return records, nil
				}),
				"getSchedule": rivet.ActionWithContext(func(
					ctx context.Context,
					actor *rivet.Context[durableScheduleState],
					id string,
				) (durableScheduleRecord, error) {
					schedule, err := actor.Schedules().Get(ctx, id)
					if err != nil || schedule == nil {
						return durableScheduleRecord{}, err
					}
					var payload durableSchedulePayload
					if err := schedule.DecodeArgument(&payload); err != nil {
						return durableScheduleRecord{}, err
					}
					return durableScheduleRecord{
						ID: schedule.ID, Action: schedule.Action,
						Label: payload.Label, RunAt: schedule.RunAt.UnixMilli(),
					}, nil
				}),
				"recordScheduled": rivet.Action(func(
					actor *rivet.Context[durableScheduleState],
					payload durableSchedulePayload,
				) (bool, error) {
					actor.State().Fired = append(actor.State().Fired, payload.Label)
					executions <- durableScheduleExecution{
						Label: payload.Label, Generation: actor.Generation(),
					}
					return true, nil
				}),
				"failScheduled": rivet.Action(func(
					actor *rivet.Context[durableScheduleState],
					payload durableSchedulePayload,
				) (bool, error) {
					executions <- durableScheduleExecution{
						Label: payload.Label, Generation: actor.Generation(), Failed: true,
					}
					return false, rivet.ActionError{Code: "scheduled_failure", Message: "intentional scheduled failure"}
				}),
				"state": rivet.Action(func(
					actor *rivet.Context[durableScheduleState],
					_ struct{},
				) (durableScheduleState, error) {
					return *actor.State(), nil
				}),
				"sleep": rivet.Action(func(actor *rivet.Context[durableScheduleState], _ struct{}) (bool, error) {
					return true, actor.Sleep()
				}),
			},
		})
		if err != nil {
			t.Fatalf("register durable schedule actor: %v", err)
		}
		return registry
	}

	runnerName := fmt.Sprintf("rivet-go-action-schedules-%d", time.Now().UnixNano())
	firstRunner := startRegistry(t, engine, runnerName, newRegistry())
	actor := createActor(t, engine.endpoint, "action-schedules", runnerName, "restart", nil, nil)
	initialGeneration := <-starts

	records := decodeActionOutput[[]durableScheduleRecord](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "scheduleBatch", []any{struct{}{}}, 40*time.Second,
	), http.StatusOK)
	if len(records) != 4 {
		t.Fatalf("created schedules = %#v", records)
	}
	for index := 1; index < len(records); index++ {
		if records[index].ID == records[index-1].ID {
			t.Fatalf("schedule IDs are not unique: %#v", records)
		}
	}
	got := decodeActionOutput[durableScheduleRecord](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "getSchedule", []any{records[0].ID}, 10*time.Second,
	), http.StatusOK)
	if got.ID != records[0].ID || got.Label != "first" {
		t.Fatalf("get schedule = %#v, want %#v", got, records[0])
	}
	cancelled := decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "cancelSchedule", []any{records[1].ID}, 10*time.Second,
	), http.StatusOK)
	if !cancelled {
		t.Fatal("pending schedule was not cancelled")
	}
	pending := decodeActionOutput[[]durableScheduleRecord](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "inspectSchedules", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	if len(pending) != 3 || pending[0].Label != "first" || pending[1].Label != "fails" || pending[2].Label != "last" {
		t.Fatalf("pending schedules after cancellation = %#v", pending)
	}

	decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "sleep", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	firstRunner.stop(t)
	startRegistry(t, engine, runnerName, newRegistry())

	var wakeGeneration uint64
	select {
	case wakeGeneration = <-starts:
	case <-time.After(120 * time.Second):
		t.Fatal("scheduled action did not wake the actor after runner restart")
	}
	if wakeGeneration <= initialGeneration {
		t.Fatalf("wake generation = %d, initial = %d", wakeGeneration, initialGeneration)
	}
	wantExecutions := []durableScheduleExecution{
		{Label: "first"},
		{Label: "fails", Failed: true},
		{Label: "last"},
	}
	for index, want := range wantExecutions {
		select {
		case got := <-executions:
			if got.Label != want.Label || got.Failed != want.Failed || got.Generation != wakeGeneration {
				t.Fatalf("execution %d = %#v, want label=%q failed=%t generation=%d", index, got, want.Label, want.Failed, wakeGeneration)
			}
		case <-time.After(120 * time.Second):
			t.Fatalf("timed out waiting for scheduled execution %d (%s)", index, want.Label)
		}
	}
	select {
	case extra := <-executions:
		t.Fatalf("cancelled or duplicate schedule executed: %#v", extra)
	case <-time.After(2 * time.Second):
	}

	state := decodeActionOutput[durableScheduleState](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "state", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	if len(state.Fired) != 2 || state.Fired[0] != "first" || state.Fired[1] != "last" {
		t.Fatalf("state after scheduled action error = %#v", state)
	}
	pending = decodeActionOutput[[]durableScheduleRecord](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "inspectSchedules", []any{struct{}{}}, 10*time.Second,
	), http.StatusOK)
	if len(pending) != 0 {
		t.Fatalf("completed schedules remain pending: %#v", pending)
	}
	deleteActor(t, engine.endpoint, actor.ActorID)
}
