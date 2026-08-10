package conformance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
	"github.com/gorilla/websocket"
)

type destroyActionResult struct {
	Accepted    int  `json:"accepted"`
	Stopping    int  `json:"stopping"`
	LeaseClosed bool `json:"leaseClosed"`
}

func TestContextDestroyOwnsGenerationTeardown(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	starting := make(chan error, 1)
	stopping := make(chan error, 1)
	workStarted := make(chan struct{})
	workRelease := make(chan struct{})
	workDone := make(chan struct{})
	echoed := make(chan error, 1)
	var workReleaseOnce sync.Once
	releaseWork := func() { workReleaseOnce.Do(func() { close(workRelease) }) }
	t.Cleanup(releaseWork)
	var starts sync.WaitGroup
	starts.Add(1)

	registry := rivet.NewRegistry()
	err = rivet.Register(registry, "m12-destroy", rivet.Actor[struct{}]{
		Database: true,
		OnStart: func(ctx *rivet.Context[struct{}]) error {
			starting <- ctx.Destroy()
			starts.Done()
			return nil
		},
		OnStop: func(ctx *rivet.Context[struct{}]) error {
			stopping <- ctx.Destroy()
			return nil
		},
		Actions: rivet.Actions[struct{}]{
			"health": rivet.Action(func(_ *rivet.Context[struct{}], _ struct{}) (bool, error) {
				return true, nil
			}),
			"destroy": rivet.Action(func(ctx *rivet.Context[struct{}], _ struct{}) (destroyActionResult, error) {
				txCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				tx, err := ctx.DB().Begin(txCtx)
				if err != nil {
					return destroyActionResult{}, fmt.Errorf("begin destroy lease: %w", err)
				}
				if err := ctx.WaitUntil(context.Background(), func(context.Context) error {
					close(workStarted)
					<-workRelease
					close(workDone)
					return nil
				}); err != nil {
					return destroyActionResult{}, fmt.Errorf("register destroy work: %w", err)
				}

				errorsSeen := make(chan error, 2)
				var calls sync.WaitGroup
				calls.Add(2)
				for range 2 {
					go func() {
						defer calls.Done()
						errorsSeen <- ctx.Destroy()
					}()
				}
				calls.Wait()
				close(errorsSeen)
				result := destroyActionResult{}
				for err := range errorsSeen {
					switch {
					case err == nil:
						result.Accepted++
					case errors.Is(err, rivet.ErrActorStopping):
						result.Stopping++
					default:
						return destroyActionResult{}, fmt.Errorf("destroy actor: %w", err)
					}
				}
				_, leaseErr := tx.Exec(context.Background(), "SELECT 1")
				var sqliteErr *rivet.SQLiteError
				result.LeaseClosed = errors.As(leaseErr, &sqliteErr) &&
					(sqliteErr.Code == "sqlite_endpoint_closed" || sqliteErr.Code == "invalid_lease_key")
				return result, nil
			}),
		},
		OnConnect: func(*rivet.Context[struct{}], *rivet.Connection) error { return nil },
		OnMessage: func(_ *rivet.Context[struct{}], connection *rivet.Connection, message rivet.Message) {
			echoed <- connection.Send(message.Data)
		},
	})
	if err != nil {
		t.Fatalf("register destroy actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m12-%d", time.Now().UnixNano())
	served := startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "m12-destroy", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})

	starts.Wait()
	if err := <-starting; !errors.Is(err, rivet.ErrActorStarting) {
		t.Fatalf("OnStart Destroy = %v, want ErrActorStarting", err)
	}
	client := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "destroy", true)
	client.write(t, websocket.TextMessage, []byte("destroy-ready"))
	select {
	case err := <-echoed:
		if err != nil {
			t.Fatalf("echo WebSocket readiness probe: %v", err)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("WebSocket readiness probe did not reach the actor")
	}
	waitTextFrame(t, client, "destroy-ready")
	result := decodeActionOutput[destroyActionResult](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "destroy", []any{struct{}{}}, websocketTestTimeout,
	), http.StatusOK)
	if result.Accepted != 1 || result.Stopping != 1 || !result.LeaseClosed {
		t.Fatalf("destroy action result = %#v", result)
	}
	select {
	case <-workStarted:
	case <-time.After(websocketTestTimeout):
		t.Fatal("managed work did not start before destroy")
	}
	if current, getErr := getActor(engine.endpoint, actor.ActorID, true); getErr != nil {
		t.Fatal(getErr)
	} else if current.DestroyTS != nil {
		t.Fatalf("actor destroyed before admitted work drained: %#v", current)
	}
	releaseWork()
	select {
	case <-workDone:
	case <-time.After(websocketTestTimeout):
		t.Fatal("managed work did not finish during destroy")
	}
	select {
	case err := <-stopping:
		if !errors.Is(err, rivet.ErrActorStopping) {
			t.Fatalf("OnStop Destroy = %v, want ErrActorStopping", err)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("OnStop did not run during destroy")
	}
	assertGatewayWebSocketClose(t, client, 1001, "actor stopped")
	destroyed := waitForActor(t, engine.endpoint, actor.ActorID, true, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS == nil && actor.DestroyTS != nil
	})
	if len(destroyed.Error) != 0 && string(destroyed.Error) != "null" {
		t.Fatalf("destroyed actor recorded a crash: %s", destroyed.Error)
	}
	query := url.Values{"namespace": {"default"}, "name": {"m12-destroy"}, "include_destroyed": {"false"}}
	var visible actorListResponse
	requestJSON(t, http.MethodGet, engine.endpoint+"/actors?"+query.Encode(), nil, &visible)
	for _, listed := range visible.Actors {
		if listed.ActorID == actor.ActorID {
			t.Fatal("destroyed actor remains visible in the default actor list")
		}
	}
	wake := gatewayAction(t, engine.endpoint, actor.ActorID, "health", []any{struct{}{}}, 5*time.Second)
	if wake.err == nil && wake.response != nil && wake.response.StatusCode >= 200 && wake.response.StatusCode < 300 {
		t.Fatalf("destroyed actor accepted a wake action: status=%s body=%s", responseStatus(wake.response), wake.body)
	}
	select {
	case err := <-starting:
		t.Fatalf("destroyed actor started another generation: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	served.stop(t)
	engine.stop()
}
