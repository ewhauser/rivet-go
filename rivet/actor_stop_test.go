package rivet

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/ewhauser/rivet-go/internal/wire"
)

func TestCompleteActorStopAlwaysCancelsAndClosesDatabase(t *testing.T) {
	stopErr := errors.New("on-stop failed")
	drainErr := errors.New("managed work did not drain")
	closeErr := errors.New("database close failed")
	var order []string

	err := completeActorStop(
		stopErr,
		drainErr,
		func() { order = append(order, "cancel") },
		func() error {
			order = append(order, "close")
			return closeErr
		},
	)

	if want := []string{"cancel", "close"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("cleanup order = %v, want %v", order, want)
	}
	for _, cause := range []error{stopErr, drainErr, closeErr} {
		if !errors.Is(err, cause) {
			t.Errorf("completeActorStop error %v does not include %v", err, cause)
		}
	}
}

func TestActorAdapterStopClosesDatabaseWhenOnStopFails(t *testing.T) {
	stopErr := errors.New("on-stop failed")
	backend := &lifecycleSQLiteBackend{closed: make(chan struct{})}
	database := makeDB(backend)
	managedCtx, managedCancel := context.WithCancel(context.Background())
	actorContext := &Context[struct{}]{
		db:            database,
		managedCtx:    managedCtx,
		managedCancel: managedCancel,
	}
	adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		OnStop: func(*Context[struct{}]) error { return stopErr },
	}}

	err := adapter.Stop(context.Background(), nil, wire.Event{}, actorContext)
	if !errors.Is(err, stopErr) {
		t.Fatalf("Stop error = %v, want %v", err, stopErr)
	}
	select {
	case <-backend.closed:
	default:
		t.Fatal("database was not closed")
	}
	select {
	case <-managedCtx.Done():
	default:
		t.Fatal("managed context was not cancelled")
	}
}

func TestActorAdapterStopFencesManagedWorkBeforeOnStop(t *testing.T) {
	backend := &lifecycleSQLiteBackend{closed: make(chan struct{})}
	managedCtx, managedCancel := context.WithCancel(context.Background())
	actorContext := &Context[struct{}]{
		session:       &pump.ActorSession{},
		db:            makeDB(backend),
		managedCtx:    managedCtx,
		managedCancel: managedCancel,
	}
	var waitErr error
	var keepErr error
	workRan := false
	adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		OnStop: func(ctx *Context[struct{}]) error {
			waitErr = ctx.WaitUntil(context.Background(), func(context.Context) error {
				workRan = true
				return nil
			})
			keepErr = ctx.KeepAwake(context.Background(), func(context.Context) error {
				workRan = true
				return nil
			})
			return nil
		},
	}}

	if err := adapter.Stop(context.Background(), nil, wire.Event{}, actorContext); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(waitErr, ErrActorStopping) {
		t.Fatalf("OnStop WaitUntil error = %v, want ErrActorStopping", waitErr)
	}
	if !errors.Is(keepErr, ErrActorStopping) {
		t.Fatalf("OnStop KeepAwake error = %v, want ErrActorStopping", keepErr)
	}
	if workRan {
		t.Fatal("managed work ran after lifecycle stop began")
	}
}
