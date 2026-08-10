package rivet

import (
	"context"
	"errors"
	"testing"

	"github.com/ewhauser/rivet-go/internal/wire"
)

func TestCleanupFailedActorStartCancelsWorkAndClosesDatabase(t *testing.T) {
	backend := &lifecycleSQLiteBackend{closed: make(chan struct{})}
	managedCtx, managedCancel := context.WithCancel(context.Background())
	managedFinished := make(chan struct{})
	actorContext := &Context[struct{}]{
		db:            makeDB(backend),
		managedCtx:    managedCtx,
		managedCancel: managedCancel,
	}
	actorContext.managedWG.Add(1)
	go func() {
		defer actorContext.managedWG.Done()
		defer close(managedFinished)
		<-managedCtx.Done()
	}()

	if err := cleanupFailedActorStart(actorContext); err != nil {
		t.Fatal(err)
	}
	select {
	case <-managedFinished:
	default:
		t.Fatal("managed work was not cancelled and drained")
	}
	select {
	case <-backend.closed:
	default:
		t.Fatal("database was not closed")
	}
	actorContext.lifecycleMu.Lock()
	stopping := actorContext.lifecycleStopping
	actorContext.lifecycleMu.Unlock()
	if !stopping {
		t.Fatal("failed actor start was not marked stopping")
	}
	actorContext.managedMu.Lock()
	managedStopping := actorContext.managedStopping
	actorContext.managedMu.Unlock()
	if !managedStopping {
		t.Fatal("failed actor start still accepted managed work")
	}
}

func TestActorAdapterStartCleansUpWhenOnStartPanics(t *testing.T) {
	managedFinished := make(chan struct{})
	var actorContext *Context[struct{}]
	adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		OnStart: func(ctx *Context[struct{}]) error {
			actorContext = ctx
			ctx.managedWG.Add(1)
			go func() {
				defer ctx.managedWG.Done()
				defer close(managedFinished)
				<-ctx.managedCtx.Done()
			}()
			panic("on-start panic")
		},
	}}

	recovered := func() (panicValue any) {
		defer func() { panicValue = recover() }()
		_, _ = adapter.Start(context.Background(), nil, wire.Event{})
		return nil
	}()
	if recovered != "on-start panic" {
		t.Fatalf("Start panic = %#v, want on-start panic", recovered)
	}
	if actorContext == nil {
		t.Fatal("OnStart did not receive actor context")
	}
	select {
	case <-managedFinished:
	default:
		t.Fatal("OnStart panic did not drain managed work")
	}
	select {
	case <-actorContext.managedCtx.Done():
	default:
		t.Fatal("OnStart panic did not cancel generation context")
	}
}

func TestActorAdapterStartCleansUpWhenOnStartReturnsError(t *testing.T) {
	want := errors.New("on-start failed")
	var actorContext *Context[struct{}]
	adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		OnStart: func(ctx *Context[struct{}]) error {
			actorContext = ctx
			return want
		},
	}}

	_, err := adapter.Start(context.Background(), nil, wire.Event{})
	if !errors.Is(err, want) {
		t.Fatalf("Start error = %v, want %v", err, want)
	}
	select {
	case <-actorContext.managedCtx.Done():
	default:
		t.Fatal("OnStart error did not cancel generation context")
	}
}
