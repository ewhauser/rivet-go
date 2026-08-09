package rivet

import (
	"errors"
	"testing"

	"github.com/ewhauser/rivet-go/internal/pump"
)

func TestDestroyLifecycleErrorsAreStable(t *testing.T) {
	var missing *Context[struct{}]
	if err := missing.Destroy(); !errors.Is(err, ErrActorUnavailable) {
		t.Fatalf("nil Context Destroy = %v, want ErrActorUnavailable", err)
	}

	starting := &Context[struct{}]{session: &pump.ActorSession{}}
	if err := starting.Destroy(); !errors.Is(err, ErrActorStarting) {
		t.Fatalf("starting Context Destroy = %v, want ErrActorStarting", err)
	}

	stopping := &Context[struct{}]{
		session:           &pump.ActorSession{},
		lifecycleStarted:  true,
		lifecycleStopping: true,
	}
	if err := stopping.Destroy(); !errors.Is(err, ErrActorStopping) {
		t.Fatalf("stopping Context Destroy = %v, want ErrActorStopping", err)
	}

	destroyed := &Context[struct{}]{
		session:          &pump.ActorSession{},
		lifecycleStarted: true,
		destroyRequested: true,
	}
	if err := destroyed.Destroy(); !errors.Is(err, ErrActorStopping) {
		t.Fatalf("duplicate Context Destroy = %v, want ErrActorStopping", err)
	}
}

func TestDestroyIntentCanRetryAfterSQLiteCleanupFails(t *testing.T) {
	cleanupFailure := errors.New("close transport")
	coreFailure := errors.New("submit destroy intent")
	backend := &lifecycleSQLiteBackend{
		closed:   make(chan struct{}),
		closeErr: cleanupFailure,
	}
	database := makeDB(backend)
	attempts := 0
	requestDestroy := func() error {
		attempts++
		if attempts == 1 {
			return coreFailure
		}
		return nil
	}

	accepted, err := closeSQLiteAndRequestDestroy(database, requestDestroy)
	if accepted {
		t.Fatal("failed destroy intent reported accepted")
	}
	if !errors.Is(err, cleanupFailure) || !errors.Is(err, coreFailure) {
		t.Fatalf("first destroy error = %v, want cleanup and core failures", err)
	}

	accepted, err = closeSQLiteAndRequestDestroy(database, requestDestroy)
	if !accepted {
		t.Fatal("successful destroy retry was not accepted")
	}
	if !errors.Is(err, cleanupFailure) {
		t.Fatalf("successful destroy retry error = %v, want cleanup failure", err)
	}
	if attempts != 2 {
		t.Fatalf("destroy intent attempts = %d, want 2", attempts)
	}
	select {
	case <-backend.closed:
	default:
		t.Fatal("SQLite transport was not fenced")
	}
}

func TestDestroyMapsCoreLifecycleErrors(t *testing.T) {
	tests := []struct {
		code string
		want error
	}{
		{code: "starting", want: ErrActorStarting},
		{code: "stopping", want: ErrActorStopping},
		{code: "destroying", want: ErrActorStopping},
		{code: "actor_generation_stale", want: ErrActorStopping},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			err := actorDestroyError(pump.HandlerError{Code: test.code, Message: "lifecycle error"})
			if !errors.Is(err, test.want) {
				t.Fatalf("actorDestroyError = %v, want %v", err, test.want)
			}
		})
	}
}
