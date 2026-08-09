package rivet

import (
	"context"
	"testing"
)

func TestYieldActorTurnClearsAndRestoresCurrentConnection(t *testing.T) {
	actorContext := &Context[struct{}]{}
	connection := &Connection{id: "caller"}
	actorContext.currentConnection = connection
	actorContext.turnMu.Lock()

	resumeTurn := actorContext.yieldTurn()
	observed := make(chan *Connection, 1)
	go func() {
		actorContext.turnMu.Lock()
		observed <- actorContext.CurrentConnection()
		actorContext.turnMu.Unlock()
	}()
	if current := <-observed; current != nil {
		t.Fatalf("yielded actor turn exposed current connection %q", current.ID())
	}

	resumeTurn()
	if current := actorContext.CurrentConnection(); current != connection {
		t.Fatalf("resumed actor turn current connection = %#v, want %#v", current, connection)
	}
	actorContext.turnMu.Unlock()
}

func TestActorAdapterRunEnabledOnlyWhenConfigured(t *testing.T) {
	withoutRun := &actorAdapter[struct{}]{definition: Actor[struct{}]{}}
	if withoutRun.RunEnabled() {
		t.Fatal("RunEnabled = true for actor without Run")
	}

	withRun := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		Run: func(context.Context, *RunContext[struct{}]) error { return nil },
	}}
	if !withRun.RunEnabled() {
		t.Fatal("RunEnabled = false for actor with Run")
	}
}

func TestActorAdapterRunDoesNotEnterStopLifecycle(t *testing.T) {
	actorContext := &Context[struct{}]{lifecycleStarted: true}
	adapter := &actorAdapter[struct{}]{definition: Actor[struct{}]{
		Run: func(context.Context, *RunContext[struct{}]) error { return nil },
	}}
	if err := adapter.Run(context.Background(), nil, actorContext); err != nil {
		t.Fatal(err)
	}
	actorContext.lifecycleMu.Lock()
	stopping := actorContext.lifecycleStopping
	actorContext.lifecycleMu.Unlock()
	if stopping {
		t.Fatal("Run marked the actor generation as stopping")
	}
}
