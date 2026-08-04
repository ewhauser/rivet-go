package main

import (
	"strings"
	"testing"
)

func TestCounterTruthComparesEveryField(t *testing.T) {
	initial := counterState{Value: 7, Operations: 3, LastToken: "old", LastDelta: -2, Checksum: 41}
	args := counterArgs{Token: "next", Delta: 5}
	want := truthCounterUpdate(initial, args)
	got := initial
	actorCounterUpdate(&got, args)
	if err := compareCounter("unit", got, want); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*counterState)
	}{
		{name: "value", mutate: func(state *counterState) { state.Value++ }},
		{name: "operations", mutate: func(state *counterState) { state.Operations++ }},
		{name: "last token", mutate: func(state *counterState) { state.LastToken += "-wrong" }},
		{name: "last delta", mutate: func(state *counterState) { state.LastDelta++ }},
		{name: "checksum", mutate: func(state *counterState) { state.Checksum++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := got
			test.mutate(&changed)
			if err := compareCounter("unit", changed, want); err == nil {
				t.Fatal("field divergence passed")
			}
		})
	}
}

func TestChatReceiptLedgersAreOrderedAndExactlyOnce(t *testing.T) {
	oracle := newChatOracle()
	oracle.connect("alpha")
	state := chatState{Sequence: 1, Messages: 1, LastToken: "one", Checksum: tokenHash("one") + 1}
	receipt := chatReceipt{Sequence: 1, Token: "one"}
	if err := oracle.onMessage(state, receipt); err != nil {
		t.Fatal(err)
	}
	if err := oracle.record("alpha", receipt); err != nil {
		t.Fatal(err)
	}
	if err := oracle.convergence("alpha"); err != nil {
		t.Fatal(err)
	}
	if err := oracle.record("alpha", receipt); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate receipt error = %v", err)
	}
}

func TestChaosActivationGuardRejectsVacuousRun(t *testing.T) {
	counts := &activationCounts{}
	counts.engineRestarts.Store(1)
	counts.disconnects.Store(1)
	counts.sleepWakes.Store(1)
	counts.stalls.Store(1)
	if err := counts.validate(); err == nil || !strings.Contains(err.Error(), "action_panic") {
		t.Fatalf("activation error = %v", err)
	}
	counts.panics.Store(1)
	if err := counts.validate(); err != nil {
		t.Fatal(err)
	}
}
