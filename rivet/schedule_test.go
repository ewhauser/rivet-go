package rivet

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/fxamacker/cbor/v2"
)

func TestScheduledActionDecodeArgument(t *testing.T) {
	type reminder struct {
		ID      string `json:"id"`
		Message string `json:"message"`
	}
	want := reminder{ID: "one", Message: "hello"}
	arguments, err := cbor.Marshal([]any{want})
	if err != nil {
		t.Fatal(err)
	}
	schedule := ScheduledAction{Arguments: arguments}
	var got reminder
	if err := schedule.DecodeArgument(&got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded argument = %#v, want %#v", got, want)
	}
}

func TestScheduledActionDecodeArgumentRejectsWrongArity(t *testing.T) {
	arguments, err := cbor.Marshal([]any{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	var destination int
	if err := (ScheduledAction{Arguments: arguments}).DecodeArgument(&destination); err == nil {
		t.Fatal("DecodeArgument accepted two arguments")
	}
}

func TestActionSchedulesValidateBeforeSubmitting(t *testing.T) {
	schedules := &ActionSchedules{
		session: &pump.ActorSession{},
		actions: map[string]struct{}{"remind": {}},
	}
	if _, err := schedules.After(context.Background(), 0, "missing", 1); err == nil ||
		!strings.Contains(err.Error(), "not registered") {
		t.Fatalf("After unregistered action error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := schedules.After(cancelled, 0, "remind", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("After cancelled error = %v, want context.Canceled", err)
	}
}
