package rivet

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ewhauser/rivet-go/internal/pump"
)

func TestQueueValueAndResponseEncoding(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
		Rank int    `json:"rank"`
	}
	encoded, err := encodeQueueValue("jobs", payload{Name: "build", Rank: 3})
	if err != nil {
		t.Fatal(err)
	}
	message := (&Queue{}).message(pump.QueueMessage{
		ID: 7, Name: "jobs", Body: encoded, CreatedAt: 123, Completable: true,
	})
	var decoded payload
	if err := message.DecodeBody(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != (payload{Name: "build", Rank: 3}) || !message.Completable() || message.ID != 7 {
		t.Fatalf("decoded queue message = %#v / %#v", decoded, message)
	}
	response := QueueResponse{Present: true, Data: encoded}
	decoded = payload{}
	if err := response.Decode(&decoded); err != nil || decoded.Name != "build" {
		t.Fatalf("decoded queue response = %#v, %v", decoded, err)
	}
	if err := (QueueResponse{}).Decode(&decoded); err == nil {
		t.Fatal("absent queue response decoded successfully")
	}
}

func TestNormalizeQueueNamesAndErrors(t *testing.T) {
	names, err := normalizeQueueNames([]string{"high", "low", "high"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"high", "low"}) {
		t.Fatalf("normalized names = %#v", names)
	}
	if _, err := normalizeQueueNames([]string{"ok", " "}); err == nil {
		t.Fatal("blank queue name was accepted")
	}
	for code, target := range map[string]error{
		"full":                   ErrQueueFull,
		"message_too_large":      ErrQueueMessageTooLarge,
		"timed_out":              ErrQueueTimedOut,
		"aborted":                ErrActorAborted,
		"actor_generation_stale": ErrActorStopping,
	} {
		err := queueError(pump.HandlerError{Code: code, Message: "native failure"})
		if !errors.Is(err, target) {
			t.Fatalf("queue error %q = %v, want %v", code, err, target)
		}
	}
}
