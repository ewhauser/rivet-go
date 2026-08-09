package main

import (
	"testing"
	"time"
)

func TestValidateScheduleRequest(t *testing.T) {
	message, delay, err := validateScheduleRequest(scheduleRequest{
		Message: "  deploy  ", DelayMilliseconds: 1_500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if message != "deploy" || delay != 1500*time.Millisecond {
		t.Fatalf("validated request = (%q, %s)", message, delay)
	}
}

func TestValidateScheduleRequestRejectsInvalidInput(t *testing.T) {
	for _, request := range []scheduleRequest{
		{Message: " ", DelayMilliseconds: 1},
		{Message: "valid", DelayMilliseconds: 0},
		{Message: "valid", DelayMilliseconds: maxScheduleDelay.Milliseconds() + 1},
	} {
		if _, _, err := validateScheduleRequest(request); err == nil {
			t.Fatalf("validateScheduleRequest(%#v) succeeded", request)
		}
	}
}

func TestNewReminderID(t *testing.T) {
	first, err := newReminderID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newReminderID()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len("reminder-")+16 || first == second {
		t.Fatalf("reminder IDs = %q, %q", first, second)
	}
}
