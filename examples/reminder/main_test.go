package main

import (
	"testing"
	"time"
)

func TestReminderSchedule(t *testing.T) {
	t.Parallel()
	now := time.Unix(100, 0)
	message, dueAt, err := reminderSchedule(scheduleArgs{
		Message:           "  deploy  ",
		DelayMilliseconds: 1_500,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if message != "deploy" {
		t.Fatalf("message = %q, want deploy", message)
	}
	if want := now.Add(1500 * time.Millisecond); !dueAt.Equal(want) {
		t.Fatalf("due time = %s, want %s", dueAt, want)
	}
}

func TestReminderScheduleRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	for _, test := range []scheduleArgs{
		{Message: " ", DelayMilliseconds: 1},
		{Message: "valid", DelayMilliseconds: 0},
		{Message: "valid", DelayMilliseconds: maxReminderDelay.Milliseconds() + 1},
	} {
		if _, _, err := reminderSchedule(test, time.Now()); err == nil {
			t.Fatalf("reminderSchedule(%#v) succeeded", test)
		}
	}
}
