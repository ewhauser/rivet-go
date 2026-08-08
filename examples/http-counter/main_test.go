package main

import (
	"strings"
	"testing"
)

func TestDecodeIncrement(t *testing.T) {
	t.Parallel()
	got, err := decodeIncrement(strings.NewReader(`{"amount":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.Amount != 3 {
		t.Fatalf("amount = %d, want 3", got.Amount)
	}
}

func TestDecodeIncrementRejectsUnknownOrTrailingValues(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"amount":3,"surprise":true}`,
		`{"amount":3} {"amount":4}`,
		``,
	} {
		if _, err := decodeIncrement(strings.NewReader(body)); err == nil {
			t.Fatalf("decodeIncrement(%q) succeeded", body)
		}
	}
}
