package wire

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// The regression input from fuzzing: a valid envelope prefix whose events
// field is an array32 header claiming ~1.95 billion elements in an 84-byte
// input. Decode must reject it quickly instead of allocating.
func TestDecodeRejectsOversizedArrayHeader(t *testing.T) {
	payload := append(
		[]byte("\x82\xa3seq\x02\xa6events\xdd"),
		[]byte("ted\xa6nnect{\xd6")...,
	)
	start := time.Now()
	_, err := DecodeEventBatch(payload)
	if err == nil {
		t.Fatal("expected error for oversized array header")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("rejection took %v; must be immediate", elapsed)
	}
	if !strings.Contains(err.Error(), "entries") && !strings.Contains(err.Error(), "length") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateShapeRejections(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"reserved 0xc1", []byte{0xc1}},
		{"str32 overrun", []byte{0xdb, 0xff, 0xff, 0xff, 0xff, 'x'}},
		{"bin16 overrun", []byte{0xc5, 0xff, 0xff, 'x'}},
		{"map32 huge", []byte{0xdf, 0x7f, 0xff, 0xff, 0xff}},
		{"truncated fixstr", []byte{0xa5, 'a', 'b'}},
		{"truncated uint32", []byte{0xce, 0x00}},
		{"trailing bytes", []byte{0xc0, 0xc0}},
		{"deep nesting", bytes.Repeat([]byte{0x91}, maxScanDepth+2)},
		{"ext32 overrun", []byte{0xc9, 0xff, 0xff, 0xff, 0xf0, 0x01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateShape(tc.data); err == nil {
				t.Fatalf("expected rejection for %q", tc.name)
			}
		})
	}
}

// Every golden produced by the Rust encoder must pass shape validation —
// the guard must never reject legitimate envelopes.
func TestValidateShapeAcceptsGoldens(t *testing.T) {
	for _, name := range []string{
		"event_connected.msgpack",
		"event_disconnected.msgpack",
		"event_stopped.msgpack",
	} {
		data := golden(t, name)
		if err := validateShape(data); err != nil {
			t.Fatalf("golden %s rejected: %v", name, err)
		}
	}
}
