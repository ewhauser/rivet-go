package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
		"event_actor_start.msgpack",
		"event_actor_start_fresh.msgpack",
		"event_actor_start_empty_state.msgpack",
		"event_actor_stop.msgpack",
		"event_action_call.msgpack",
		"event_http_request.msgpack",
		"event_http_request_chunk.msgpack",
		"event_http_request_abort.msgpack",
		"event_kv_result.msgpack",
		"event_state_persisted.msgpack",
	} {
		data := golden(t, name)
		if err := validateShape(data); err != nil {
			t.Fatalf("golden %s rejected: %v", name, err)
		}
	}
}

func TestValidateShapeAcceptsMaximumM3HTTPChunkAndHeaderSchema(t *testing.T) {
	headers := make(map[string]string, 256)
	for index := range 256 {
		headers[fmt.Sprintf("x-rivet-go-%03d", index)] = "value"
	}
	data, err := encode(EventBatch{Seq: 1, Events: []Event{{
		Kind:       EventHTTPRequest,
		AID:        "actor",
		Generation: 1,
		RequestID:  1,
		Method:     "POST",
		Path:       "/upload",
		Headers:    headers,
		Body:       bytes.Repeat([]byte("x"), maxBlobBytes),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateShape(data); err != nil {
		t.Fatalf("valid M3 HTTP boundary event rejected: %v", err)
	}
	if _, err := DecodeEventBatch(data); err != nil {
		t.Fatalf("decode valid M3 HTTP boundary event: %v", err)
	}
}

func TestValidateShapeCoversEveryMessagePackTypeFamily(t *testing.T) {
	zeros := func(count int) []byte { return make([]byte, count) }
	withPayload := func(prefix []byte, count int) []byte {
		return append(append([]byte(nil), prefix...), zeros(count)...)
	}
	cases := map[string][]byte{
		"positive fixint": {0x00},
		"negative fixint": {0xe0},
		"fixmap":          {0x80},
		"fixarray":        {0x90},
		"fixstr":          {0xa0},
		"nil":             {0xc0},
		"false":           {0xc2},
		"true":            {0xc3},
		"bin8":            {0xc4, 0x00},
		"bin16":           {0xc5, 0x00, 0x00},
		"bin32":           {0xc6, 0x00, 0x00, 0x00, 0x00},
		"ext8":            {0xc7, 0x00, 0x01},
		"ext16":           {0xc8, 0x00, 0x00, 0x01},
		"ext32":           {0xc9, 0x00, 0x00, 0x00, 0x00, 0x01},
		"float32":         withPayload([]byte{0xca}, 4),
		"float64":         withPayload([]byte{0xcb}, 8),
		"uint8":           withPayload([]byte{0xcc}, 1),
		"uint16":          withPayload([]byte{0xcd}, 2),
		"uint32":          withPayload([]byte{0xce}, 4),
		"uint64":          withPayload([]byte{0xcf}, 8),
		"int8":            withPayload([]byte{0xd0}, 1),
		"int16":           withPayload([]byte{0xd1}, 2),
		"int32":           withPayload([]byte{0xd2}, 4),
		"int64":           withPayload([]byte{0xd3}, 8),
		"fixext1":         withPayload([]byte{0xd4}, 2),
		"fixext2":         withPayload([]byte{0xd5}, 3),
		"fixext4":         withPayload([]byte{0xd6}, 5),
		"fixext8":         withPayload([]byte{0xd7}, 9),
		"fixext16":        withPayload([]byte{0xd8}, 17),
		"str8":            {0xd9, 0x00},
		"str16":           {0xda, 0x00, 0x00},
		"str32":           {0xdb, 0x00, 0x00, 0x00, 0x00},
		"array16":         {0xdc, 0x00, 0x00},
		"array32":         {0xdd, 0x00, 0x00, 0x00, 0x00},
		"map16":           {0xde, 0x00, 0x00},
		"map32":           {0xdf, 0x00, 0x00, 0x00, 0x00},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateShape(data); err != nil {
				t.Fatalf("valid %s rejected: %v", name, err)
			}
		})
	}
}

func TestValidateShapeEnforcesBoundaryLimitsBeforeDecode(t *testing.T) {
	array := make([]byte, 5+maxArrayEntries+1)
	array[0] = 0xdd
	binary.BigEndian.PutUint32(array[1:], uint32(maxArrayEntries+1))
	for i := 5; i < len(array); i++ {
		array[i] = 0xc0
	}

	mapValue := make([]byte, 3+2*(maxMapEntries+1))
	mapValue[0] = 0xde
	binary.BigEndian.PutUint16(mapValue[1:], uint16(maxMapEntries+1))
	for i := 3; i < len(mapValue); i++ {
		mapValue[i] = 0xc0
	}

	blob := make([]byte, 5+maxBlobBytes+1)
	blob[0] = 0xc6
	binary.BigEndian.PutUint32(blob[1:], uint32(maxBlobBytes+1))

	for name, data := range map[string][]byte{
		"array cardinality": array,
		"map cardinality":   mapValue,
		"blob size":         blob,
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateShape(data); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
				t.Fatalf("limit rejection = %v, want exceeds limit", err)
			}
		})
	}
}

func TestGoEncoderOutputsPassShapeValidation(t *testing.T) {
	values := []any{
		RunnerConfig{
			EngineEndpoint: "http://127.0.0.1:6420",
			Namespace:      "default",
			RunnerName:     "shape-test",
			Version:        1,
			TotalSlots:     1,
			ActorNames:     []string{},
			ActorActions:   map[string][]string{},
			LogLevel:       "info",
		},
		CommandBatch{Commands: []Command{}},
		EventBatch{Seq: 1, Events: []Event{{
			Kind:     EventRunnerConnected,
			RunnerID: "runner",
			Metadata: map[string]string{"protocol": "envoy-v6"},
		}}},
		EventBatch{Seq: 2, Events: []Event{{
			Kind:   EventRunnerDisconnected,
			Reason: "test",
		}}},
		EventBatch{Seq: 3, Events: []Event{{
			Kind: EventRunnerStopped,
			DrainReport: &DrainReport{
				Graceful: true,
			},
		}}},
		EventBatch{Seq: 4, Events: []Event{{
			Kind:           EventActorStart,
			AID:            "actor",
			Name:           "counter",
			PersistedState: []byte("state"),
		}}},
		EventBatch{Seq: 5, Events: []Event{{
			Kind:   EventActorStop,
			AID:    "actor",
			Reason: "destroy",
		}}},
		EventBatch{Seq: 6, Events: []Event{{
			Kind: EventKVResult,
			KVID: 1,
			Entries: []KVEntry{{
				Key: []byte("key"), Value: []byte("value"),
			}},
		}}},
		EventBatch{Seq: 7, Events: []Event{{
			Kind:         EventStatePersisted,
			AID:          "actor",
			StateVersion: 1,
		}}},
	}
	for _, value := range values {
		encoded, err := encode(value)
		if err != nil {
			t.Fatalf("encode %T: %v", value, err)
		}
		if err := validateShape(encoded); err != nil {
			t.Fatalf("encoded %T rejected: %v", value, err)
		}
	}
}
