package wire

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func golden(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return data
}

func TestRustRunnerConfigGolden(t *testing.T) {
	want := RunnerConfig{
		EngineEndpoint: "http://127.0.0.1:6420",
		Namespace:      "default",
		RunnerName:     "rivet-go-golden",
		Version:        1,
		TotalSlots:     4,
		ActorNames:     []string{"counter"},
		ActorActions:   map[string][]string{"counter": {"increment"}},
		LogLevel:       "info",
	}
	var got RunnerConfig
	data := golden(t, "runner_config.msgpack")
	if err := decode(data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded config mismatch:\n got: %#v\nwant: %#v", got, want)
	}
	encoded, err := EncodeRunnerConfig(got)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatalf("Go RunnerConfig encoding differs from Rust golden:\n got: %x\nwant: %x", encoded, data)
	}
}

func TestRustEventBatchGoldens(t *testing.T) {
	tests := []struct {
		name string
		kind EventKind
		seq  uint64
	}{
		{"event_connected.msgpack", EventRunnerConnected, 1},
		{"event_disconnected.msgpack", EventRunnerDisconnected, 2},
		{"event_stopped.msgpack", EventRunnerStopped, 3},
		{"event_actor_start.msgpack", EventActorStart, 4},
		{"event_actor_stop.msgpack", EventActorStop, 5},
		{"event_kv_result.msgpack", EventKVResult, 6},
		{"event_state_persisted.msgpack", EventStatePersisted, 7},
		{"event_action_call.msgpack", EventActionCall, 10},
		{"event_http_request.msgpack", EventHTTPRequest, 11},
		{"event_http_request_chunk.msgpack", EventHTTPRequestChunk, 12},
		{"event_http_request_abort.msgpack", EventHTTPRequestAbort, 13},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch, err := DecodeEventBatch(golden(t, test.name))
			if err != nil {
				t.Fatal(err)
			}
			if batch.Seq != test.seq || len(batch.Events) != 1 || batch.Events[0].Kind != test.kind {
				t.Fatalf("unexpected decoded batch: %#v", batch)
			}
		})
	}
}

func TestRustM3CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m3.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	want := []CommandKind{
		CommandActionResult,
		CommandHTTPResponseStart,
		CommandHTTPResponseChunk,
	}
	if len(batch.Commands) != len(want) {
		t.Fatalf("command count = %d, want %d", len(batch.Commands), len(want))
	}
	for index, kind := range want {
		if batch.Commands[index].Kind != kind {
			t.Fatalf("command %d kind = %q, want %q", index, batch.Commands[index].Kind, kind)
		}
	}
	if batch.Commands[0].CallID != 21 || batch.Commands[0].Output == nil {
		t.Fatalf("unexpected action result: %#v", batch.Commands[0])
	}
	if batch.Commands[1].RequestID != 22 || batch.Commands[1].Status != 201 || !batch.Commands[1].Stream {
		t.Fatalf("unexpected HTTP response start: %#v", batch.Commands[1])
	}
	if !batch.Commands[2].Finish || string(batch.Commands[2].Body) != "response" {
		t.Fatalf("unexpected HTTP response chunk: %#v", batch.Commands[2])
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M3 golden")
	}
}

func TestRustM2CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m2.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	want := []CommandKind{
		CommandActorStartResult,
		CommandActorStopResult,
		CommandSaveState,
		CommandKVGet,
		CommandKVList,
		CommandKVPut,
		CommandKVDelete,
	}
	if len(batch.Commands) != len(want) {
		t.Fatalf("command count = %d, want %d", len(batch.Commands), len(want))
	}
	for i, kind := range want {
		if batch.Commands[i].Kind != kind {
			t.Fatalf("command %d kind = %q, want %q", i, batch.Commands[i].Kind, kind)
		}
	}
	if batch.Commands[0].AID != "actor-golden" || !batch.Commands[0].OK {
		t.Fatalf("unexpected actor start result: %#v", batch.Commands[0])
	}
	if batch.Commands[4].Limit == nil || *batch.Commands[4].Limit != 32 {
		t.Fatalf("unexpected KV list limit: %#v", batch.Commands[4].Limit)
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M2 golden")
	}
}

func TestRustActorStartGoldenPreservesStatePresence(t *testing.T) {
	fresh, err := DecodeEventBatch(golden(t, "event_actor_start_fresh.msgpack"))
	if err != nil {
		t.Fatal(err)
	}
	empty, err := DecodeEventBatch(golden(t, "event_actor_start_empty_state.msgpack"))
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Events[0].PersistedState != nil {
		t.Fatal("fresh ActorStart decoded with persisted state present")
	}
	if empty.Events[0].PersistedState == nil || len(empty.Events[0].PersistedState) != 0 {
		t.Fatal("persisted empty ActorStart did not retain zero-length state presence")
	}
}

func TestRustEmptyCommandBatchGolden(t *testing.T) {
	data := golden(t, "command_empty.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Commands) != 0 {
		t.Fatalf("expected no commands, got %#v", batch.Commands)
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatalf("Go CommandBatch encoding differs from Rust golden:\n got: %x\nwant: %x", encoded, data)
	}
}

func decode(data []byte, value any) error {
	return msgpack.Unmarshal(data, value)
}
