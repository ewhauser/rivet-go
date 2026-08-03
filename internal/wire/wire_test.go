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
		ActorNames:     []string{},
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
