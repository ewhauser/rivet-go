package wire

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzDecodeEventBatch(f *testing.F) {
	for _, name := range []string{
		"event_connected.msgpack",
		"event_disconnected.msgpack",
		"event_stopped.msgpack",
		"event_actor_start.msgpack",
		"event_actor_stop.msgpack",
		"event_kv_result.msgpack",
		"event_state_persisted.msgpack",
	} {
		data, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			f.Fatalf("read fuzz seed %s: %v", name, err)
		}
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeEventBatch(data)
	})
}
