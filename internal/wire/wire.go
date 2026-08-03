// Package wire defines the private MessagePack contract shared with the Rust
// FFI crate. It is versioned by the native ABI and is not a public SDK API.
package wire

import (
	"bytes"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type EventKind string

const (
	EventRunnerConnected    EventKind = "runner_connected"
	EventRunnerDisconnected EventKind = "runner_disconnected"
	EventRunnerStopped      EventKind = "runner_stopped"
)

// RunnerConfig is consumed by rk_runner_new.
type RunnerConfig struct {
	EngineEndpoint string   `msgpack:"engine_endpoint"`
	Namespace      string   `msgpack:"namespace"`
	RunnerName     string   `msgpack:"runner_name"`
	Version        uint32   `msgpack:"version"`
	TotalSlots     uint32   `msgpack:"total_slots"`
	ActorNames     []string `msgpack:"actor_names"`
	LogLevel       string   `msgpack:"log_level"`
}

// EventBatch is returned by rk_runner_poll. Seq is monotonic per runner.
type EventBatch struct {
	Seq    uint64  `msgpack:"seq"`
	Events []Event `msgpack:"events"`
}

// Event is the M1 event union. Fields not selected by Kind are absent from
// the encoded map.
type Event struct {
	Kind        EventKind         `msgpack:"kind"`
	RunnerID    string            `msgpack:"runner_id,omitempty"`
	Metadata    map[string]string `msgpack:"metadata,omitempty"`
	Reason      string            `msgpack:"reason,omitempty"`
	DrainReport *DrainReport      `msgpack:"drain_report,omitempty"`
}

type DrainReport struct {
	Graceful        bool   `msgpack:"graceful"`
	ElapsedMS       uint64 `msgpack:"elapsed_ms"`
	ActorsStopped   uint32 `msgpack:"actors_stopped"`
	ActorsRemaining uint32 `msgpack:"actors_remaining"`
}

// CommandBatch is accepted by rk_runner_submit. M1 supports empty batches;
// non-empty batches are represented so Rust can return unknown_command.
type CommandBatch struct {
	Commands []Command `msgpack:"commands"`
}

type Command struct {
	Kind string `msgpack:"kind"`
}

func EncodeRunnerConfig(config RunnerConfig) ([]byte, error) {
	return encode(config)
}

func EncodeCommandBatch(batch CommandBatch) ([]byte, error) {
	return encode(batch)
}

func DecodeEventBatch(data []byte) (EventBatch, error) {
	if err := validateShape(data); err != nil {
		return EventBatch{}, fmt.Errorf("decode EventBatch: %w", err)
	}
	var batch EventBatch
	if err := msgpack.Unmarshal(data, &batch); err != nil {
		return EventBatch{}, fmt.Errorf("decode EventBatch: %w", err)
	}
	for i, event := range batch.Events {
		if err := validateEvent(event); err != nil {
			return EventBatch{}, fmt.Errorf("event %d: %w", i, err)
		}
	}
	return batch, nil
}

func encode(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := msgpack.NewEncoder(&buffer)
	encoder.SetSortMapKeys(true)
	encoder.UseCompactInts(true)
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("encode MessagePack envelope: %w", err)
	}
	return buffer.Bytes(), nil
}

func validateEvent(event Event) error {
	switch event.Kind {
	case EventRunnerConnected:
		if event.RunnerID == "" {
			return fmt.Errorf("%s event has empty runner_id", event.Kind)
		}
	case EventRunnerDisconnected:
		if event.Reason == "" {
			return fmt.Errorf("%s event has empty reason", event.Kind)
		}
	case EventRunnerStopped:
		if event.DrainReport == nil {
			return fmt.Errorf("%s event has no drain_report", event.Kind)
		}
	default:
		return fmt.Errorf("unknown event kind %q", event.Kind)
	}
	return nil
}
