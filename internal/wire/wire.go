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
	EventActorStart         EventKind = "actor_start"
	EventActorStop          EventKind = "actor_stop"
	EventKVResult           EventKind = "kv_result"
	EventStatePersisted     EventKind = "state_persisted"
)

type CommandKind string

const (
	CommandActorStartResult CommandKind = "actor_start_result"
	CommandActorStopResult  CommandKind = "actor_stop_result"
	CommandSaveState        CommandKind = "save_state"
	CommandKVGet            CommandKind = "kv_get"
	CommandKVList           CommandKind = "kv_list"
	CommandKVPut            CommandKind = "kv_put"
	CommandKVDelete         CommandKind = "kv_delete"
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

// Event is the M2 event union. Fields not selected by Kind are absent from
// Rust's encoded map and remain zero-valued after decoding.
type Event struct {
	Kind           EventKind         `msgpack:"kind"`
	RunnerID       string            `msgpack:"runner_id,omitempty"`
	Metadata       map[string]string `msgpack:"metadata,omitempty"`
	Reason         string            `msgpack:"reason,omitempty"`
	DrainReport    *DrainReport      `msgpack:"drain_report,omitempty"`
	AID            string            `msgpack:"aid,omitempty"`
	Generation     uint64            `msgpack:"gen,omitempty"`
	Name           string            `msgpack:"name,omitempty"`
	Key            string            `msgpack:"key,omitempty"`
	CreateTS       int64             `msgpack:"create_ts,omitempty"`
	Input          []byte            `msgpack:"input,omitempty"`
	PersistedState []byte            `msgpack:"persisted_state,omitempty"`
	KVID           uint64            `msgpack:"kv_id,omitempty"`
	Value          []byte            `msgpack:"value,omitempty"`
	Entries        []KVEntry         `msgpack:"entries,omitempty"`
	StateVersion   uint64            `msgpack:"state_version,omitempty"`
	Error          *WireError        `msgpack:"error,omitempty"`
}

type DrainReport struct {
	Graceful        bool   `msgpack:"graceful"`
	ElapsedMS       uint64 `msgpack:"elapsed_ms"`
	ActorsStopped   uint32 `msgpack:"actors_stopped"`
	ActorsRemaining uint32 `msgpack:"actors_remaining"`
}

type WireError struct {
	Code    string `msgpack:"code"`
	Message string `msgpack:"message"`
}

func (e WireError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

type KVEntry struct {
	Key   []byte `msgpack:"key"`
	Value []byte `msgpack:"value"`
}

type CommandBatch struct {
	Commands []Command `msgpack:"commands"`
}

// Command is the M2 command union. All fields are encoded so zero-length keys,
// values, state, and generation zero remain distinguishable from a missing
// required field. Rust ignores fields that do not belong to the selected kind.
type Command struct {
	Kind       CommandKind `msgpack:"kind"`
	AID        string      `msgpack:"aid"`
	Generation uint64      `msgpack:"gen"`
	OK         bool        `msgpack:"ok"`
	Error      *WireError  `msgpack:"error"`
	State      []byte      `msgpack:"state"`
	KVID       uint64      `msgpack:"kv_id"`
	Key        []byte      `msgpack:"key"`
	Prefix     []byte      `msgpack:"prefix"`
	Reverse    bool        `msgpack:"reverse"`
	Limit      *uint32     `msgpack:"limit"`
	Value      []byte      `msgpack:"value"`
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
	case EventActorStart:
		if event.AID == "" || event.Name == "" {
			return fmt.Errorf("%s event requires aid and name", event.Kind)
		}
	case EventActorStop:
		if event.AID == "" || event.Reason == "" {
			return fmt.Errorf("%s event requires aid and reason", event.Kind)
		}
	case EventKVResult:
		if event.KVID == 0 {
			return fmt.Errorf("%s event has invalid kv_id", event.Kind)
		}
	case EventStatePersisted:
		if event.AID == "" {
			return fmt.Errorf("%s event has empty aid", event.Kind)
		}
	default:
		return fmt.Errorf("unknown event kind %q", event.Kind)
	}
	if event.Error != nil && (event.Error.Code == "" || event.Error.Message == "") {
		return fmt.Errorf("%s event has incomplete structured error", event.Kind)
	}
	return nil
}
