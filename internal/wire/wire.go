// Package wire defines the private MessagePack contract shared with the Rust
// FFI crate. It is versioned by the native ABI and is not a public SDK API.
package wire

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/vmihailenco/msgpack/v5"
)

type EventKind string

const (
	EventRunnerConnected    EventKind = "runner_connected"
	EventRunnerDisconnected EventKind = "runner_disconnected"
	EventRunnerStopped      EventKind = "runner_stopped"
	EventActorStart         EventKind = "actor_start"
	EventActorStop          EventKind = "actor_stop"
	EventActorAlarm         EventKind = "actor_alarm"
	EventActorIntentResult  EventKind = "actor_intent_result"
	EventActionCall         EventKind = "action_call"
	EventHTTPRequest        EventKind = "http_request"
	EventHTTPRequestChunk   EventKind = "http_request_chunk"
	EventHTTPRequestAbort   EventKind = "http_request_abort"
	EventWSOpen             EventKind = "ws_open"
	EventWSMessage          EventKind = "ws_message"
	EventWSClose            EventKind = "ws_close"
	EventKVResult           EventKind = "kv_result"
	EventStatePersisted     EventKind = "state_persisted"
	EventSQLiteResult       EventKind = "sqlite_result"
)

type CommandKind string

const (
	CommandActorStartResult  CommandKind = "actor_start_result"
	CommandActorStopResult   CommandKind = "actor_stop_result"
	CommandAlarmHandled      CommandKind = "alarm_handled"
	CommandActionResult      CommandKind = "action_result"
	CommandHTTPResponseStart CommandKind = "http_response_start"
	CommandHTTPResponseChunk CommandKind = "http_response_chunk"
	CommandWSOpenResult      CommandKind = "ws_open_result"
	CommandWSMessageAck      CommandKind = "ws_message_ack"
	CommandWSSend            CommandKind = "ws_send"
	CommandWSClose           CommandKind = "ws_close_cmd"
	CommandBroadcast         CommandKind = "broadcast"
	CommandStopIntent        CommandKind = "stop_intent"
	CommandSetAlarm          CommandKind = "set_alarm"
	CommandSleepIntent       CommandKind = "sleep_intent"
	CommandSaveState         CommandKind = "save_state"
	CommandKVGet             CommandKind = "kv_get"
	CommandKVList            CommandKind = "kv_list"
	CommandKVPut             CommandKind = "kv_put"
	CommandKVDelete          CommandKind = "kv_delete"
	CommandSQLiteExec        CommandKind = "sqlite_exec"
	CommandSQLiteQuery       CommandKind = "sqlite_query"
	CommandSQLiteBegin       CommandKind = "sqlite_begin"
	CommandSQLiteCommit      CommandKind = "sqlite_commit"
	CommandSQLiteRollback    CommandKind = "sqlite_rollback"
)

// RunnerConfig is consumed by rk_runner_new.
type RunnerConfig struct {
	EngineEndpoint           string              `msgpack:"engine_endpoint"`
	Namespace                string              `msgpack:"namespace"`
	RunnerName               string              `msgpack:"runner_name"`
	Version                  uint32              `msgpack:"version"`
	TotalSlots               uint32              `msgpack:"total_slots"`
	ActorNames               []string            `msgpack:"actor_names"`
	ActorActions             map[string][]string `msgpack:"actor_actions"`
	ActorHibernateWebSockets map[string]bool     `msgpack:"actor_hibernate_websockets"`
	ActorDatabases           map[string]bool     `msgpack:"actor_databases"`
	SQLiteTransport          string              `msgpack:"sqlite_transport"`
	LogLevel                 string              `msgpack:"log_level"`
}

// EventBatch is returned by rk_runner_poll. Seq is monotonic per runner.
type EventBatch struct {
	Seq    uint64  `msgpack:"seq"`
	Events []Event `msgpack:"events"`
}

// Event is the M5 event union. Fields not selected by Kind are absent from
// Rust's encoded map and remain zero-valued after decoding.
type Event struct {
	Kind             EventKind         `msgpack:"kind"`
	RunnerID         string            `msgpack:"runner_id,omitempty"`
	Metadata         map[string]string `msgpack:"metadata,omitempty"`
	Reason           string            `msgpack:"reason,omitempty"`
	DrainReport      *DrainReport      `msgpack:"drain_report,omitempty"`
	AID              string            `msgpack:"aid,omitempty"`
	Generation       uint64            `msgpack:"gen,omitempty"`
	Name             string            `msgpack:"name,omitempty"`
	Key              string            `msgpack:"key,omitempty"`
	CreateTS         int64             `msgpack:"create_ts,omitempty"`
	Input            []byte            `msgpack:"input,omitempty"`
	PersistedState   []byte            `msgpack:"persisted_state,omitempty"`
	SQLiteSocketPath string            `msgpack:"sqlite_socket_path,omitempty"`
	KVID             uint64            `msgpack:"kv_id,omitempty"`
	Value            []byte            `msgpack:"value,omitempty"`
	Entries          []KVEntry         `msgpack:"entries,omitempty"`
	StateVersion     uint64            `msgpack:"state_version,omitempty"`
	Error            *WireError        `msgpack:"error,omitempty"`
	CallID           uint64            `msgpack:"call_id,omitempty"`
	Action           string            `msgpack:"action,omitempty"`
	ActionTimeoutMS  uint32            `msgpack:"timeout_ms,omitempty"`
	Args             []byte            `msgpack:"args,omitempty"`
	ConnID           *string           `msgpack:"conn_id,omitempty"`
	RequestID        uint64            `msgpack:"req_id,omitempty"`
	Method           string            `msgpack:"method,omitempty"`
	Path             string            `msgpack:"path,omitempty"`
	Headers          map[string]string `msgpack:"headers,omitempty"`
	Body             []byte            `msgpack:"body,omitempty"`
	Stream           bool              `msgpack:"stream,omitempty"`
	Finish           bool              `msgpack:"finish,omitempty"`
	WSID             string            `msgpack:"ws_id,omitempty"`
	CanHibernate     bool              `msgpack:"can_hibernate,omitempty"`
	Resumed          bool              `msgpack:"resumed,omitempty"`
	Data             []byte            `msgpack:"data,omitempty"`
	Binary           bool              `msgpack:"binary,omitempty"`
	MessageIndex     uint16            `msgpack:"msg_index,omitempty"`
	CloseCode        *uint16           `msgpack:"code,omitempty"`
	AlarmTS          int64             `msgpack:"alarm_ts,omitempty"`
	OperationID      uint64            `msgpack:"op_id,omitempty"`
	SQLiteValues     []SQLiteValue     `msgpack:"values,omitempty"`
	Columns          []string          `msgpack:"columns,omitempty"`
	RowsAffected     int64             `msgpack:"rows_affected,omitempty"`
	LastInsertID     *int64            `msgpack:"last_insert_id,omitempty"`
	ChunkIndex       uint32            `msgpack:"chunk_index,omitempty"`
	Done             bool              `msgpack:"done,omitempty"`
	SQLiteRequestID  uint64            `msgpack:"request_id,omitempty"`
}

type DrainReport struct {
	Graceful        bool   `msgpack:"graceful"`
	ElapsedMS       uint64 `msgpack:"elapsed_ms"`
	ActorsStopped   uint32 `msgpack:"actors_stopped"`
	ActorsRemaining uint32 `msgpack:"actors_remaining"`
}

type WireError struct {
	Code           string  `msgpack:"code"`
	Message        string  `msgpack:"message"`
	SQLiteCode     *int32  `msgpack:"sqlite_code,omitempty"`
	StatementIndex *uint32 `msgpack:"statement_index,omitempty"`
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

type SQLiteValue struct {
	Kind    string  `msgpack:"kind"`
	Integer *int64  `msgpack:"integer,omitempty"`
	Bits    *uint64 `msgpack:"bits,omitempty"`
	Text    *string `msgpack:"text,omitempty"`
	Blob    *[]byte `msgpack:"blob,omitempty"`
}

type CommandBatch struct {
	Commands []Command `msgpack:"commands"`
}

// Command is the M5 command union. All fields are encoded so zero-length keys,
// values, state, and generation zero remain distinguishable from a missing
// required field. Rust ignores fields that do not belong to the selected kind.
type Command struct {
	Kind            CommandKind       `msgpack:"kind"`
	AID             string            `msgpack:"aid"`
	Generation      uint64            `msgpack:"gen"`
	OK              bool              `msgpack:"ok"`
	Error           *WireError        `msgpack:"error"`
	State           []byte            `msgpack:"state"`
	KVID            uint64            `msgpack:"kv_id"`
	Key             []byte            `msgpack:"key"`
	Prefix          []byte            `msgpack:"prefix"`
	Reverse         bool              `msgpack:"reverse"`
	Limit           *uint32           `msgpack:"limit"`
	Value           []byte            `msgpack:"value"`
	CallID          uint64            `msgpack:"call_id"`
	Output          []byte            `msgpack:"output"`
	RequestID       uint64            `msgpack:"req_id"`
	Status          uint16            `msgpack:"status"`
	Headers         map[string]string `msgpack:"headers"`
	Body            []byte            `msgpack:"body"`
	Stream          bool              `msgpack:"stream"`
	Finish          bool              `msgpack:"finish"`
	WSID            string            `msgpack:"ws_id"`
	Accept          bool              `msgpack:"accept"`
	Data            []byte            `msgpack:"data"`
	Binary          bool              `msgpack:"binary"`
	MessageIndex    uint16            `msgpack:"msg_index"`
	CloseCode       *uint16           `msgpack:"code"`
	CloseReason     *string           `msgpack:"reason"`
	Hibernate       bool              `msgpack:"hibernate"`
	Event           string            `msgpack:"event"`
	Payload         []byte            `msgpack:"payload"`
	ExcludeConn     *string           `msgpack:"exclude_conn"`
	AlarmTS         *int64            `msgpack:"alarm_ts"`
	OperationID     uint64            `msgpack:"op_id"`
	SQLiteRequestID uint64            `msgpack:"request_id"`
	SQL             string            `msgpack:"sql"`
	SQLArgs         []SQLiteValue     `msgpack:"args"`
	LeaseKey        *string           `msgpack:"lease_key"`
	DeadlineMS      uint32            `msgpack:"deadline_ms"`
	TimeoutMS       uint64            `msgpack:"timeout_ms"`
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
		if event.SQLiteSocketPath != "" && event.SQLiteSocketPath[0] != '/' {
			return fmt.Errorf("%s event has a non-absolute SQLite socket path", event.Kind)
		}
	case EventActorStop:
		if event.AID == "" || event.Reason == "" {
			return fmt.Errorf("%s event requires aid and reason", event.Kind)
		}
		if event.Reason != "sleep" && event.Reason != "stop" && event.Reason != "destroy" {
			return fmt.Errorf("%s event has unsupported reason %q", event.Kind, event.Reason)
		}
	case EventActorAlarm:
		if event.AID == "" {
			return fmt.Errorf("%s event has empty aid", event.Kind)
		}
	case EventActorIntentResult:
		if event.OperationID == 0 {
			return fmt.Errorf("%s event has invalid op_id", event.Kind)
		}
	case EventActionCall:
		if event.AID == "" || event.CallID == 0 || event.Action == "" || event.ActionTimeoutMS == 0 {
			return fmt.Errorf("%s event requires aid, call_id, action, and timeout_ms", event.Kind)
		}
	case EventHTTPRequest:
		if event.AID == "" || event.RequestID == 0 || event.Method == "" || event.Path == "" {
			return fmt.Errorf("%s event requires aid, req_id, method, and path", event.Kind)
		}
		if len(event.Headers) > 256 {
			return fmt.Errorf("%s event exceeds the 256-header schema limit", event.Kind)
		}
	case EventHTTPRequestChunk:
		if event.RequestID == 0 || (len(event.Body) == 0 && !event.Finish) {
			return fmt.Errorf("%s event requires req_id and body or finish", event.Kind)
		}
	case EventHTTPRequestAbort:
		if event.RequestID == 0 {
			return fmt.Errorf("%s event has invalid req_id", event.Kind)
		}
	case EventWSOpen:
		if event.AID == "" || event.WSID == "" || event.Path == "" {
			return fmt.Errorf("%s event requires aid, ws_id, and path", event.Kind)
		}
		if len(event.Headers) > 256 {
			return fmt.Errorf("%s event exceeds the 256-header schema limit", event.Kind)
		}
		if event.Resumed && !event.CanHibernate {
			return fmt.Errorf("%s resumed event is not hibernatable", event.Kind)
		}
	case EventWSMessage:
		if event.WSID == "" {
			return fmt.Errorf("%s event has empty ws_id", event.Kind)
		}
		if len(event.Data) > 1<<20 {
			return fmt.Errorf("%s event exceeds the one MiB message limit", event.Kind)
		}
		if !event.Binary && !utf8.Valid(event.Data) {
			return fmt.Errorf("%s text data is not valid UTF-8", event.Kind)
		}
	case EventWSClose:
		if event.WSID == "" {
			return fmt.Errorf("%s event has empty ws_id", event.Kind)
		}
		if event.CloseCode != nil && !validWebSocketCloseCode(*event.CloseCode) {
			return fmt.Errorf("%s event has invalid close code %d", event.Kind, *event.CloseCode)
		}
		if len(event.Reason) > 123 || !utf8.ValidString(event.Reason) {
			return fmt.Errorf("%s event has an invalid close reason", event.Kind)
		}
	case EventKVResult:
		if event.KVID == 0 {
			return fmt.Errorf("%s event has invalid kv_id", event.Kind)
		}
	case EventStatePersisted:
		if event.AID == "" {
			return fmt.Errorf("%s event has empty aid", event.Kind)
		}
	case EventSQLiteResult:
		if event.SQLiteRequestID == 0 {
			return fmt.Errorf("%s event has invalid request_id", event.Kind)
		}
		if event.Error != nil && (!event.Done || len(event.Columns) != 0 || len(event.SQLiteValues) != 0) {
			return fmt.Errorf("%s error event must be terminal and contain no rows", event.Kind)
		}
		for index, value := range event.SQLiteValues {
			if err := validateSQLiteValue(value); err != nil {
				return fmt.Errorf("%s value %d: %w", event.Kind, index, err)
			}
		}
	default:
		return fmt.Errorf("unknown event kind %q", event.Kind)
	}
	if event.Error != nil && (event.Error.Code == "" || event.Error.Message == "") {
		return fmt.Errorf("%s event has incomplete structured error", event.Kind)
	}
	return nil
}

func validateSQLiteValue(value SQLiteValue) error {
	set := 0
	if value.Integer != nil {
		set++
	}
	if value.Bits != nil {
		set++
	}
	if value.Text != nil {
		set++
		if len(*value.Text) > 1<<20 {
			return errors.New("text exceeds the one MiB value limit")
		}
	}
	if value.Blob != nil {
		set++
		if len(*value.Blob) > 1<<20 {
			return errors.New("blob exceeds the one MiB value limit")
		}
	}
	switch value.Kind {
	case "null":
		if set != 0 {
			return errors.New("null has a payload")
		}
	case "integer":
		if value.Integer == nil || set != 1 {
			return errors.New("integer requires exactly one integer payload")
		}
	case "real":
		if value.Bits == nil || set != 1 {
			return errors.New("real requires exactly one bits payload")
		}
	case "text":
		if value.Text == nil || set != 1 {
			return errors.New("text requires exactly one text payload")
		}
	case "blob":
		if value.Blob == nil || set != 1 {
			return errors.New("blob requires exactly one blob payload")
		}
	default:
		return fmt.Errorf("unknown SQLite value kind %q", value.Kind)
	}
	return nil
}

func validWebSocketCloseCode(code uint16) bool {
	return code >= 1000 && code <= 1003 || code >= 1007 && code <= 1014 || code >= 3000 && code <= 4999
}
