package wire

import (
	"crypto/sha256"
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
		EngineEndpoint:           "http://127.0.0.1:6420",
		Namespace:                "default",
		RunnerName:               "rivet-go-golden",
		Version:                  1,
		TotalSlots:               4,
		ActorNames:               []string{"counter"},
		ActorActions:             map[string][]string{"counter": {"increment"}},
		ActorHibernateWebSockets: map[string]bool{"counter": true},
		ActorDatabases:           map[string]bool{"counter": true},
		SQLiteTransport:          "ffi",
		LogLevel:                 "info",
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
		t.Fatalf(
			"Go RunnerConfig encoding differs from Rust golden: got len=%d sha256=%x, want len=%d sha256=%x",
			len(encoded), sha256.Sum256(encoded), len(data), sha256.Sum256(data),
		)
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
		{"event_actor_alarm.msgpack", EventActorAlarm, 17},
		{"event_actor_intent_result.msgpack", EventActorIntentResult, 18},
		{"event_actor_schedule_result.msgpack", EventActorScheduleResult, 20},
		{"event_actor_queue_result.msgpack", EventActorQueueResult, 24},
		{"event_connection_preflight.msgpack", EventConnectionPreflight, 21},
		{"event_connection_open.msgpack", EventConnectionOpen, 22},
		{"event_connection_close.msgpack", EventConnectionClose, 23},
		{"event_kv_result.msgpack", EventKVResult, 6},
		{"event_state_persisted.msgpack", EventStatePersisted, 7},
		{"event_action_call.msgpack", EventActionCall, 10},
		{"event_http_request.msgpack", EventHTTPRequest, 11},
		{"event_http_request_chunk.msgpack", EventHTTPRequestChunk, 12},
		{"event_http_request_abort.msgpack", EventHTTPRequestAbort, 13},
		{"event_ws_open.msgpack", EventWSOpen, 14},
		{"event_ws_message.msgpack", EventWSMessage, 15},
		{"event_ws_close.msgpack", EventWSClose, 16},
		{"event_sqlite_result.msgpack", EventSQLiteResult, 19},
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
			if test.kind == EventActionCall && batch.Events[0].ActionTimeoutMS != 60_000 {
				t.Fatalf("action timeout = %d ms, want 60000", batch.Events[0].ActionTimeoutMS)
			}
			if test.kind == EventWSOpen && !batch.Events[0].CanHibernate {
				t.Fatal("WebSocket open golden does not carry the M5 hibernation marker")
			}
			if test.kind == EventWSOpen && !batch.Events[0].Resumed {
				t.Fatal("WebSocket open golden does not carry the M5 resume marker")
			}
			if test.kind == EventActorStart &&
				(len(batch.Events[0].Connections) != 1 ||
					batch.Events[0].Connections[0].ID != "connection-restored" ||
					!batch.Events[0].Connections[0].Resumed) {
				t.Fatalf("actor start restored connections = %#v", batch.Events[0].Connections)
			}
			if (test.kind == EventConnectionPreflight || test.kind == EventConnectionOpen ||
				test.kind == EventConnectionClose) &&
				(batch.Events[0].Connection == nil || batch.Events[0].Connection.ID != "conn-golden") {
				t.Fatalf("connection lifecycle golden = %#v", batch.Events[0])
			}
			if test.kind == EventActorQueueResult &&
				(batch.Events[0].QueueOperation != "next" || batch.Events[0].QueueMessage == nil ||
					!batch.Events[0].QueueMessage.Completable) {
				t.Fatalf("queue result golden = %#v", batch.Events[0])
			}
		})
	}
}

func TestRustM11CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m11.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	want := []CommandKind{
		CommandActorRunResult,
		CommandQueueSend,
		CommandQueueEnqueueWait,
		CommandQueueNext,
		CommandQueueComplete,
		CommandQueueRetry,
		CommandQueueCancel,
		CommandManagedWorkBegin,
		CommandManagedWorkEnd,
	}
	if len(batch.Commands) != len(want) {
		t.Fatalf("command count = %d, want %d", len(batch.Commands), len(want))
	}
	for index, kind := range want {
		if batch.Commands[index].Kind != kind {
			t.Fatalf("command %d kind = %q, want %q", index, batch.Commands[index].Kind, kind)
		}
	}
	if batch.Commands[1].Name != "message" || batch.Commands[3].Names[0] != "message" ||
		!batch.Commands[3].Completable || batch.Commands[4].MessageID != 9 ||
		batch.Commands[7].WorkKind != "wait_until" {
		t.Fatalf("M11 queue and managed-work commands = %#v", batch.Commands)
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M11 golden")
	}
}

func TestRustM12CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m12.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Commands) != 1 || batch.Commands[0].Kind != CommandDestroyIntent ||
		batch.Commands[0].OperationID != 87 || batch.Commands[0].Generation != 12 {
		t.Fatalf("M12 destroy command = %#v", batch.Commands)
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M12 golden")
	}
}

func TestRustM10CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m10.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	want := []CommandKind{
		CommandConnectionResult,
		CommandActionResult,
	}
	if len(batch.Commands) != len(want) {
		t.Fatalf("command count = %d, want %d", len(batch.Commands), len(want))
	}
	for index, kind := range want {
		if batch.Commands[index].Kind != kind {
			t.Fatalf("command %d kind = %q, want %q", index, batch.Commands[index].Kind, kind)
		}
	}
	if batch.Commands[0].OperationID != 71 || batch.Commands[0].ConnectionState == nil {
		t.Fatalf("connection result = %#v", batch.Commands[0])
	}
	if batch.Commands[1].CallID != 74 || batch.Commands[1].ConnectionState == nil {
		t.Fatalf("connected action result = %#v", batch.Commands[1])
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M10 golden")
	}
}

func TestRustM9CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m9.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	want := []CommandKind{
		CommandScheduleAfter,
		CommandScheduleAt,
		CommandScheduleCancel,
		CommandScheduleGet,
		CommandScheduleList,
	}
	if len(batch.Commands) != len(want) {
		t.Fatalf("command count = %d, want %d", len(batch.Commands), len(want))
	}
	for index, kind := range want {
		if batch.Commands[index].Kind != kind {
			t.Fatalf("command %d kind = %q, want %q", index, batch.Commands[index].Kind, kind)
		}
	}
	if batch.Commands[0].OperationID != 61 || batch.Commands[0].Generation != 9 ||
		batch.Commands[0].DelayMS != 1_500 || batch.Commands[0].Action != "remind" {
		t.Fatalf("schedule after command = %#v", batch.Commands[0])
	}
	if batch.Commands[1].RunAt != 1_788_500_000_000 || batch.Commands[2].ScheduleID != "schedule-golden" {
		t.Fatalf("M9 schedule commands = %#v", batch.Commands)
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M9 golden")
	}
}

func TestRustM7CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m7.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	want := []CommandKind{
		CommandSQLiteExec,
		CommandSQLiteQuery,
		CommandSQLiteBegin,
		CommandSQLiteCommit,
		CommandSQLiteRollback,
	}
	if len(batch.Commands) != len(want) {
		t.Fatalf("command count = %d, want %d", len(batch.Commands), len(want))
	}
	for index, kind := range want {
		if batch.Commands[index].Kind != kind {
			t.Fatalf("command %d kind = %q, want %q", index, batch.Commands[index].Kind, kind)
		}
	}
	if batch.Commands[0].SQLiteRequestID != 51 || batch.Commands[0].SQLArgs[0].Text == nil {
		t.Fatalf("unexpected SQLite exec command: %#v", batch.Commands[0])
	}
	if batch.Commands[2].LeaseKey == nil || batch.Commands[2].TimeoutMS != 60_000 {
		t.Fatalf("unexpected SQLite begin command: %#v", batch.Commands[2])
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M7 golden")
	}
}

func TestRustM5CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m5.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	want := []CommandKind{
		CommandAlarmHandled,
		CommandSetAlarm,
		CommandSetAlarm,
		CommandSleepIntent,
	}
	if len(batch.Commands) != len(want) {
		t.Fatalf("command count = %d, want %d", len(batch.Commands), len(want))
	}
	for index, kind := range want {
		if batch.Commands[index].Kind != kind {
			t.Fatalf("command %d kind = %q, want %q", index, batch.Commands[index].Kind, kind)
		}
	}
	if batch.Commands[0].Generation != 8 {
		t.Fatalf("alarm handled generation = %d, want 8", batch.Commands[0].Generation)
	}
	if batch.Commands[1].OperationID != 41 || batch.Commands[1].Generation != 8 {
		t.Fatalf("set alarm identity = %#v", batch.Commands[1])
	}
	if batch.Commands[1].AlarmTS == nil || *batch.Commands[1].AlarmTS != 1_788_500_000_000 {
		t.Fatalf("set alarm command = %#v", batch.Commands[1])
	}
	if batch.Commands[2].AlarmTS != nil {
		t.Fatalf("clear alarm command = %#v", batch.Commands[2])
	}
	if batch.Commands[2].OperationID != 42 || batch.Commands[3].OperationID != 43 ||
		batch.Commands[3].Generation != 8 {
		t.Fatalf("M5 intent identities = %#v", batch.Commands)
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M5 golden")
	}
}

func TestRustM4CommandBatchGolden(t *testing.T) {
	data := golden(t, "command_m4.msgpack")
	var batch CommandBatch
	if err := decode(data, &batch); err != nil {
		t.Fatal(err)
	}
	want := []CommandKind{
		CommandWSOpenResult,
		CommandWSMessageAck,
		CommandWSSend,
		CommandWSClose,
		CommandBroadcast,
		CommandStopIntent,
	}
	if len(batch.Commands) != len(want) {
		t.Fatalf("command count = %d, want %d", len(batch.Commands), len(want))
	}
	for index, kind := range want {
		if batch.Commands[index].Kind != kind {
			t.Fatalf("command %d kind = %q, want %q", index, batch.Commands[index].Kind, kind)
		}
	}
	if batch.Commands[0].WSID != "ws-golden" || !batch.Commands[0].Accept {
		t.Fatalf("unexpected WebSocket open result: %#v", batch.Commands[0])
	}
	if batch.Commands[1].MessageIndex != 3 {
		t.Fatalf("unexpected WebSocket message acknowledgement: %#v", batch.Commands[1])
	}
	if string(batch.Commands[2].Data) != "targeted" || batch.Commands[2].Binary {
		t.Fatalf("unexpected targeted WebSocket send: %#v", batch.Commands[2])
	}
	if batch.Commands[3].CloseCode == nil || *batch.Commands[3].CloseCode != 1000 {
		t.Fatalf("unexpected WebSocket close command: %#v", batch.Commands[3])
	}
	if !batch.Commands[3].Hibernate {
		t.Fatal("WebSocket close golden does not carry the M5 hibernation marker")
	}
	if batch.Commands[4].Generation != 7 || batch.Commands[4].Event != "countChanged" ||
		batch.Commands[4].ExcludeConn == nil {
		t.Fatalf("unexpected broadcast command: %#v", batch.Commands[4])
	}
	encoded, err := EncodeCommandBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded, data) {
		t.Fatal("Go CommandBatch encoding differs from the Rust-generated M4 golden")
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
	for index := 3; index <= 6; index++ {
		if batch.Commands[index].Generation != 7 {
			t.Fatalf("KV command %d generation = %d, want 7", index, batch.Commands[index].Generation)
		}
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
		t.Fatalf(
			"Go CommandBatch encoding differs from Rust golden: got len=%d sha256=%x, want len=%d sha256=%x",
			len(encoded), sha256.Sum256(encoded), len(data), sha256.Sum256(data),
		)
	}
}

func TestScheduleResultValidationRejectsInconsistentPayloads(t *testing.T) {
	cancelled := true
	schedule := ScheduledEvent{ID: "one", Action: "run", Args: []byte{0x81, 0x01}, RunAt: 1}
	tests := []Event{
		{Kind: EventActorScheduleResult, OperationID: 1, ScheduleOperation: "create"},
		{Kind: EventActorScheduleResult, OperationID: 1, ScheduleOperation: "cancel"},
		{
			Kind: EventActorScheduleResult, OperationID: 1, ScheduleOperation: "get",
			Schedules: []ScheduledEvent{schedule, schedule},
		},
		{
			Kind: EventActorScheduleResult, OperationID: 1, ScheduleOperation: "list",
			Cancelled: &cancelled,
		},
		{
			Kind: EventActorScheduleResult, OperationID: 1, ScheduleOperation: "create",
			ScheduleID: "one", Error: &WireError{Code: "failed", Message: "failed"},
		},
	}
	for index, event := range tests {
		data, err := encode(EventBatch{Seq: uint64(index + 1), Events: []Event{event}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeEventBatch(data); err == nil {
			t.Fatalf("invalid schedule result %d was accepted: %#v", index, event)
		}
	}
}

func TestConnectionEventValidationRejectsRawAndOversizedSnapshots(t *testing.T) {
	valid := Connection{ID: "connection", ActorConnect: true}
	tests := []Event{
		{Kind: EventConnectionOpen, AID: "actor", OperationID: 1, Connection: &Connection{ID: "raw"}},
		{
			Kind: EventConnectionOpen, AID: "actor", OperationID: 1,
			Connection: &Connection{ID: "connection", ActorConnect: true, State: make([]byte, 1<<20+1)},
		},
		{
			Kind: EventActorStart, AID: "actor", Name: "kind",
			Connections: make([]Connection, 1_025),
		},
	}
	for index, event := range tests {
		if event.Kind == EventActorStart {
			for connectionIndex := range event.Connections {
				event.Connections[connectionIndex] = valid
			}
		}
		data, err := encode(EventBatch{Seq: uint64(index + 1), Events: []Event{event}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeEventBatch(data); err == nil {
			t.Fatalf("invalid connection event %d was accepted", index)
		}
	}
}

func decode(data []byte, value any) error {
	return msgpack.Unmarshal(data, value)
}
