//! Versioned-by-ABI MessagePack values shared with Go's `internal/wire` package.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

const MAX_BODY_CHUNK: usize = 1 << 20;

#[derive(Serialize)]
struct ActorConnectEnvelope<'a> {
    body: ActorConnectBody<'a>,
}

#[derive(Serialize)]
struct ActorConnectBody<'a> {
    tag: &'static str,
    val: ActorConnectEvent<'a>,
}

#[derive(Serialize)]
struct ActorConnectEvent<'a> {
    name: &'a str,
    args: ciborium::Value,
}

/// Encodes the pinned client's CBOR event envelope. This mirrors
/// rivetkit-core's ActorConnectToClient::Event shape:
/// `{body: {tag: "Event", val: {name, args}}}`.
pub(crate) fn encode_actor_connect_event_frame(name: &str, args: &[u8]) -> Result<Vec<u8>, String> {
    let args: ciborium::Value = ciborium::from_reader(args)
        .map_err(|error| format!("broadcast payload must be CBOR: {error}"))?;
    if !matches!(args, ciborium::Value::Array(_)) {
        return Err("broadcast payload must be a CBOR argument array".to_owned());
    }
    let mut frame = Vec::new();
    ciborium::into_writer(
        &ActorConnectEnvelope {
            body: ActorConnectBody {
                tag: "Event",
                val: ActorConnectEvent { name, args },
            },
        },
        &mut frame,
    )
    .map_err(|error| format!("encode broadcast event frame: {error}"))?;
    if frame.len() > MAX_BODY_CHUNK {
        return Err(format!(
            "broadcast event frame exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
        ));
    }
    Ok(frame)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunnerConfig {
    pub engine_endpoint: String,
    pub namespace: String,
    pub runner_name: String,
    pub version: u32,
    pub total_slots: u32,
    #[serde(default)]
    pub actor_names: Vec<String>,
    #[serde(default)]
    pub actor_actions: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub actor_hibernate_websockets: BTreeMap<String, bool>,
    #[serde(default)]
    pub actor_databases: BTreeMap<String, bool>,
    #[serde(default)]
    pub sqlite_transport: String,
    pub log_level: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct EventBatch {
    pub seq: u64,
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)] // Names are fixed by FFI-BOUNDARY.md.
pub(crate) enum Event {
    RunnerConnected {
        runner_id: String,
        metadata: BTreeMap<String, String>,
    },
    RunnerDisconnected {
        reason: String,
    },
    RunnerStopped {
        drain_report: DrainReport,
    },
    ActorStart {
        aid: String,
        r#gen: u64,
        name: String,
        key: String,
        create_ts: i64,
        #[serde(with = "serde_bytes")]
        input: Vec<u8>,
        #[serde(default, with = "optional_bytes")]
        persisted_state: Option<Vec<u8>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sqlite_socket_path: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        connections: Vec<Connection>,
    },
    ActorStop {
        aid: String,
        r#gen: u64,
        reason: String,
    },
    ActorAlarm {
        aid: String,
        r#gen: u64,
        alarm_ts: i64,
    },
    ActorIntentResult {
        op_id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    ActorScheduleResult {
        op_id: u64,
        operation: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schedule_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cancelled: Option<bool>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        schedules: Vec<ScheduledEvent>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    ActorQueueResult {
        op_id: u64,
        queue_operation: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<QueueMessage>,
        #[serde(
            default,
            with = "optional_bytes",
            skip_serializing_if = "Option::is_none"
        )]
        response: Option<Vec<u8>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    ConnectionPreflight {
        aid: String,
        r#gen: u64,
        op_id: u64,
        connection: Connection,
    },
    ConnectionOpen {
        aid: String,
        r#gen: u64,
        op_id: u64,
        connection: Connection,
    },
    ConnectionClose {
        aid: String,
        r#gen: u64,
        op_id: u64,
        connection: Connection,
    },
    ActionCall {
        aid: String,
        r#gen: u64,
        call_id: u64,
        action: String,
        timeout_ms: u32,
        #[serde(with = "serde_bytes")]
        args: Vec<u8>,
        conn_id: Option<String>,
    },
    HttpRequest {
        aid: String,
        r#gen: u64,
        req_id: u64,
        method: String,
        path: String,
        headers: BTreeMap<String, String>,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
        stream: bool,
    },
    HttpRequestChunk {
        req_id: u64,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
        finish: bool,
    },
    HttpRequestAbort {
        req_id: u64,
    },
    WsOpen {
        aid: String,
        ws_id: String,
        path: String,
        headers: BTreeMap<String, String>,
        can_hibernate: bool,
        #[serde(default)]
        resumed: bool,
    },
    WsMessage {
        ws_id: String,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
        binary: bool,
        msg_index: u16,
    },
    WsClose {
        ws_id: String,
        code: Option<u16>,
        reason: Option<String>,
    },
    KvResult {
        kv_id: u64,
        #[serde(
            default,
            with = "optional_bytes",
            skip_serializing_if = "Option::is_none"
        )]
        value: Option<Vec<u8>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        entries: Vec<KvEntry>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    StatePersisted {
        aid: String,
        r#gen: u64,
        state_version: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    SqliteResult {
        request_id: u64,
        chunk_index: u32,
        done: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        columns: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        values: Vec<SqliteValue>,
        rows_affected: i64,
        last_insert_id: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct Connection {
    pub id: String,
    #[serde(with = "serde_bytes")]
    pub parameters: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub state: Vec<u8>,
    pub path: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub can_hibernate: bool,
    pub resumed: bool,
    pub actor_connect: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct DrainReport {
    pub graceful: bool,
    pub elapsed_ms: u64,
    pub actors_stopped: u32,
    pub actors_remaining: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct WireError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlite_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_index: Option<u32>,
}

impl WireError {
    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            sqlite_code: None,
            statement_index: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum SqliteValue {
    Null,
    Integer {
        integer: i64,
    },
    Real {
        bits: u64,
    },
    Text {
        text: String,
    },
    Blob {
        #[serde(with = "serde_bytes")]
        blob: Vec<u8>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct KvEntry {
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct ScheduledEvent {
    pub id: String,
    pub action: String,
    #[serde(with = "serde_bytes")]
    pub args: Vec<u8>,
    pub run_at: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct QueueMessage {
    pub id: u64,
    pub name: String,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
    pub created_at: i64,
    pub completable: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommandBatch {
    pub commands: Vec<Command>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Command {
    ActorStartResult {
        aid: String,
        #[serde(default)]
        r#gen: u64,
        ok: bool,
        #[serde(default)]
        run: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    ActorStopResult {
        aid: String,
        #[serde(default)]
        r#gen: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    ActorRunResult {
        aid: String,
        #[serde(default)]
        r#gen: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    AlarmHandled {
        aid: String,
        #[serde(default)]
        r#gen: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    ActionResult {
        call_id: u64,
        #[serde(default, with = "optional_bytes")]
        output: Option<Vec<u8>>,
        #[serde(default, with = "optional_bytes")]
        connection_state: Option<Vec<u8>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    ConnectionResult {
        op_id: u64,
        #[serde(default, with = "optional_bytes")]
        connection_state: Option<Vec<u8>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    HttpResponseStart {
        req_id: u64,
        status: u16,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default, with = "serde_bytes")]
        body: Vec<u8>,
        #[serde(default)]
        stream: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    HttpResponseChunk {
        req_id: u64,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
        finish: bool,
    },
    WsOpenResult {
        ws_id: String,
        accept: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    WsMessageAck {
        ws_id: String,
        msg_index: u16,
    },
    WsSend {
        ws_id: String,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
        binary: bool,
    },
    WsCloseCmd {
        ws_id: String,
        code: Option<u16>,
        reason: Option<String>,
        #[serde(default)]
        hibernate: bool,
    },
    Broadcast {
        aid: String,
        event: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        exclude_conn: Option<String>,
    },
    StopIntent {
        aid: String,
    },
    SetAlarm {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        alarm_ts: Option<i64>,
    },
    SleepIntent {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
    },
    ScheduleAfter {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        delay_ms: u64,
        action: String,
        #[serde(with = "serde_bytes")]
        schedule_args: Vec<u8>,
    },
    ScheduleAt {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        run_at: i64,
        action: String,
        #[serde(with = "serde_bytes")]
        schedule_args: Vec<u8>,
    },
    ScheduleCancel {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        schedule_id: String,
    },
    ScheduleGet {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        schedule_id: String,
    },
    ScheduleList {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
    },
    QueueSend {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        name: String,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
    },
    QueueEnqueueWait {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        name: String,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
        queue_timeout_ms: Option<u64>,
    },
    QueueNext {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        #[serde(default)]
        names: Vec<String>,
        queue_timeout_ms: Option<u64>,
        completable: bool,
    },
    QueueComplete {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        message_id: u64,
        #[serde(default, with = "optional_bytes")]
        response: Option<Vec<u8>>,
    },
    QueueRetry {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        message_id: u64,
    },
    QueueCancel {
        aid: String,
        #[serde(default)]
        r#gen: u64,
        target_op_id: u64,
    },
    ManagedWorkBegin {
        op_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        work_id: u64,
        work_kind: String,
    },
    ManagedWorkEnd {
        aid: String,
        #[serde(default)]
        r#gen: u64,
        work_id: u64,
    },
    SaveState {
        aid: String,
        #[serde(default)]
        r#gen: u64,
        #[serde(with = "serde_bytes")]
        state: Vec<u8>,
    },
    KvGet {
        kv_id: u64,
        aid: String,
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
    },
    KvList {
        kv_id: u64,
        aid: String,
        #[serde(with = "serde_bytes")]
        prefix: Vec<u8>,
        #[serde(default)]
        reverse: bool,
        #[serde(default)]
        limit: Option<u32>,
    },
    KvPut {
        kv_id: u64,
        aid: String,
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    KvDelete {
        kv_id: u64,
        aid: String,
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
    },
    SqliteExec {
        request_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        sql: String,
        #[serde(default)]
        args: Vec<SqliteValue>,
        lease_key: Option<String>,
        deadline_ms: u32,
    },
    SqliteQuery {
        request_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        sql: String,
        #[serde(default)]
        args: Vec<SqliteValue>,
        lease_key: Option<String>,
        deadline_ms: u32,
    },
    SqliteBegin {
        request_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        lease_key: String,
        timeout_ms: u64,
        deadline_ms: u32,
    },
    SqliteCommit {
        request_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        lease_key: String,
        deadline_ms: u32,
    },
    SqliteRollback {
        request_id: u64,
        aid: String,
        #[serde(default)]
        r#gen: u64,
        lease_key: String,
        deadline_ms: u32,
    },
    #[serde(other)]
    Unknown,
}

pub(crate) mod optional_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_bytes::{ByteBuf, Bytes};

    pub(crate) fn serialize<S>(value: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.as_deref().map(Bytes::new).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<ByteBuf>::deserialize(deserializer).map(|value| value.map(ByteBuf::into_vec))
    }
}

impl EventBatch {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec_named(self)
    }
}

impl CommandBatch {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }

    pub(crate) fn contains_unknown(&self) -> bool {
        self.commands
            .iter()
            .any(|command| matches!(command, Command::Unknown))
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        const MAX_KV_LIST_ENTRIES: u32 = 1_024;
        const MAX_HTTP_HEADERS: usize = 256;
        for command in &self.commands {
            match command {
                Command::ActorStartResult {
                    aid,
                    ok,
                    run,
                    error,
                    ..
                } => {
                    require_aid(aid)?;
                    if *ok == error.is_some() {
                        return Err(
                            "actor_start_result must contain exactly one of ok=true or error"
                                .to_owned(),
                        );
                    }
                    if !*ok && *run {
                        return Err(
                            "actor_start_result cannot start Run after startup failure".to_owned()
                        );
                    }
                    require_wire_error(error.as_ref())?;
                }
                Command::ActorStopResult { aid, error, .. } => {
                    require_aid(aid)?;
                    require_wire_error(error.as_ref())?;
                }
                Command::ActorRunResult { aid, error, .. } => {
                    require_aid(aid)?;
                    require_wire_error(error.as_ref())?;
                }
                Command::AlarmHandled { aid, error, .. } => {
                    require_aid(aid)?;
                    require_wire_error(error.as_ref())?;
                }
                Command::SaveState { aid, .. } => {
                    require_aid(aid)?;
                }
                Command::ActionResult {
                    call_id,
                    output,
                    connection_state,
                    error,
                } => {
                    require_correlation("action_result", *call_id)?;
                    if output.is_some() == error.is_some() {
                        return Err(
                            "action_result must contain exactly one of output or error".to_owned()
                        );
                    }
                    if error.is_some() && connection_state.is_some() {
                        return Err(
                            "action_result error must not contain connection state".to_owned()
                        );
                    }
                    require_wire_error(error.as_ref())?;
                    if output
                        .as_ref()
                        .is_some_and(|output| output.len() > MAX_BODY_CHUNK)
                    {
                        return Err(format!(
                            "action_result output exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                        ));
                    }
                    require_connection_state(connection_state.as_deref())?;
                }
                Command::ConnectionResult {
                    op_id,
                    connection_state,
                    error,
                } => {
                    require_correlation("connection_result", *op_id)?;
                    if connection_state.is_some() == error.is_some() {
                        return Err(
                            "connection_result must contain exactly one of state or error"
                                .to_owned(),
                        );
                    }
                    require_connection_state(connection_state.as_deref())?;
                    require_wire_error(error.as_ref())?;
                }
                Command::HttpResponseStart {
                    req_id,
                    status,
                    headers,
                    body,
                    error,
                    ..
                } => {
                    require_correlation("http_response_start", *req_id)?;
                    require_wire_error(error.as_ref())?;
                    if error.is_none() && !(100..=999).contains(status) {
                        return Err(
                            "http_response_start status must be between 100 and 999".to_owned()
                        );
                    }
                    if headers.len() > MAX_HTTP_HEADERS {
                        return Err(format!(
                            "http_response_start headers exceed boundary maximum {MAX_HTTP_HEADERS}"
                        ));
                    }
                    if headers.iter().any(|(name, value)| {
                        name.len() > MAX_BODY_CHUNK || value.len() > MAX_BODY_CHUNK
                    }) {
                        return Err(format!(
                            "http_response_start header exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                        ));
                    }
                    if body.len() > MAX_BODY_CHUNK {
                        return Err(format!(
                            "http_response_start body exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                        ));
                    }
                }
                Command::HttpResponseChunk {
                    req_id,
                    body,
                    finish,
                } => {
                    require_correlation("http_response_chunk", *req_id)?;
                    if body.len() > MAX_BODY_CHUNK {
                        return Err(format!(
                            "http_response_chunk body exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                        ));
                    }
                    if body.is_empty() && !finish {
                        return Err(
                            "http_response_chunk must contain body bytes or finish=true".to_owned()
                        );
                    }
                }
                Command::WsOpenResult {
                    ws_id,
                    accept,
                    error,
                } => {
                    require_ws_id(ws_id)?;
                    if *accept == error.is_some() {
                        return Err(
                            "ws_open_result must contain exactly one of accept=true or error"
                                .to_owned(),
                        );
                    }
                    require_wire_error(error.as_ref())?;
                }
                Command::WsMessageAck { ws_id, .. } => require_ws_id(ws_id)?,
                Command::WsSend {
                    ws_id,
                    data,
                    binary,
                } => {
                    require_ws_id(ws_id)?;
                    require_ws_data("ws_send", data, *binary, MAX_BODY_CHUNK)?;
                }
                Command::WsCloseCmd {
                    ws_id,
                    code,
                    reason,
                    ..
                } => {
                    require_ws_id(ws_id)?;
                    if code.is_some_and(|code| !valid_close_code(code)) {
                        return Err("ws_close_cmd contains an invalid close code".to_owned());
                    }
                    if reason.as_ref().is_some_and(|reason| reason.len() > 123) {
                        return Err(
                            "ws_close_cmd reason exceeds the WebSocket 123-byte limit".to_owned()
                        );
                    }
                }
                Command::Broadcast {
                    aid,
                    event,
                    payload,
                    exclude_conn,
                } => {
                    require_aid(aid)?;
                    if event.trim().is_empty() {
                        return Err("broadcast event must not be empty".to_owned());
                    }
                    if payload.len() > MAX_BODY_CHUNK {
                        return Err(format!(
                            "broadcast payload exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                        ));
                    }
                    encode_actor_connect_event_frame(event, payload)?;
                    if exclude_conn.as_ref().is_some_and(String::is_empty) {
                        return Err("broadcast exclude_conn must not be empty".to_owned());
                    }
                }
                Command::StopIntent { aid } => require_aid(aid)?,
                Command::SetAlarm { op_id, aid, .. } | Command::SleepIntent { op_id, aid, .. } => {
                    require_aid(aid)?;
                    if *op_id == 0 {
                        return Err("actor intent op_id must not be zero".to_owned());
                    }
                }
                Command::ScheduleAfter {
                    op_id,
                    aid,
                    action,
                    schedule_args,
                    ..
                }
                | Command::ScheduleAt {
                    op_id,
                    aid,
                    action,
                    schedule_args,
                    ..
                } => {
                    require_schedule_operation(*op_id, aid)?;
                    if action.trim().is_empty() {
                        return Err("scheduled action must not be empty".to_owned());
                    }
                    if action == "__rivet_go_alarm" {
                        return Err("scheduled action name is reserved".to_owned());
                    }
                    if action.len() > MAX_BODY_CHUNK || schedule_args.len() > MAX_BODY_CHUNK {
                        return Err(format!(
                            "scheduled action and args must not exceed {MAX_BODY_CHUNK} bytes"
                        ));
                    }
                    let args: ciborium::Value = ciborium::from_reader(schedule_args.as_slice())
                        .map_err(|error| format!("scheduled args must be CBOR: {error}"))?;
                    if !matches!(args, ciborium::Value::Array(_)) {
                        return Err("scheduled args must be a CBOR argument array".to_owned());
                    }
                }
                Command::ScheduleCancel {
                    op_id,
                    aid,
                    schedule_id,
                    ..
                }
                | Command::ScheduleGet {
                    op_id,
                    aid,
                    schedule_id,
                    ..
                } => {
                    require_schedule_operation(*op_id, aid)?;
                    if schedule_id.is_empty() || schedule_id.len() > MAX_BODY_CHUNK {
                        return Err("schedule_id must be non-empty and at most one MiB".to_owned());
                    }
                }
                Command::ScheduleList { op_id, aid, .. } => {
                    require_schedule_operation(*op_id, aid)?;
                }
                Command::QueueSend {
                    op_id,
                    aid,
                    name,
                    body,
                    ..
                }
                | Command::QueueEnqueueWait {
                    op_id,
                    aid,
                    name,
                    body,
                    ..
                } => {
                    require_queue_operation(*op_id, aid)?;
                    require_queue_name(name)?;
                    if body.len() > MAX_BODY_CHUNK {
                        return Err(format!(
                            "queue body exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                        ));
                    }
                }
                Command::QueueNext {
                    op_id, aid, names, ..
                } => {
                    require_queue_operation(*op_id, aid)?;
                    if names.len() > 1_024 {
                        return Err("queue next names exceed boundary maximum 1024".to_owned());
                    }
                    for name in names {
                        require_queue_name(name)?;
                    }
                }
                Command::QueueComplete {
                    op_id,
                    aid,
                    message_id,
                    response,
                    ..
                } => {
                    require_queue_operation(*op_id, aid)?;
                    if *message_id == 0 {
                        return Err("queue complete message_id must not be zero".to_owned());
                    }
                    if response
                        .as_ref()
                        .is_some_and(|response| response.len() > MAX_BODY_CHUNK)
                    {
                        return Err(format!(
                            "queue response exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                        ));
                    }
                }
                Command::QueueRetry {
                    op_id,
                    aid,
                    message_id,
                    ..
                } => {
                    require_queue_operation(*op_id, aid)?;
                    if *message_id == 0 {
                        return Err("queue retry message_id must not be zero".to_owned());
                    }
                }
                Command::QueueCancel {
                    aid, target_op_id, ..
                } => {
                    require_aid(aid)?;
                    require_correlation("queue cancel", *target_op_id)?;
                }
                Command::ManagedWorkBegin {
                    op_id,
                    aid,
                    work_id,
                    work_kind,
                    ..
                } => {
                    require_correlation("managed work", *op_id)?;
                    require_aid(aid)?;
                    if *work_id == 0 {
                        return Err("managed work ID must not be zero".to_owned());
                    }
                    if !matches!(work_kind.as_str(), "wait_until" | "keep_awake") {
                        return Err("managed work kind is invalid".to_owned());
                    }
                }
                Command::ManagedWorkEnd { aid, work_id, .. } => {
                    require_aid(aid)?;
                    if *work_id == 0 {
                        return Err("managed work ID must not be zero".to_owned());
                    }
                }
                Command::KvGet { kv_id, aid, .. }
                | Command::KvPut { kv_id, aid, .. }
                | Command::KvDelete { kv_id, aid, .. } => {
                    require_kv(*kv_id, aid)?;
                }
                Command::KvList {
                    kv_id, aid, limit, ..
                } => {
                    require_kv(*kv_id, aid)?;
                    if limit.is_some_and(|limit| limit > MAX_KV_LIST_ENTRIES) {
                        return Err(format!(
                            "kv_list limit exceeds boundary maximum {MAX_KV_LIST_ENTRIES}"
                        ));
                    }
                }
                Command::SqliteExec {
                    request_id,
                    aid,
                    sql,
                    args,
                    lease_key,
                    deadline_ms,
                    ..
                }
                | Command::SqliteQuery {
                    request_id,
                    aid,
                    sql,
                    args,
                    lease_key,
                    deadline_ms,
                    ..
                } => {
                    require_sqlite_request(*request_id, aid, *deadline_ms)?;
                    require_sql(sql, args)?;
                    require_optional_lease(lease_key.as_deref())?;
                }
                Command::SqliteBegin {
                    request_id,
                    aid,
                    lease_key,
                    timeout_ms,
                    deadline_ms,
                    ..
                } => {
                    require_sqlite_request(*request_id, aid, *deadline_ms)?;
                    require_lease(lease_key)?;
                    if *timeout_ms == 0 {
                        return Err("sqlite_begin timeout_ms must be greater than zero".to_owned());
                    }
                }
                Command::SqliteCommit {
                    request_id,
                    aid,
                    lease_key,
                    deadline_ms,
                    ..
                }
                | Command::SqliteRollback {
                    request_id,
                    aid,
                    lease_key,
                    deadline_ms,
                    ..
                } => {
                    require_sqlite_request(*request_id, aid, *deadline_ms)?;
                    require_lease(lease_key)?;
                }
                Command::Unknown => return Err("unknown command".to_owned()),
            }
        }
        Ok(())
    }
}

fn require_connection_state(state: Option<&[u8]>) -> Result<(), String> {
    if state.is_some_and(|state| state.len() > MAX_BODY_CHUNK) {
        return Err(format!(
            "connection state exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
        ));
    }
    Ok(())
}

fn require_queue_operation(op_id: u64, aid: &str) -> Result<(), String> {
    require_aid(aid)?;
    require_correlation("queue", op_id)
}

fn require_queue_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() || name.len() > MAX_BODY_CHUNK {
        return Err("queue name must be non-empty and at most one MiB".to_owned());
    }
    Ok(())
}

fn require_sqlite_request(request_id: u64, aid: &str, deadline_ms: u32) -> Result<(), String> {
    require_aid(aid)?;
    require_correlation("sqlite", request_id)?;
    if deadline_ms == 0 {
        return Err("sqlite request deadline_ms must be greater than zero".to_owned());
    }
    Ok(())
}

fn require_schedule_operation(op_id: u64, aid: &str) -> Result<(), String> {
    require_aid(aid)?;
    require_correlation("schedule", op_id)
}

fn require_sql(sql: &str, args: &[SqliteValue]) -> Result<(), String> {
    if sql.trim().is_empty() {
        return Err("sqlite SQL must not be empty".to_owned());
    }
    if sql.len() > MAX_BODY_CHUNK {
        return Err(format!(
            "sqlite SQL exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
        ));
    }
    if args.len() > 1_024 {
        return Err("sqlite args exceed boundary maximum 1024".to_owned());
    }
    for value in args {
        match value {
            SqliteValue::Text { text } if text.len() > MAX_BODY_CHUNK => {
                return Err(format!(
                    "sqlite text value exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                ));
            }
            SqliteValue::Blob { blob } if blob.len() > MAX_BODY_CHUNK => {
                return Err(format!(
                    "sqlite blob value exceeds boundary maximum {MAX_BODY_CHUNK} bytes"
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn require_optional_lease(lease_key: Option<&str>) -> Result<(), String> {
    if let Some(lease_key) = lease_key {
        require_lease(lease_key)?;
    }
    Ok(())
}

fn require_lease(lease_key: &str) -> Result<(), String> {
    if lease_key.is_empty() {
        return Err("sqlite lease_key must not be empty".to_owned());
    }
    if lease_key.len() > 256 {
        return Err("sqlite lease_key exceeds boundary maximum 256 bytes".to_owned());
    }
    Ok(())
}

fn require_aid(aid: &str) -> Result<(), String> {
    if aid.is_empty() {
        Err("actor command aid must not be empty".to_owned())
    } else {
        Ok(())
    }
}

fn require_ws_id(ws_id: &str) -> Result<(), String> {
    if ws_id.is_empty() {
        Err("WebSocket command ws_id must not be empty".to_owned())
    } else {
        Ok(())
    }
}

fn require_ws_data(kind: &str, data: &[u8], binary: bool, maximum: usize) -> Result<(), String> {
    if data.len() > maximum {
        return Err(format!(
            "{kind} data exceeds boundary maximum {maximum} bytes"
        ));
    }
    if !binary && std::str::from_utf8(data).is_err() {
        return Err(format!("{kind} text data must be valid UTF-8"));
    }
    Ok(())
}

fn valid_close_code(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
}

fn require_kv(kv_id: u64, aid: &str) -> Result<(), String> {
    require_aid(aid)?;
    if kv_id == 0 {
        Err("KV command kv_id must be greater than zero".to_owned())
    } else {
        Ok(())
    }
}

fn require_correlation(kind: &str, id: u64) -> Result<(), String> {
    if id == 0 {
        Err(format!("{kind} correlation ID must be greater than zero"))
    } else {
        Ok(())
    }
}

fn require_wire_error(error: Option<&WireError>) -> Result<(), String> {
    if error.is_some_and(|error| error.code.is_empty() || error.message.is_empty()) {
        Err("structured errors require non-empty code and message".to_owned())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

    #[derive(Serialize)]
    struct GoldenCommand {
        kind: &'static str,
        aid: &'static str,
        r#gen: u64,
        ok: bool,
        run: bool,
        error: Option<WireError>,
        #[serde(with = "optional_bytes")]
        state: Option<Vec<u8>>,
        kv_id: u64,
        #[serde(with = "optional_bytes")]
        key: Option<Vec<u8>>,
        #[serde(with = "optional_bytes")]
        prefix: Option<Vec<u8>>,
        reverse: bool,
        limit: Option<u32>,
        #[serde(with = "optional_bytes")]
        value: Option<Vec<u8>>,
        call_id: u64,
        #[serde(with = "optional_bytes")]
        output: Option<Vec<u8>>,
        #[serde(with = "optional_bytes")]
        connection_state: Option<Vec<u8>>,
        req_id: u64,
        status: u16,
        headers: Option<BTreeMap<String, String>>,
        #[serde(with = "optional_bytes")]
        body: Option<Vec<u8>>,
        stream: bool,
        finish: bool,
        ws_id: &'static str,
        accept: bool,
        #[serde(with = "optional_bytes")]
        data: Option<Vec<u8>>,
        binary: bool,
        msg_index: u16,
        code: Option<u16>,
        reason: Option<&'static str>,
        hibernate: bool,
        event: &'static str,
        #[serde(with = "optional_bytes")]
        payload: Option<Vec<u8>>,
        exclude_conn: Option<&'static str>,
        alarm_ts: Option<i64>,
        op_id: u64,
        action: &'static str,
        name: &'static str,
        #[serde(with = "optional_bytes")]
        schedule_args: Option<Vec<u8>>,
        schedule_id: &'static str,
        delay_ms: u64,
        run_at: i64,
        request_id: u64,
        sql: &'static str,
        args: Vec<SqliteValue>,
        lease_key: Option<&'static str>,
        deadline_ms: u32,
        timeout_ms: u64,
        queue_timeout_ms: Option<u64>,
        names: Vec<String>,
        completable: bool,
        message_id: u64,
        #[serde(with = "optional_bytes")]
        response: Option<Vec<u8>>,
        target_op_id: u64,
        work_id: u64,
        work_kind: &'static str,
    }

    #[derive(Serialize)]
    struct GoldenCommandBatch {
        commands: Vec<GoldenCommand>,
    }

    fn golden_command(kind: &'static str) -> GoldenCommand {
        GoldenCommand {
            kind,
            aid: "actor-golden",
            r#gen: 0,
            ok: false,
            run: false,
            error: None,
            state: None,
            kv_id: 0,
            key: None,
            prefix: None,
            reverse: false,
            limit: None,
            value: None,
            call_id: 0,
            output: None,
            connection_state: None,
            req_id: 0,
            status: 0,
            headers: None,
            body: None,
            stream: false,
            finish: false,
            ws_id: "",
            accept: false,
            data: None,
            binary: false,
            msg_index: 0,
            code: None,
            reason: None,
            hibernate: false,
            event: "",
            payload: None,
            exclude_conn: None,
            alarm_ts: None,
            op_id: 0,
            action: "",
            name: "",
            schedule_args: None,
            schedule_id: "",
            delay_ms: 0,
            run_at: 0,
            request_id: 0,
            sql: "",
            args: Vec::new(),
            lease_key: None,
            deadline_ms: 0,
            timeout_ms: 0,
            queue_timeout_ms: None,
            names: Vec::new(),
            completable: false,
            message_id: 0,
            response: None,
            target_op_id: 0,
            work_id: 0,
            work_kind: "",
        }
    }

    fn golden_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("internal/wire/testdata")
    }

    fn write_golden(name: &str, bytes: &[u8]) {
        let directory = golden_dir();
        fs::create_dir_all(&directory).expect("create Go wire testdata directory");
        fs::write(directory.join(name), bytes).expect("write Rust-produced golden")
    }

    #[test]
    fn generate_go_wire_goldens() {
        let config = RunnerConfig {
            engine_endpoint: "http://127.0.0.1:6420".to_owned(),
            namespace: "default".to_owned(),
            runner_name: "rivet-go-golden".to_owned(),
            version: 1,
            total_slots: 4,
            actor_names: vec!["counter".to_owned()],
            actor_actions: BTreeMap::from([("counter".to_owned(), vec!["increment".to_owned()])]),
            actor_hibernate_websockets: BTreeMap::from([("counter".to_owned(), true)]),
            actor_databases: BTreeMap::from([("counter".to_owned(), true)]),
            sqlite_transport: "ffi".to_owned(),
            log_level: "info".to_owned(),
        };
        write_golden(
            "runner_config.msgpack",
            &rmp_serde::to_vec_named(&config).expect("encode runner config"),
        );

        let connected = EventBatch {
            seq: 1,
            events: vec![Event::RunnerConnected {
                runner_id: "envoy-golden".to_owned(),
                metadata: BTreeMap::from([
                    ("management_resource".to_owned(), "/envoys".to_owned()),
                    ("protocol".to_owned(), "envoy-v6".to_owned()),
                ]),
            }],
        };
        write_golden(
            "event_connected.msgpack",
            &connected.encode().expect("encode connected event"),
        );

        let disconnected = EventBatch {
            seq: 2,
            events: vec![Event::RunnerDisconnected {
                reason: "engine connection lost".to_owned(),
            }],
        };
        write_golden(
            "event_disconnected.msgpack",
            &disconnected.encode().expect("encode disconnected event"),
        );

        let stopped = EventBatch {
            seq: 3,
            events: vec![Event::RunnerStopped {
                drain_report: DrainReport {
                    graceful: true,
                    elapsed_ms: 12,
                    actors_stopped: 1,
                    actors_remaining: 0,
                },
            }],
        };
        write_golden(
            "event_stopped.msgpack",
            &stopped.encode().expect("encode stopped event"),
        );

        let actor_start = EventBatch {
            seq: 4,
            events: vec![Event::ActorStart {
                aid: "actor-golden".to_owned(),
                r#gen: 7,
                name: "counter".to_owned(),
                key: "tenant/counter".to_owned(),
                create_ts: 0,
                input: b"input".to_vec(),
                persisted_state: Some(b"state".to_vec()),
                sqlite_socket_path: None,
                connections: vec![Connection {
                    id: "connection-restored".to_owned(),
                    parameters: vec![
                        0xa1, 0x68, b'u', b's', b'e', b'r', b'n', b'a', b'm', b'e', 0x63, b'A',
                        b'd', b'a',
                    ],
                    state: br#"{"username":"Ada","moves":2}"#.to_vec(),
                    path: "/connect".to_owned(),
                    headers: BTreeMap::new(),
                    can_hibernate: true,
                    resumed: true,
                    actor_connect: true,
                }],
            }],
        };
        write_golden(
            "event_actor_start.msgpack",
            &actor_start.encode().expect("encode actor start event"),
        );

        let fresh_actor_start = EventBatch {
            seq: 8,
            events: vec![Event::ActorStart {
                aid: "actor-fresh".to_owned(),
                r#gen: 1,
                name: "counter".to_owned(),
                key: "fresh".to_owned(),
                create_ts: 0,
                input: Vec::new(),
                persisted_state: None,
                sqlite_socket_path: None,
                connections: Vec::new(),
            }],
        };
        write_golden(
            "event_actor_start_fresh.msgpack",
            &fresh_actor_start
                .encode()
                .expect("encode fresh actor start event"),
        );

        let empty_actor_start = EventBatch {
            seq: 9,
            events: vec![Event::ActorStart {
                aid: "actor-empty".to_owned(),
                r#gen: 2,
                name: "counter".to_owned(),
                key: "empty".to_owned(),
                create_ts: 0,
                input: Vec::new(),
                persisted_state: Some(Vec::new()),
                sqlite_socket_path: None,
                connections: Vec::new(),
            }],
        };
        write_golden(
            "event_actor_start_empty_state.msgpack",
            &empty_actor_start
                .encode()
                .expect("encode empty-state actor start event"),
        );

        let actor_stop = EventBatch {
            seq: 5,
            events: vec![Event::ActorStop {
                aid: "actor-golden".to_owned(),
                r#gen: 7,
                reason: "destroy".to_owned(),
            }],
        };
        write_golden(
            "event_actor_stop.msgpack",
            &actor_stop.encode().expect("encode actor stop event"),
        );

        let actor_alarm = EventBatch {
            seq: 17,
            events: vec![Event::ActorAlarm {
                aid: "actor-golden".to_owned(),
                r#gen: 8,
                alarm_ts: 1_788_500_000_000,
            }],
        };
        write_golden(
            "event_actor_alarm.msgpack",
            &actor_alarm.encode().expect("encode actor alarm event"),
        );

        let actor_intent_result = EventBatch {
            seq: 18,
            events: vec![Event::ActorIntentResult {
                op_id: 41,
                error: None,
            }],
        };
        write_golden(
            "event_actor_intent_result.msgpack",
            &actor_intent_result
                .encode()
                .expect("encode actor intent result event"),
        );

        let schedule_result = EventBatch {
            seq: 20,
            events: vec![Event::ActorScheduleResult {
                op_id: 61,
                operation: "list".to_owned(),
                schedule_id: None,
                cancelled: None,
                schedules: vec![ScheduledEvent {
                    id: "schedule-golden".to_owned(),
                    action: "remind".to_owned(),
                    args: vec![0x81, 0x01],
                    run_at: 1_788_500_000_000,
                }],
                error: None,
            }],
        };
        write_golden(
            "event_actor_schedule_result.msgpack",
            &schedule_result.encode().expect("encode schedule result"),
        );

        let queue_result = EventBatch {
            seq: 24,
            events: vec![Event::ActorQueueResult {
                op_id: 81,
                queue_operation: "next".to_owned(),
                message: Some(QueueMessage {
                    id: 9,
                    name: "message".to_owned(),
                    body: vec![0xa1, 0x61, 0x78, 0x01],
                    created_at: 1_788_500_000_000,
                    completable: true,
                }),
                response: None,
                error: None,
            }],
        };
        write_golden(
            "event_actor_queue_result.msgpack",
            &queue_result.encode().expect("encode queue result"),
        );

        let action_call = EventBatch {
            seq: 10,
            events: vec![Event::ActionCall {
                aid: "actor-golden".to_owned(),
                r#gen: 7,
                call_id: 21,
                action: "increment".to_owned(),
                timeout_ms: 60_000,
                args: vec![0x81, 0x02],
                conn_id: Some("conn-golden".to_owned()),
            }],
        };
        write_golden(
            "event_action_call.msgpack",
            &action_call.encode().expect("encode action call event"),
        );

        let connection = Connection {
            id: "conn-golden".to_owned(),
            parameters: vec![0xa1, 0x64, b'n', b'a', b'm', b'e', 0x63, b'A', b'd', b'a'],
            state: br#"{"moves":2}"#.to_vec(),
            path: "/connect".to_owned(),
            headers: BTreeMap::from([("x-test".to_owned(), "one".to_owned())]),
            can_hibernate: true,
            resumed: false,
            actor_connect: true,
        };
        for (name, seq, event) in [
            (
                "event_connection_preflight.msgpack",
                21,
                Event::ConnectionPreflight {
                    aid: "actor-golden".to_owned(),
                    r#gen: 7,
                    op_id: 71,
                    connection: connection.clone(),
                },
            ),
            (
                "event_connection_open.msgpack",
                22,
                Event::ConnectionOpen {
                    aid: "actor-golden".to_owned(),
                    r#gen: 7,
                    op_id: 72,
                    connection: connection.clone(),
                },
            ),
            (
                "event_connection_close.msgpack",
                23,
                Event::ConnectionClose {
                    aid: "actor-golden".to_owned(),
                    r#gen: 7,
                    op_id: 73,
                    connection: connection.clone(),
                },
            ),
        ] {
            write_golden(
                name,
                &EventBatch {
                    seq,
                    events: vec![event],
                }
                .encode()
                .expect("encode connection event"),
            );
        }

        let http_request = EventBatch {
            seq: 11,
            events: vec![Event::HttpRequest {
                aid: "actor-golden".to_owned(),
                r#gen: 7,
                req_id: 22,
                method: "POST".to_owned(),
                path: "/upload?part=1".to_owned(),
                headers: BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
                body: b"first".to_vec(),
                stream: true,
            }],
        };
        write_golden(
            "event_http_request.msgpack",
            &http_request.encode().expect("encode HTTP request event"),
        );
        let http_chunk = EventBatch {
            seq: 12,
            events: vec![Event::HttpRequestChunk {
                req_id: 22,
                body: b"second".to_vec(),
                finish: true,
            }],
        };
        write_golden(
            "event_http_request_chunk.msgpack",
            &http_chunk
                .encode()
                .expect("encode HTTP request chunk event"),
        );
        let http_abort = EventBatch {
            seq: 13,
            events: vec![Event::HttpRequestAbort { req_id: 23 }],
        };
        write_golden(
            "event_http_request_abort.msgpack",
            &http_abort
                .encode()
                .expect("encode HTTP request abort event"),
        );

        let ws_open = EventBatch {
            seq: 14,
            events: vec![Event::WsOpen {
                aid: "actor-golden".to_owned(),
                ws_id: "ws-golden".to_owned(),
                path: "/chat?room=golden".to_owned(),
                headers: BTreeMap::from([("x-test".to_owned(), "one".to_owned())]),
                can_hibernate: true,
                resumed: true,
            }],
        };
        write_golden(
            "event_ws_open.msgpack",
            &ws_open.encode().expect("encode WebSocket open event"),
        );
        let ws_message = EventBatch {
            seq: 15,
            events: vec![Event::WsMessage {
                ws_id: "ws-golden".to_owned(),
                data: b"hello".to_vec(),
                binary: false,
                msg_index: 3,
            }],
        };
        write_golden(
            "event_ws_message.msgpack",
            &ws_message.encode().expect("encode WebSocket message event"),
        );
        let ws_close = EventBatch {
            seq: 16,
            events: vec![Event::WsClose {
                ws_id: "ws-golden".to_owned(),
                code: Some(1000),
                reason: Some("done".to_owned()),
            }],
        };
        write_golden(
            "event_ws_close.msgpack",
            &ws_close.encode().expect("encode WebSocket close event"),
        );

        let sqlite_result = EventBatch {
            seq: 19,
            events: vec![Event::SqliteResult {
                request_id: 51,
                chunk_index: 0,
                done: true,
                columns: vec![
                    "i".to_owned(),
                    "r".to_owned(),
                    "t".to_owned(),
                    "b".to_owned(),
                ],
                values: vec![
                    SqliteValue::Integer { integer: 7 },
                    SqliteValue::Real {
                        bits: 1.5f64.to_bits(),
                    },
                    SqliteValue::Text {
                        text: "hello".to_owned(),
                    },
                    SqliteValue::Blob { blob: Vec::new() },
                ],
                rows_affected: 1,
                last_insert_id: Some(9),
                error: None,
            }],
        };
        write_golden(
            "event_sqlite_result.msgpack",
            &sqlite_result.encode().expect("encode SQLite result event"),
        );

        let kv_result = EventBatch {
            seq: 6,
            events: vec![Event::KvResult {
                kv_id: 11,
                value: None,
                entries: vec![KvEntry {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }],
                error: None,
            }],
        };
        write_golden(
            "event_kv_result.msgpack",
            &kv_result.encode().expect("encode KV result event"),
        );

        let state_persisted = EventBatch {
            seq: 7,
            events: vec![Event::StatePersisted {
                aid: "actor-golden".to_owned(),
                r#gen: 7,
                state_version: 2,
                error: None,
            }],
        };
        write_golden(
            "event_state_persisted.msgpack",
            &state_persisted
                .encode()
                .expect("encode state persisted event"),
        );

        write_golden(
            "command_empty.msgpack",
            &rmp_serde::to_vec_named(&CommandBatch {
                commands: Vec::new(),
            })
            .expect("encode empty command batch"),
        );
        let mut start = golden_command("actor_start_result");
        start.r#gen = 7;
        start.ok = true;
        let mut stop = golden_command("actor_stop_result");
        stop.r#gen = 7;
        let mut save = golden_command("save_state");
        save.r#gen = 7;
        save.state = Some(b"state".to_vec());
        let mut get = golden_command("kv_get");
        get.kv_id = 11;
        get.key = Some(b"key".to_vec());
        let mut list = golden_command("kv_list");
        list.kv_id = 12;
        list.prefix = Some(b"prefix".to_vec());
        list.limit = Some(32);
        let mut put = golden_command("kv_put");
        put.kv_id = 13;
        put.key = Some(b"key".to_vec());
        put.value = Some(b"value".to_vec());
        let mut delete = golden_command("kv_delete");
        delete.kv_id = 14;
        delete.key = Some(b"key".to_vec());
        let command_m2 = rmp_serde::to_vec_named(&GoldenCommandBatch {
            commands: vec![start, stop, save, get, list, put, delete],
        })
        .expect("encode M2 command batch");
        let decoded = CommandBatch::decode(&command_m2)
            .expect("Rust command decoder accepts the full Go command shape");
        assert_eq!(decoded.commands.len(), 7);
        write_golden("command_m2.msgpack", &command_m2);

        let mut action = golden_command("action_result");
        action.call_id = 21;
        action.output = Some(vec![0x18, 0x2a]);
        let mut response_start = golden_command("http_response_start");
        response_start.req_id = 22;
        response_start.status = 201;
        response_start.headers = Some(BTreeMap::from([(
            "content-type".to_owned(),
            "text/plain".to_owned(),
        )]));
        response_start.body = Some(Vec::new());
        response_start.stream = true;
        let mut response_chunk = golden_command("http_response_chunk");
        response_chunk.req_id = 22;
        response_chunk.body = Some(b"response".to_vec());
        response_chunk.finish = true;
        let command_m3 = rmp_serde::to_vec_named(&GoldenCommandBatch {
            commands: vec![action, response_start, response_chunk],
        })
        .expect("encode M3 command batch");
        let decoded = CommandBatch::decode(&command_m3)
            .expect("Rust command decoder accepts the full Go M3 command shape");
        assert_eq!(decoded.commands.len(), 3);
        write_golden("command_m3.msgpack", &command_m3);

        let mut ws_open_result = golden_command("ws_open_result");
        ws_open_result.ws_id = "ws-golden";
        ws_open_result.accept = true;
        let mut ws_ack = golden_command("ws_message_ack");
        ws_ack.ws_id = "ws-golden";
        ws_ack.msg_index = 3;
        let mut ws_send = golden_command("ws_send");
        ws_send.ws_id = "ws-golden";
        ws_send.data = Some(b"targeted".to_vec());
        let mut ws_close_cmd = golden_command("ws_close_cmd");
        ws_close_cmd.ws_id = "ws-golden";
        ws_close_cmd.code = Some(1000);
        ws_close_cmd.reason = Some("done");
        ws_close_cmd.hibernate = true;
        let mut broadcast = golden_command("broadcast");
        broadcast.event = "countChanged";
        broadcast.payload = Some(vec![0x81, 0x18, 0x2a]);
        broadcast.exclude_conn = Some("ws-excluded");
        let mut stop_intent = golden_command("stop_intent");
        stop_intent.aid = "actor-golden";
        let command_m4 = rmp_serde::to_vec_named(&GoldenCommandBatch {
            commands: vec![
                ws_open_result,
                ws_ack,
                ws_send,
                ws_close_cmd,
                broadcast,
                stop_intent,
            ],
        })
        .expect("encode M4 command batch");
        let decoded = CommandBatch::decode(&command_m4)
            .expect("Rust command decoder accepts the full Go M4 command shape");
        assert_eq!(decoded.commands.len(), 6);
        write_golden("command_m4.msgpack", &command_m4);

        let mut alarm_handled = golden_command("alarm_handled");
        alarm_handled.r#gen = 8;
        let mut set_alarm = golden_command("set_alarm");
        set_alarm.op_id = 41;
        set_alarm.r#gen = 8;
        set_alarm.alarm_ts = Some(1_788_500_000_000);
        let mut clear_alarm = golden_command("set_alarm");
        clear_alarm.op_id = 42;
        clear_alarm.r#gen = 8;
        clear_alarm.alarm_ts = None;
        let mut sleep_intent = golden_command("sleep_intent");
        sleep_intent.op_id = 43;
        sleep_intent.r#gen = 8;
        let command_m5 = rmp_serde::to_vec_named(&GoldenCommandBatch {
            commands: vec![alarm_handled, set_alarm, clear_alarm, sleep_intent],
        })
        .expect("encode M5 command batch");
        let decoded = CommandBatch::decode(&command_m5)
            .expect("Rust command decoder accepts the full Go M5 command shape");
        assert_eq!(decoded.commands.len(), 4);
        write_golden("command_m5.msgpack", &command_m5);

        let mut exec = golden_command("sqlite_exec");
        exec.request_id = 51;
        exec.r#gen = 9;
        exec.sql = "INSERT INTO todo(title) VALUES (?)";
        exec.args = vec![SqliteValue::Text {
            text: "ship".to_owned(),
        }];
        exec.deadline_ms = 65_000;
        let mut query = golden_command("sqlite_query");
        query.request_id = 52;
        query.r#gen = 9;
        query.sql = "SELECT ?";
        query.args = vec![SqliteValue::Integer { integer: 7 }];
        query.deadline_ms = 65_000;
        let mut begin = golden_command("sqlite_begin");
        begin.request_id = 53;
        begin.r#gen = 9;
        begin.lease_key = Some("lease-golden");
        begin.deadline_ms = 65_000;
        begin.timeout_ms = 60_000;
        let mut commit = golden_command("sqlite_commit");
        commit.request_id = 54;
        commit.r#gen = 9;
        commit.lease_key = Some("lease-golden");
        commit.deadline_ms = 65_000;
        let mut rollback = golden_command("sqlite_rollback");
        rollback.request_id = 55;
        rollback.r#gen = 9;
        rollback.lease_key = Some("lease-other");
        rollback.deadline_ms = 65_000;
        let command_m7 = rmp_serde::to_vec_named(&GoldenCommandBatch {
            commands: vec![exec, query, begin, commit, rollback],
        })
        .expect("encode M7 command batch");
        let decoded = CommandBatch::decode(&command_m7)
            .expect("Rust command decoder accepts the full Go M7 command shape");
        assert_eq!(decoded.commands.len(), 5);
        write_golden("command_m7.msgpack", &command_m7);

        let mut schedule_after = golden_command("schedule_after");
        schedule_after.r#gen = 9;
        schedule_after.op_id = 61;
        schedule_after.delay_ms = 1_500;
        schedule_after.action = "remind";
        schedule_after.schedule_args = Some(vec![0x81, 0x01]);
        let mut schedule_at = golden_command("schedule_at");
        schedule_at.r#gen = 9;
        schedule_at.op_id = 62;
        schedule_at.run_at = 1_788_500_000_000;
        schedule_at.action = "remind";
        schedule_at.schedule_args = Some(vec![0x81, 0x02]);
        let mut schedule_cancel = golden_command("schedule_cancel");
        schedule_cancel.r#gen = 9;
        schedule_cancel.op_id = 63;
        schedule_cancel.schedule_id = "schedule-golden";
        let mut schedule_get = golden_command("schedule_get");
        schedule_get.r#gen = 9;
        schedule_get.op_id = 64;
        schedule_get.schedule_id = "schedule-golden";
        let mut schedule_list = golden_command("schedule_list");
        schedule_list.r#gen = 9;
        schedule_list.op_id = 65;
        let command_m9 = rmp_serde::to_vec_named(&GoldenCommandBatch {
            commands: vec![
                schedule_after,
                schedule_at,
                schedule_cancel,
                schedule_get,
                schedule_list,
            ],
        })
        .expect("encode M9 command batch");
        let decoded = CommandBatch::decode(&command_m9)
            .expect("Rust command decoder accepts the full Go M9 command shape");
        decoded.validate().expect("M9 command batch is valid");
        assert_eq!(decoded.commands.len(), 5);
        write_golden("command_m9.msgpack", &command_m9);

        let mut connection_result = golden_command("connection_result");
        connection_result.op_id = 71;
        connection_result.connection_state = Some(br#"{"moves":1}"#.to_vec());
        let mut connected_action = golden_command("action_result");
        connected_action.call_id = 74;
        connected_action.output = Some(vec![0xf6]);
        connected_action.connection_state = Some(br#"{"moves":3}"#.to_vec());
        let command_m10 = rmp_serde::to_vec_named(&GoldenCommandBatch {
            commands: vec![connection_result, connected_action],
        })
        .expect("encode M10 command batch");
        let decoded = CommandBatch::decode(&command_m10)
            .expect("Rust command decoder accepts the full Go M10 command shape");
        decoded.validate().expect("M10 command batch is valid");
        assert_eq!(decoded.commands.len(), 2);
        write_golden("command_m10.msgpack", &command_m10);

        let mut run_result = golden_command("actor_run_result");
        run_result.r#gen = 11;
        let mut queue_send = golden_command("queue_send");
        queue_send.r#gen = 11;
        queue_send.op_id = 81;
        queue_send.name = "message";
        queue_send.body = Some(vec![0xa1, 0x61, 0x78, 0x01]);
        let mut queue_wait = golden_command("queue_enqueue_wait");
        queue_wait.r#gen = 11;
        queue_wait.op_id = 82;
        queue_wait.name = "message";
        queue_wait.body = Some(vec![0xf6]);
        queue_wait.queue_timeout_ms = Some(5_000);
        let mut queue_next = golden_command("queue_next");
        queue_next.r#gen = 11;
        queue_next.op_id = 83;
        queue_next.names = vec!["message".to_owned()];
        queue_next.completable = true;
        let mut queue_complete = golden_command("queue_complete");
        queue_complete.r#gen = 11;
        queue_complete.op_id = 84;
        queue_complete.message_id = 9;
        queue_complete.response = Some(vec![0xf5]);
        let mut queue_retry = golden_command("queue_retry");
        queue_retry.r#gen = 11;
        queue_retry.op_id = 85;
        queue_retry.message_id = 9;
        let mut queue_cancel = golden_command("queue_cancel");
        queue_cancel.r#gen = 11;
        queue_cancel.target_op_id = 83;
        let mut wait_until = golden_command("managed_work_begin");
        wait_until.r#gen = 11;
        wait_until.op_id = 86;
        wait_until.work_id = 1;
        wait_until.work_kind = "wait_until";
        let mut work_end = golden_command("managed_work_end");
        work_end.r#gen = 11;
        work_end.work_id = 1;
        let command_m11 = rmp_serde::to_vec_named(&GoldenCommandBatch {
            commands: vec![
                run_result,
                queue_send,
                queue_wait,
                queue_next,
                queue_complete,
                queue_retry,
                queue_cancel,
                wait_until,
                work_end,
            ],
        })
        .expect("encode M11 command batch");
        let decoded = CommandBatch::decode(&command_m11)
            .expect("Rust command decoder accepts the full Go M11 command shape");
        decoded.validate().expect("M11 command batch is valid");
        assert_eq!(decoded.commands.len(), 9);
        write_golden("command_m11.msgpack", &command_m11);
    }

    #[test]
    fn event_batch_round_trip() {
        let batch = EventBatch {
            seq: 7,
            events: vec![Event::ActorStop {
                aid: "actor".to_owned(),
                r#gen: 2,
                reason: "destroy".to_owned(),
            }],
        };
        let bytes = batch.encode().expect("encode event batch");
        let decoded: EventBatch = rmp_serde::from_slice(&bytes).expect("decode event batch");
        assert_eq!(decoded, batch);
    }

    #[test]
    fn broadcast_frame_uses_the_pinned_actor_connect_cbor_envelope() {
        #[derive(Deserialize)]
        struct Envelope {
            body: Body,
        }
        #[derive(Deserialize)]
        struct Body {
            tag: String,
            val: EventValue,
        }
        #[derive(Deserialize)]
        struct EventValue {
            name: String,
            args: Vec<u32>,
        }

        let mut args = Vec::new();
        ciborium::into_writer(&vec![42_u32], &mut args).expect("encode event arguments");
        let frame = encode_actor_connect_event_frame("countChanged", &args)
            .expect("encode actor-connect event frame");
        let decoded: Envelope =
            ciborium::from_reader(frame.as_slice()).expect("decode actor-connect event frame");
        assert_eq!(decoded.body.tag, "Event");
        assert_eq!(decoded.body.val.name, "countChanged");
        assert_eq!(decoded.body.val.args, vec![42]);
    }

    #[test]
    fn command_validation_rejects_inconsistent_start_result() {
        let batch = CommandBatch {
            commands: vec![Command::ActorStartResult {
                aid: "actor".to_owned(),
                r#gen: 1,
                ok: true,
                run: false,
                error: Some(WireError::new("unexpected", "both result arms")),
            }],
        };
        assert!(batch.validate().is_err());

        let failed_run = CommandBatch {
            commands: vec![Command::ActorStartResult {
                aid: "actor".to_owned(),
                r#gen: 1,
                ok: false,
                run: true,
                error: Some(WireError::new("start_failed", "failed")),
            }],
        };
        assert!(failed_run.validate().is_err());
    }

    #[test]
    fn schedule_command_validation_requires_registered_shape() {
        let valid = Command::ScheduleAfter {
            op_id: 1,
            aid: "actor".to_owned(),
            r#gen: 2,
            delay_ms: 100,
            action: "run".to_owned(),
            schedule_args: vec![0x81, 0x01],
        };
        CommandBatch {
            commands: vec![valid.clone()],
        }
        .validate()
        .expect("valid schedule command");

        let invalid = [
            Command::ScheduleAfter {
                op_id: 0,
                aid: "actor".to_owned(),
                r#gen: 2,
                delay_ms: 100,
                action: "run".to_owned(),
                schedule_args: vec![0x81, 0x01],
            },
            Command::ScheduleAfter {
                op_id: 1,
                aid: "actor".to_owned(),
                r#gen: 2,
                delay_ms: 100,
                action: "__rivet_go_alarm".to_owned(),
                schedule_args: vec![0x81, 0x01],
            },
            Command::ScheduleAfter {
                op_id: 1,
                aid: "actor".to_owned(),
                r#gen: 2,
                delay_ms: 100,
                action: "run".to_owned(),
                schedule_args: vec![0x01],
            },
        ];
        for command in invalid {
            assert!(
                CommandBatch {
                    commands: vec![command]
                }
                .validate()
                .is_err()
            );
        }
    }

    #[test]
    fn connection_command_validation_requires_correlation_and_state() {
        CommandBatch {
            commands: vec![Command::ConnectionResult {
                op_id: 1,
                connection_state: Some(Vec::new()),
                error: None,
            }],
        }
        .validate()
        .expect("present empty connection state is valid");

        for command in [
            Command::ConnectionResult {
                op_id: 0,
                connection_state: Some(Vec::new()),
                error: None,
            },
            Command::ConnectionResult {
                op_id: 1,
                connection_state: None,
                error: None,
            },
            Command::ActionResult {
                call_id: 2,
                output: None,
                connection_state: Some(Vec::new()),
                error: Some(WireError::new("rejected", "action failed")),
            },
        ] {
            assert!(
                CommandBatch {
                    commands: vec![command]
                }
                .validate()
                .is_err()
            );
        }
    }

    #[test]
    fn unknown_command_is_preserved_for_structured_rejection() {
        let bytes = rmp_serde::to_vec_named(&serde_json::json!({
            "commands": [{"kind": "future_command", "value": 1}]
        }))
        .expect("encode command batch");
        let batch = CommandBatch::decode(&bytes).expect("decode command batch");
        assert!(batch.contains_unknown());
    }
}
