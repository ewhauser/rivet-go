//! Versioned-by-ABI MessagePack values shared with Go's `internal/wire` package.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

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
    },
    ActorStop {
        aid: String,
        r#gen: u64,
        reason: String,
    },
    ActionCall {
        aid: String,
        r#gen: u64,
        call_id: u64,
        action: String,
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
}

impl WireError {
    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct KvEntry {
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
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
    ActionResult {
        call_id: u64,
        #[serde(default, with = "optional_bytes")]
        output: Option<Vec<u8>>,
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
        const MAX_BODY_CHUNK: usize = 1 << 20;

        for command in &self.commands {
            match command {
                Command::ActorStartResult { aid, ok, error, .. } => {
                    require_aid(aid)?;
                    if *ok == error.is_some() {
                        return Err(
                            "actor_start_result must contain exactly one of ok=true or error"
                                .to_owned(),
                        );
                    }
                    require_wire_error(error.as_ref())?;
                }
                Command::ActorStopResult { aid, error, .. } => {
                    require_aid(aid)?;
                    require_wire_error(error.as_ref())?;
                }
                Command::SaveState { aid, .. } => {
                    require_aid(aid)?;
                }
                Command::ActionResult {
                    call_id,
                    output,
                    error,
                } => {
                    require_correlation("action_result", *call_id)?;
                    if output.is_some() == error.is_some() {
                        return Err(
                            "action_result must contain exactly one of output or error".to_owned()
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
                Command::Unknown => return Err("unknown command".to_owned()),
            }
        }
        Ok(())
    }
}

fn require_aid(aid: &str) -> Result<(), String> {
    if aid.is_empty() {
        Err("actor command aid must not be empty".to_owned())
    } else {
        Ok(())
    }
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
        req_id: u64,
        status: u16,
        headers: Option<BTreeMap<String, String>>,
        #[serde(with = "optional_bytes")]
        body: Option<Vec<u8>>,
        stream: bool,
        finish: bool,
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
            req_id: 0,
            status: 0,
            headers: None,
            body: None,
            stream: false,
            finish: false,
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

        let action_call = EventBatch {
            seq: 10,
            events: vec![Event::ActionCall {
                aid: "actor-golden".to_owned(),
                r#gen: 7,
                call_id: 21,
                action: "increment".to_owned(),
                args: vec![0x81, 0x02],
                conn_id: Some("conn-golden".to_owned()),
            }],
        };
        write_golden(
            "event_action_call.msgpack",
            &action_call.encode().expect("encode action call event"),
        );

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
    fn command_validation_rejects_inconsistent_start_result() {
        let batch = CommandBatch {
            commands: vec![Command::ActorStartResult {
                aid: "actor".to_owned(),
                r#gen: 1,
                ok: true,
                error: Some(WireError::new("unexpected", "both result arms")),
            }],
        };
        assert!(batch.validate().is_err());
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
