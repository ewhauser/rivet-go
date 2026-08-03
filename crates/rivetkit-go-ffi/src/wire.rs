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
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct DrainReport {
    pub graceful: bool,
    pub elapsed_ms: u64,
    pub actors_stopped: u32,
    pub actors_remaining: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommandBatch {
    pub commands: Vec<Command>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Command {
    #[serde(other)]
    Unknown,
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
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

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
            actor_names: Vec::new(),
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
                    actors_stopped: 0,
                    actors_remaining: 0,
                },
            }],
        };
        write_golden(
            "event_stopped.msgpack",
            &stopped.encode().expect("encode stopped event"),
        );

        write_golden(
            "command_empty.msgpack",
            &rmp_serde::to_vec_named(&CommandBatch {
                commands: Vec::new(),
            })
            .expect("encode empty command batch"),
        );
    }

    #[test]
    fn event_batch_round_trip() {
        let batch = EventBatch {
            seq: 7,
            events: vec![Event::RunnerDisconnected {
                reason: "test".to_owned(),
            }],
        };
        let bytes = batch.encode().expect("encode event batch");
        let decoded: EventBatch = rmp_serde::from_slice(&bytes).expect("decode event batch");
        assert_eq!(decoded, batch);
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
