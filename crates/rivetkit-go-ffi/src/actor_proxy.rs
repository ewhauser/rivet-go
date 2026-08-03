//! Callback-free proxy from rivetkit-core actor factories to the Go pump.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::Sender;
use rivetkit_core::actor::ShutdownKind;
use rivetkit_core::{
    ActorConfig, ActorContext, ActorEvent, ActorFactory, ActorStart, CoreRegistry, ListOpts,
    StateDelta, format_actor_key,
};
use serde::{Deserialize, Serialize};

use crate::correlation::{CorrelationError, CorrelationTable};
use crate::wire::{Command, Event, KvEntry, WireError};

const LIFECYCLE_RESULT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_KV_LIST_ENTRIES: u32 = 1_024;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ActorIdentity {
    aid: String,
    generation: u64,
}

impl ActorIdentity {
    fn new(aid: impl Into<String>, generation: u64) -> Self {
        Self {
            aid: aid.into(),
            generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum LifecycleKind {
    Start,
    Stop,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LifecycleKey {
    actor: ActorIdentity,
    kind: LifecycleKind,
}

#[derive(Clone)]
struct ActiveActor {
    ctx: ActorContext,
    state_version: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct LifecycleResolution {
    error: Option<WireError>,
}

#[derive(Clone, Default)]
struct LifecyclePending {
    entries: Arc<Mutex<HashMap<LifecycleKey, u64>>>,
}

impl LifecyclePending {
    fn insert(&self, key: LifecycleKey, id: u64) -> Result<()> {
        let mut entries = self
            .entries
            .lock()
            .expect("lifecycle pending table poisoned");
        if entries.insert(key, id).is_some() {
            return Err(anyhow!("duplicate pending actor lifecycle result"));
        }
        Ok(())
    }

    fn resolve(
        &self,
        key: &LifecycleKey,
        resolution: LifecycleResolution,
        correlations: &CorrelationTable,
    ) -> bool {
        let id = self
            .entries
            .lock()
            .expect("lifecycle pending table poisoned")
            .remove(key);
        let Some(id) = id else {
            return false;
        };
        let payload = rmp_serde::to_vec_named(&resolution)
            .expect("LifecycleResolution serialization is infallible");
        correlations.resolve(id, payload)
    }

    fn remove(&self, key: &LifecycleKey) -> Option<u64> {
        self.entries
            .lock()
            .expect("lifecycle pending table poisoned")
            .remove(key)
    }

    fn retain_live(&self, correlations: &CorrelationTable) {
        self.entries
            .lock()
            .expect("lifecycle pending table poisoned")
            .retain(|_, id| correlations.contains(*id));
    }

    fn clear(&self) {
        self.entries
            .lock()
            .expect("lifecycle pending table poisoned")
            .clear();
    }
}

#[derive(Clone)]
pub(crate) struct ActorProxy {
    events: Sender<Event>,
    correlations: CorrelationTable,
    pending: LifecyclePending,
    actors: Arc<Mutex<HashMap<ActorIdentity, ActiveActor>>>,
}

impl ActorProxy {
    pub(crate) fn new(events: Sender<Event>, correlations: CorrelationTable) -> Self {
        Self {
            events,
            correlations,
            pending: LifecyclePending::default(),
            actors: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn register(&self, registry: &mut CoreRegistry, actor_names: &[String]) {
        for actor_name in actor_names {
            let proxy = self.clone();
            let config = ActorConfig {
                name: Some(actor_name.clone()),
                has_state: true,
                // Core's remote SQLite backend persists actor state through
                // the engine/envoy. The local backend requires an
                // atomic-write-enabled SQLite build that the Go SDK does not
                // otherwise need to ship.
                remote_sqlite: true,
                // Sleep/alarm behavior belongs to M5. Keeping M2 actors awake
                // makes lifecycle changes engine-driven and deterministic.
                no_sleep: true,
                ..ActorConfig::default()
            };
            registry.register(
                actor_name,
                ActorFactory::new_with_manual_startup_ready(config, move |start| {
                    let proxy = proxy.clone();
                    Box::pin(async move { proxy.run_actor(start).await })
                }),
            );
        }
    }

    pub(crate) fn handle_command(&self, command: Command) {
        match command {
            Command::ActorStartResult {
                aid,
                r#gen: generation,
                ok: _,
                error,
            } => {
                self.pending.resolve(
                    &LifecycleKey {
                        actor: ActorIdentity::new(aid, generation),
                        kind: LifecycleKind::Start,
                    },
                    LifecycleResolution { error },
                    &self.correlations,
                );
            }
            Command::ActorStopResult {
                aid,
                r#gen: generation,
                error,
            } => {
                self.pending.resolve(
                    &LifecycleKey {
                        actor: ActorIdentity::new(aid, generation),
                        kind: LifecycleKind::Stop,
                    },
                    LifecycleResolution { error },
                    &self.correlations,
                );
            }
            command => {
                let proxy = self.clone();
                tokio::spawn(async move {
                    proxy.execute_actor_command(command).await;
                });
            }
        }
    }

    pub(crate) fn sweep_pending(&self) {
        self.pending.retain_live(&self.correlations);
    }

    pub(crate) fn drain_shutdown(&self) {
        self.pending.clear();
    }

    async fn run_actor(&self, start: ActorStart) -> Result<()> {
        let ActorStart {
            ctx,
            input,
            snapshot,
            mut events,
            startup_ready,
            ..
        } = start;
        let generation = ctx
            .sql()
            .runtime_config()
            .context("read actor generation from core SQLite runtime configuration")?
            .generation
            .unwrap_or(0);
        let identity = ActorIdentity::new(ctx.actor_id(), generation);
        self.actors
            .lock()
            .expect("active actor table poisoned")
            .insert(
                identity.clone(),
                ActiveActor {
                    ctx: ctx.clone(),
                    state_version: Arc::new(AtomicU64::new(0)),
                },
            );

        let result = self
            .run_actor_inner(
                &identity,
                &ctx,
                input.unwrap_or_default(),
                snapshot.unwrap_or_default(),
                &mut events,
                startup_ready,
            )
            .await;
        self.actors
            .lock()
            .expect("active actor table poisoned")
            .remove(&identity);
        result
    }

    async fn run_actor_inner(
        &self,
        identity: &ActorIdentity,
        ctx: &ActorContext,
        input: Vec<u8>,
        persisted_state: Vec<u8>,
        events: &mut rivetkit_core::ActorEvents,
        startup_ready: Option<tokio::sync::oneshot::Sender<Result<()>>>,
    ) -> Result<()> {
        let start_result = self
            .request_lifecycle(
                LifecycleKey {
                    actor: identity.clone(),
                    kind: LifecycleKind::Start,
                },
                Event::ActorStart {
                    aid: identity.aid.clone(),
                    r#gen: identity.generation,
                    name: ctx.name().to_owned(),
                    key: format_actor_key(ctx.key()),
                    // v2.3.10's ActorStart/ActorContext do not expose the
                    // engine actor create timestamp. The stable field remains
                    // present and uses the documented zero sentinel.
                    create_ts: 0,
                    input,
                    persisted_state,
                },
                None,
            )
            .await;

        match start_result {
            Ok(()) => {
                if let Some(startup_ready) = startup_ready {
                    let _ = startup_ready.send(Ok(()));
                }
            }
            Err(error) => {
                if let Some(startup_ready) = startup_ready {
                    let _ = startup_ready.send(Err(anyhow!("{error:#}")));
                }
                return Err(error);
            }
        }

        while let Some(event) = events.recv().await {
            match event {
                ActorEvent::SerializeState { reply, .. } => {
                    // Go explicitly saves through SaveState. Returning no
                    // deltas retains the last successfully persisted snapshot.
                    reply.send(Ok(Vec::new()));
                }
                ActorEvent::RunGracefulCleanup { reason, reply } => {
                    let shutdown_deadline = ctx.shutdown_deadline_token();
                    let result = self
                        .request_lifecycle(
                            LifecycleKey {
                                actor: identity.clone(),
                                kind: LifecycleKind::Stop,
                            },
                            Event::ActorStop {
                                aid: identity.aid.clone(),
                                r#gen: identity.generation,
                                reason: shutdown_reason(reason).to_owned(),
                            },
                            Some(shutdown_deadline),
                        )
                        .await;
                    reply.send(result);
                }
                ActorEvent::Action { reply, .. } => {
                    reply.send(Err(anyhow!("actions are not supported before M3")));
                }
                ActorEvent::HttpRequest { reply, .. } => {
                    reply.send(Err(anyhow!("HTTP requests are not supported before M3")));
                }
                ActorEvent::QueueSend { reply, .. } => {
                    reply.send(Err(anyhow!("queue requests are not supported in M2")));
                }
                ActorEvent::WebSocketOpen { reply, .. }
                | ActorEvent::ConnectionPreflight { reply, .. }
                | ActorEvent::ConnectionOpen { reply, .. }
                | ActorEvent::SubscribeRequest { reply, .. } => {
                    reply.send(Err(anyhow!("connections are not supported before M4")));
                }
                ActorEvent::DisconnectConn { reply, .. } => reply.send(Ok(())),
                ActorEvent::ConnectionClosed { .. } => {}
                ActorEvent::WorkflowHistoryRequested { reply }
                | ActorEvent::WorkflowReplayRequested { reply, .. } => reply.send(Ok(None)),
            }
        }
        Ok(())
    }

    async fn request_lifecycle(
        &self,
        key: LifecycleKey,
        event: Event,
        shutdown_deadline: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<()> {
        let (id, receiver) = self.correlations.insert(LIFECYCLE_RESULT_TIMEOUT);
        self.pending.insert(key.clone(), id)?;
        if self.events.send(event).is_err() {
            self.pending.remove(&key);
            self.correlations.resolve(
                id,
                rmp_serde::to_vec_named(&LifecycleResolution {
                    error: Some(WireError::new("runner_stopped", "Go event queue is closed")),
                })
                .expect("encode lifecycle queue error"),
            );
            return Err(anyhow!("Go event queue is closed"));
        }

        let result = if let Some(shutdown_deadline) = shutdown_deadline {
            tokio::select! {
                result = receiver => result,
                _ = shutdown_deadline.cancelled() => {
                    self.pending.remove(&key);
                    return Err(anyhow!("actor stop handler exceeded core shutdown deadline"));
                }
            }
        } else {
            receiver.await
        };
        self.pending.remove(&key);

        let payload = result
            .context("actor lifecycle correlation sender dropped")?
            .map_err(|error| match error {
                CorrelationError::Timeout => anyhow!("actor lifecycle handler timed out"),
                CorrelationError::Shutdown => anyhow!("runner shut down during actor lifecycle"),
            })?;
        let resolution: LifecycleResolution =
            rmp_serde::from_slice(&payload).context("decode actor lifecycle result")?;
        if let Some(error) = resolution.error {
            return Err(anyhow!("{}: {}", error.code, error.message));
        }
        Ok(())
    }

    async fn execute_actor_command(&self, command: Command) {
        match command {
            Command::SaveState {
                aid,
                r#gen: generation,
                state,
            } => self.save_state(aid, generation, state).await,
            Command::KvGet { kv_id, aid, key } => self.kv_get(kv_id, aid, key).await,
            Command::KvList {
                kv_id,
                aid,
                prefix,
                reverse,
                limit,
            } => {
                self.kv_list(kv_id, aid, prefix, reverse, limit).await;
            }
            Command::KvPut {
                kv_id,
                aid,
                key,
                value,
            } => self.kv_put(kv_id, aid, key, value).await,
            Command::KvDelete { kv_id, aid, key } => self.kv_delete(kv_id, aid, key).await,
            Command::ActorStartResult { .. }
            | Command::ActorStopResult { .. }
            | Command::Unknown => {}
        }
    }

    async fn save_state(&self, aid: String, generation: u64, state: Vec<u8>) {
        let identity = ActorIdentity::new(aid.clone(), generation);
        let actor = self.actor_exact(&identity);
        let (state_version, error) = match actor {
            Some(actor) => match actor
                .ctx
                .save_state(vec![StateDelta::ActorState(state)])
                .await
            {
                Ok(()) => (actor.state_version.fetch_add(1, Ordering::AcqRel) + 1, None),
                Err(error) => (
                    actor.state_version.load(Ordering::Acquire),
                    Some(WireError::new("state_persist_failed", format!("{error:#}"))),
                ),
            },
            None => (
                0,
                Some(WireError::new(
                    "actor_not_found",
                    format!("actor `{aid}` generation {generation} is not active"),
                )),
            ),
        };
        let _ = self.events.send(Event::StatePersisted {
            aid,
            r#gen: generation,
            state_version,
            error,
        });
    }

    #[allow(deprecated)] // The M2 boundary intentionally mirrors the pinned core KV surface.
    async fn kv_get(&self, kv_id: u64, aid: String, key: Vec<u8>) {
        let result = match self.actor_current(&aid) {
            Some(actor) => actor.ctx.kv().get(&key).await,
            None => {
                self.send_kv_actor_not_found(kv_id, &aid);
                return;
            }
        };
        let (value, error) = match result {
            Ok(value) => (value, None),
            Err(error) => (
                None,
                Some(WireError::new("kv_get_failed", format!("{error:#}"))),
            ),
        };
        let _ = self.events.send(Event::KvResult {
            kv_id,
            value,
            entries: Vec::new(),
            error,
        });
    }

    #[allow(deprecated)] // The M2 boundary intentionally mirrors the pinned core KV surface.
    async fn kv_list(
        &self,
        kv_id: u64,
        aid: String,
        prefix: Vec<u8>,
        reverse: bool,
        limit: Option<u32>,
    ) {
        let Some(actor) = self.actor_current(&aid) else {
            self.send_kv_actor_not_found(kv_id, &aid);
            return;
        };
        let opts = ListOpts {
            reverse,
            limit: Some(limit.unwrap_or(MAX_KV_LIST_ENTRIES)),
        };
        let (entries, error) = match actor.ctx.kv().list_prefix(&prefix, opts).await {
            Ok(entries) => (
                entries
                    .into_iter()
                    .map(|(key, value)| KvEntry { key, value })
                    .collect(),
                None,
            ),
            Err(error) => (
                Vec::new(),
                Some(WireError::new("kv_list_failed", format!("{error:#}"))),
            ),
        };
        let _ = self.events.send(Event::KvResult {
            kv_id,
            value: None,
            entries,
            error,
        });
    }

    #[allow(deprecated)] // The M2 boundary intentionally mirrors the pinned core KV surface.
    async fn kv_put(&self, kv_id: u64, aid: String, key: Vec<u8>, value: Vec<u8>) {
        let result = match self.actor_current(&aid) {
            Some(actor) => actor.ctx.kv().put(&key, &value).await,
            None => {
                self.send_kv_actor_not_found(kv_id, &aid);
                return;
            }
        };
        self.send_empty_kv_result(kv_id, "kv_put_failed", result);
    }

    #[allow(deprecated)] // The M2 boundary intentionally mirrors the pinned core KV surface.
    async fn kv_delete(&self, kv_id: u64, aid: String, key: Vec<u8>) {
        let result = match self.actor_current(&aid) {
            Some(actor) => actor.ctx.kv().delete(&key).await,
            None => {
                self.send_kv_actor_not_found(kv_id, &aid);
                return;
            }
        };
        self.send_empty_kv_result(kv_id, "kv_delete_failed", result);
    }

    fn send_empty_kv_result(&self, kv_id: u64, code: &str, result: Result<()>) {
        let error = result
            .err()
            .map(|error| WireError::new(code, format!("{error:#}")));
        let _ = self.events.send(Event::KvResult {
            kv_id,
            value: None,
            entries: Vec::new(),
            error,
        });
    }

    fn send_kv_actor_not_found(&self, kv_id: u64, aid: &str) {
        let _ = self.events.send(Event::KvResult {
            kv_id,
            value: None,
            entries: Vec::new(),
            error: Some(WireError::new(
                "actor_not_found",
                format!("actor `{aid}` is not active"),
            )),
        });
    }

    fn actor_exact(&self, identity: &ActorIdentity) -> Option<ActiveActor> {
        self.actors
            .lock()
            .expect("active actor table poisoned")
            .get(identity)
            .cloned()
    }

    fn actor_current(&self, aid: &str) -> Option<ActiveActor> {
        self.actors
            .lock()
            .expect("active actor table poisoned")
            .iter()
            .filter(|(identity, _)| identity.aid == aid)
            .max_by_key(|(identity, _)| identity.generation)
            .map(|(_, actor)| actor.clone())
    }
}

fn shutdown_reason(reason: ShutdownKind) -> &'static str {
    match reason {
        ShutdownKind::Sleep => "sleep",
        ShutdownKind::Destroy => "destroy",
    }
}
