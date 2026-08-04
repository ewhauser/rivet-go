//! Callback-free proxy from rivetkit-core actor factories to the Go pump.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::{Sender, bounded};
use rivet_error::{RivetError, RivetErrorKind};
use rivetkit_core::actor::ShutdownKind;
use rivetkit_core::{
    ActionDefinition, ActorConfig, ActorContext, ActorEvent, ActorFactory, ActorStart,
    CanHibernateWebSocket, ConnHandle, CoreRegistry, ListOpts, Response, StateDelta, WebSocket,
    WsMessage, format_actor_key,
};
use serde::{Deserialize, Serialize};

use crate::correlation::{CorrelationError, CorrelationTable};
use crate::wire::{Command, Event, KvEntry, WireError};

const LIFECYCLE_RESULT_TIMEOUT: Duration = Duration::from_secs(30);
const SAVE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const ACTION_RESULT_TIMEOUT: Duration = Duration::from_secs(60);
const ALARM_RESULT_TIMEOUT: Duration = ACTION_RESULT_TIMEOUT;
// DatabaseKv polls workflow signals every 1.5 seconds at the pinned engine.
// Alarm and sleep are separate workflow signals, so hold the serialized alarm
// operation for two complete polls plus a one-second scheduling margin before
// allowing the Go handler to submit a later alarm or sleep checkpoint.
const ALARM_TRANSPORT_SETTLEMENT: Duration = Duration::from_millis(2 * 1_500 + 1_000);
const HTTP_RESULT_TIMEOUT: Duration = Duration::from_secs(30);
const WS_OPEN_RESULT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BODY_CHUNK: usize = 1 << 20;
const MAX_HTTP_HEADERS: usize = 256;
const MAX_KV_LIST_ENTRIES: u32 = 1_024;
const WS_OUTBOUND_QUEUE_CAPACITY: usize = 64;
const WS_BACKPRESSURE_CLOSE_CODE: u16 = 1013;
const WS_MESSAGE_ACK_TIMEOUT: Duration = Duration::from_secs(60);
const INTERNAL_ALARM_ACTION: &str = "__rivet_go_alarm";

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
    Alarm,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LifecycleKey {
    actor: ActorIdentity,
    kind: LifecycleKind,
}

#[derive(Clone)]
struct ActiveActor {
    identity: ActorIdentity,
    ctx: ActorContext,
    state_version: Arc<AtomicU64>,
    operations: ActorOperations,
    alarm_updates: ActorAlarmUpdates,
}

#[derive(Clone, Default)]
struct ActorAlarmUpdates {
    revision: Arc<AtomicU64>,
    apply: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Default)]
struct ActorOperationState {
    in_flight: usize,
    stopping: bool,
}

#[derive(Clone, Default)]
struct ActorOperations {
    state: Arc<Mutex<ActorOperationState>>,
    changed: Arc<tokio::sync::Notify>,
}

struct ActorOperationGuard {
    operations: ActorOperations,
}

impl ActorOperations {
    fn begin(&self) -> Option<ActorOperationGuard> {
        let mut state = self.state.lock().expect("actor operations table poisoned");
        if state.stopping {
            return None;
        }
        state.in_flight += 1;
        Some(ActorOperationGuard {
            operations: self.clone(),
        })
    }

    fn begin_stop(&self) {
        self.state
            .lock()
            .expect("actor operations table poisoned")
            .stopping = true;
    }

    async fn wait_idle(&self) {
        loop {
            let changed = self.changed.notified();
            if self
                .state
                .lock()
                .expect("actor operations table poisoned")
                .in_flight
                == 0
            {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for ActorOperationGuard {
    fn drop(&mut self) {
        let became_idle = {
            let mut state = self
                .operations
                .state
                .lock()
                .expect("actor operations table poisoned");
            state.in_flight -= 1;
            state.in_flight == 0
        };
        if became_idle {
            self.operations.changed.notify_waiters();
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct LifecycleResolution {
    error: Option<WireError>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ActionResolution {
    #[serde(default, with = "crate::wire::optional_bytes")]
    output: Option<Vec<u8>>,
    error: Option<WireError>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HttpResolution {
    status: u16,
    headers: BTreeMap<String, String>,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
    error: Option<WireError>,
}

struct HttpResponseAssembly {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
struct WsOpenResolution {
    error: Option<WireError>,
}

enum WsOutbound {
    Send(WsMessage),
    Close {
        code: Option<u16>,
        reason: Option<String>,
    },
}

#[derive(Clone)]
struct ActiveWebSocket {
    actor: ActorIdentity,
    can_hibernate: bool,
    ws: WebSocket,
    outbound: tokio::sync::mpsc::Sender<WsOutbound>,
    acknowledgements: Arc<Mutex<WsAcknowledgements>>,
}

#[derive(Default)]
struct WsAcknowledgements {
    last_received: Option<u16>,
    pending: VecDeque<PendingWsAcknowledgement>,
}

struct PendingWsAcknowledgement {
    msg_index: u16,
    completion: Option<Sender<()>>,
}

#[derive(Clone, Default)]
struct LifecyclePending {
    entries: Arc<Mutex<HashMap<LifecycleKey, u64>>>,
}

#[derive(Clone, Default)]
struct WsOpenPending {
    entries: Arc<Mutex<HashMap<String, u64>>>,
}

impl WsOpenPending {
    fn insert(&self, ws_id: &str, id: u64) -> Result<()> {
        let mut entries = self.entries.lock().expect("WebSocket open table poisoned");
        if entries.insert(ws_id.to_owned(), id).is_some() {
            return Err(anyhow!("duplicate pending WebSocket open result"));
        }
        Ok(())
    }

    fn resolve(
        &self,
        ws_id: &str,
        resolution: WsOpenResolution,
        correlations: &CorrelationTable,
    ) -> bool {
        let id = self
            .entries
            .lock()
            .expect("WebSocket open table poisoned")
            .remove(ws_id);
        let Some(id) = id else {
            return false;
        };
        let payload = rmp_serde::to_vec_named(&resolution)
            .expect("WsOpenResolution serialization is infallible");
        correlations.resolve(id, payload)
    }

    fn remove(&self, ws_id: &str) -> Option<u64> {
        self.entries
            .lock()
            .expect("WebSocket open table poisoned")
            .remove(ws_id)
    }

    fn retain_live(&self, correlations: &CorrelationTable) {
        self.entries
            .lock()
            .expect("WebSocket open table poisoned")
            .retain(|_, id| correlations.contains(*id));
    }

    fn clear(&self) {
        self.entries
            .lock()
            .expect("WebSocket open table poisoned")
            .clear();
    }
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
    pending_ws_open: WsOpenPending,
    actors: Arc<Mutex<HashMap<ActorIdentity, ActiveActor>>>,
    websockets: Arc<Mutex<HashMap<String, ActiveWebSocket>>>,
    restoring_websockets: Arc<Mutex<HashMap<String, ActorIdentity>>>,
    stop_intents: Arc<Mutex<HashSet<ActorIdentity>>>,
    active_http: Arc<Mutex<HashSet<u64>>>,
    http_responses: Arc<Mutex<HashMap<u64, HttpResponseAssembly>>>,
    runner_draining: Arc<AtomicBool>,
}

impl ActorProxy {
    pub(crate) fn new(events: Sender<Event>, correlations: CorrelationTable) -> Self {
        Self {
            events,
            correlations,
            pending: LifecyclePending::default(),
            pending_ws_open: WsOpenPending::default(),
            actors: Arc::new(Mutex::new(HashMap::new())),
            websockets: Arc::new(Mutex::new(HashMap::new())),
            restoring_websockets: Arc::new(Mutex::new(HashMap::new())),
            stop_intents: Arc::new(Mutex::new(HashSet::new())),
            active_http: Arc::new(Mutex::new(HashSet::new())),
            http_responses: Arc::new(Mutex::new(HashMap::new())),
            runner_draining: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn register(
        &self,
        registry: &mut CoreRegistry,
        actor_names: &[String],
        actor_actions: &BTreeMap<String, Vec<String>>,
    ) {
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
                no_sleep: false,
                can_hibernate_websocket: CanHibernateWebSocket::Bool(true),
                max_incoming_message_size: MAX_BODY_CHUNK as u32,
                max_outgoing_message_size: MAX_BODY_CHUNK as u32,
                action_timeout: ACTION_RESULT_TIMEOUT,
                actions: actor_actions
                    .get(actor_name)
                    .into_iter()
                    .flatten()
                    .map(|name| ActionDefinition { name: name.clone() })
                    .collect(),
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
                let key = LifecycleKey {
                    actor: ActorIdentity::new(aid, generation),
                    kind: LifecycleKind::Stop,
                };
                if let Some(actor) = self.actor_exact(&key.actor) {
                    actor.operations.begin_stop();
                    let pending = self.pending.clone();
                    let correlations = self.correlations.clone();
                    tokio::spawn(async move {
                        actor.operations.wait_idle().await;
                        pending.resolve(&key, LifecycleResolution { error }, &correlations);
                    });
                } else {
                    self.pending
                        .resolve(&key, LifecycleResolution { error }, &self.correlations);
                }
            }
            Command::AlarmHandled {
                aid,
                r#gen: generation,
                error,
            } => {
                self.pending.resolve(
                    &LifecycleKey {
                        actor: ActorIdentity::new(aid, generation),
                        kind: LifecycleKind::Alarm,
                    },
                    LifecycleResolution { error },
                    &self.correlations,
                );
            }
            Command::ActionResult {
                call_id,
                output,
                error,
            } => {
                let payload = rmp_serde::to_vec_named(&ActionResolution { output, error })
                    .expect("ActionResolution serialization is infallible");
                self.correlations.resolve(call_id, payload);
            }
            Command::HttpResponseStart {
                req_id,
                status,
                headers,
                body,
                stream,
                error,
            } => self.start_http_response(req_id, status, headers, body, stream, error),
            Command::HttpResponseChunk {
                req_id,
                body,
                finish,
            } => self.append_http_response(req_id, body, finish),
            Command::WsOpenResult {
                ws_id,
                accept: _,
                error,
            } => {
                self.pending_ws_open.resolve(
                    &ws_id,
                    WsOpenResolution { error },
                    &self.correlations,
                );
            }
            Command::WsMessageAck { ws_id, msg_index } => {
                self.ack_ws_message(&ws_id, msg_index);
            }
            Command::WsSend {
                ws_id,
                data,
                binary,
            } => self.send_ws(&ws_id, data, binary),
            Command::WsCloseCmd {
                ws_id,
                code,
                reason,
                hibernate,
            } => {
                if hibernate {
                    self.hibernate_ws(&ws_id);
                } else {
                    self.close_ws(&ws_id, code, reason);
                }
            }
            Command::Broadcast {
                aid,
                event,
                payload,
                exclude_conn,
            } => self.broadcast(&aid, &event, payload, exclude_conn.as_deref()),
            Command::StopIntent { aid } => {
                if let Some(actor) = self.actor_current(&aid) {
                    self.stop_intents
                        .lock()
                        .expect("stop intent table poisoned")
                        .insert(actor.identity.clone());
                    let _ = actor.ctx.stop_with_error("Go WebSocket handler panicked");
                }
            }
            Command::SetAlarm {
                op_id,
                aid,
                r#gen: generation,
                alarm_ts,
            } => self.dispatch_set_alarm(op_id, aid, generation, alarm_ts),
            Command::SleepIntent {
                op_id,
                aid,
                r#gen: generation,
            } => self.dispatch_sleep_intent(op_id, aid, generation),
            Command::SaveState {
                aid,
                r#gen: generation,
                state,
            } => self.dispatch_save_state(aid, generation, state),
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
        self.pending_ws_open.retain_live(&self.correlations);
        let expired = {
            let mut active = self.active_http.lock().expect("active HTTP table poisoned");
            let expired = active
                .iter()
                .filter(|req_id| !self.correlations.contains(**req_id))
                .copied()
                .collect::<Vec<_>>();
            for req_id in &expired {
                active.remove(req_id);
            }
            expired
        };
        let mut responses = self
            .http_responses
            .lock()
            .expect("HTTP response table poisoned");
        for req_id in expired {
            responses.remove(&req_id);
            let _ = self.events.send(Event::HttpRequestAbort { req_id });
        }
    }

    pub(crate) fn drain_shutdown(&self) {
        self.begin_shutdown();
        self.pending.clear();
        self.pending_ws_open.clear();
        self.close_all_websockets(Some(1001), Some("runner shutting down".to_owned()));
        self.active_http
            .lock()
            .expect("active HTTP table poisoned")
            .clear();
        self.http_responses
            .lock()
            .expect("HTTP response table poisoned")
            .clear();
    }

    pub(crate) fn begin_shutdown(&self) {
        self.runner_draining.store(true, Ordering::Release);
    }

    async fn run_actor(&self, start: ActorStart) -> Result<()> {
        let ActorStart {
            ctx,
            input,
            snapshot,
            hibernated,
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
                    identity: identity.clone(),
                    ctx: ctx.clone(),
                    state_version: Arc::new(AtomicU64::new(0)),
                    operations: ActorOperations::default(),
                    alarm_updates: ActorAlarmUpdates::default(),
                },
            );
        {
            let mut restoring = self
                .restoring_websockets
                .lock()
                .expect("restoring WebSocket table poisoned");
            for (conn, _) in hibernated {
                restoring.insert(conn.id().to_owned(), identity.clone());
            }
        }

        let result = self
            .run_actor_inner(
                &identity,
                &ctx,
                input.unwrap_or_default(),
                snapshot,
                &mut events,
                startup_ready,
            )
            .await;
        match &result {
            Ok(true) if !self.runner_draining.load(Ordering::Acquire) => {
                self.hibernate_actor_websockets(&identity);
            }
            Ok(_) | Err(_) => {
                let reason = if self.runner_draining.load(Ordering::Acquire) {
                    "runner shutting down"
                } else {
                    "actor stopped"
                };
                self.close_actor_websockets(&identity, Some(1001), Some(reason.to_owned()));
            }
        }
        self.restoring_websockets
            .lock()
            .expect("restoring WebSocket table poisoned")
            .retain(|_, owner| owner != &identity);
        self.stop_intents
            .lock()
            .expect("stop intent table poisoned")
            .remove(&identity);
        self.actors
            .lock()
            .expect("active actor table poisoned")
            .remove(&identity);
        result.map(|_| ())
    }

    async fn run_actor_inner(
        &self,
        identity: &ActorIdentity,
        ctx: &ActorContext,
        input: Vec<u8>,
        persisted_state: Option<Vec<u8>>,
        events: &mut rivetkit_core::ActorEvents,
        startup_ready: Option<tokio::sync::oneshot::Sender<Result<()>>>,
    ) -> Result<bool> {
        let mut slept = false;
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
                    let stop_reason = self.actor_stop_reason(identity, reason);
                    slept = matches!(reason, ShutdownKind::Sleep);
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
                                reason: stop_reason,
                            },
                            Some(shutdown_deadline),
                        )
                        .await;
                    match result {
                        Ok(()) => reply.send(Ok(())),
                        Err(error) => {
                            // At v2.3.10 core logs graceful-cleanup reply errors but
                            // otherwise completes a requested destroy. Ending the
                            // factory future with the same failure keeps the error on
                            // core's structured run-handler path while teardown ends.
                            reply.send(Err(anyhow!("{error:#}")));
                            return Err(error);
                        }
                    }
                }
                ActorEvent::Action {
                    name,
                    args,
                    conn,
                    scheduled_fire,
                    reply,
                    ..
                } => {
                    if name == INTERNAL_ALARM_ACTION {
                        let Some(fire) = scheduled_fire else {
                            reply.send(Err(rivetkit_core::error::action_not_found(&name)));
                            continue;
                        };
                        let resolution = self.request_alarm(identity, fire.scheduled_at).await;
                        match resolution {
                            Ok(()) => reply.send(Ok(cbor_null())),
                            Err(error) => {
                                let fatal = error.code == "handler_panic";
                                reply.send(Err(actor_wire_error(&error)));
                                if fatal {
                                    return Err(anyhow!(
                                        "Go alarm handler panicked: {}",
                                        error.message
                                    ));
                                }
                            }
                        }
                        continue;
                    }
                    let resolution = self
                        .request_action(
                            identity,
                            name.clone(),
                            args,
                            conn.map(|conn| conn.id().to_owned()),
                        )
                        .await;
                    match resolution {
                        Ok(output) => reply.send(Ok(output)),
                        Err(error) => {
                            let fatal = error.code == "handler_panic";
                            reply.send(Err(action_wire_error(&name, &error)));
                            if fatal {
                                return Err(anyhow!(
                                    "Go action handler panicked: {}",
                                    error.message
                                ));
                            }
                        }
                    }
                }
                ActorEvent::HttpRequest { request, reply } => {
                    let resolution = self.request_http(identity, request).await;
                    match resolution {
                        Ok(response) => reply.send(Ok(response)),
                        Err(error) => {
                            let fatal = error.code == "handler_panic";
                            reply.send(Err(actor_wire_error(&error)));
                            if fatal {
                                return Err(anyhow!("Go HTTP handler panicked: {}", error.message));
                            }
                        }
                    }
                }
                ActorEvent::QueueSend { reply, .. } => {
                    reply.send(Err(anyhow!("queue requests are outside the M5 surface")));
                }
                ActorEvent::ConnectionPreflight { reply, .. }
                | ActorEvent::ConnectionOpen { reply, .. } => reply.send(Ok(())),
                ActorEvent::WebSocketOpen {
                    conn,
                    ws,
                    request,
                    reply,
                } => match self.request_ws_open(identity, conn, ws, request).await {
                    Ok(()) => reply.send(Ok(())),
                    Err(error) => reply.send(Err(actor_wire_error(&error))),
                },
                ActorEvent::SubscribeRequest { reply, .. } => {
                    reply.send(Ok(()));
                }
                ActorEvent::DisconnectConn { reply, .. } => reply.send(Ok(())),
                ActorEvent::ConnectionClosed { conn } => {
                    self.websocket_closed(conn.id(), None, None);
                }
                ActorEvent::WorkflowHistoryRequested { reply }
                | ActorEvent::WorkflowReplayRequested { reply, .. } => reply.send(Ok(None)),
            }
        }
        Ok(slept)
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

    async fn request_alarm(
        &self,
        identity: &ActorIdentity,
        alarm_ts: i64,
    ) -> Result<(), WireError> {
        let key = LifecycleKey {
            actor: identity.clone(),
            kind: LifecycleKind::Alarm,
        };
        let (id, receiver) = self.correlations.insert(ALARM_RESULT_TIMEOUT);
        self.pending
            .insert(key.clone(), id)
            .map_err(|error| WireError::new("alarm_correlation_failed", error.to_string()))?;
        if self
            .events
            .send(Event::ActorAlarm {
                aid: identity.aid.clone(),
                r#gen: identity.generation,
                alarm_ts,
            })
            .is_err()
        {
            self.pending.remove(&key);
            self.correlations.resolve(
                id,
                rmp_serde::to_vec_named(&LifecycleResolution {
                    error: Some(WireError::new("runner_stopped", "Go event queue is closed")),
                })
                .expect("encode alarm queue error"),
            );
        }

        let payload = receiver
            .await
            .map_err(|_| WireError::new("runner_stopped", "alarm correlation sender dropped"))?
            .map_err(|error| match error {
                CorrelationError::Timeout => WireError::new(
                    "alarm_handler_timed_out",
                    "Go OnAlarm did not complete before the boundary deadline",
                ),
                CorrelationError::Shutdown => {
                    WireError::new("runner_stopped", "runner stopped during OnAlarm")
                }
            })?;
        self.pending.remove(&key);
        let resolution: LifecycleResolution = rmp_serde::from_slice(&payload)
            .map_err(|error| WireError::new("alarm_result_invalid", error.to_string()))?;
        if let Some(error) = resolution.error {
            return Err(error);
        }
        Ok(())
    }

    async fn execute_actor_command(&self, command: Command) {
        match command {
            Command::SaveState {
                aid: _,
                r#gen: _,
                state: _,
            } => {}
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
            | Command::AlarmHandled { .. }
            | Command::ActionResult { .. }
            | Command::HttpResponseStart { .. }
            | Command::HttpResponseChunk { .. }
            | Command::WsOpenResult { .. }
            | Command::WsMessageAck { .. }
            | Command::WsSend { .. }
            | Command::WsCloseCmd { .. }
            | Command::Broadcast { .. }
            | Command::StopIntent { .. }
            | Command::SetAlarm { .. }
            | Command::SleepIntent { .. }
            | Command::Unknown => {}
        }
    }

    fn dispatch_set_alarm(&self, op_id: u64, aid: String, generation: u64, alarm_ts: Option<i64>) {
        let identity = ActorIdentity::new(aid.clone(), generation);
        let Some(actor) = self.actor_exact(&identity) else {
            self.emit_actor_intent_result(
                op_id,
                Err(WireError::new(
                    "actor_generation_stale",
                    format!("actor {aid} generation {generation} is not active"),
                )),
            );
            return;
        };
        let Some(operation) = actor.operations.begin() else {
            self.emit_actor_intent_result(
                op_id,
                Err(WireError::new(
                    "actor_stopping",
                    format!("actor {aid} generation {generation} is stopping"),
                )),
            );
            return;
        };
        let revision = actor.alarm_updates.revision.fetch_add(1, Ordering::AcqRel) + 1;
        let proxy = self.clone();
        tokio::spawn(async move {
            let _operation = operation;
            let _apply = actor.alarm_updates.apply.lock().await;
            if actor.alarm_updates.revision.load(Ordering::Acquire) != revision {
                proxy.emit_actor_intent_result(
                    op_id,
                    Err(WireError::new(
                        "alarm_superseded",
                        "alarm update was superseded by a newer request",
                    )),
                );
                return;
            }
            let scheduled = match actor.ctx.list_scheduled_events().await {
                Ok(scheduled) => scheduled,
                Err(error) => {
                    proxy.emit_actor_intent_result(
                        op_id,
                        Err(WireError::new("alarm_list_failed", error.to_string())),
                    );
                    return;
                }
            };
            for event in scheduled {
                if event.action != INTERNAL_ALARM_ACTION {
                    continue;
                }
                if let Err(error) = actor.ctx.cancel_schedule(&event.id).await {
                    proxy.emit_actor_intent_result(
                        op_id,
                        Err(WireError::new("alarm_clear_failed", error.to_string())),
                    );
                    return;
                }
            }
            if actor.alarm_updates.revision.load(Ordering::Acquire) != revision {
                proxy.emit_actor_intent_result(
                    op_id,
                    Err(WireError::new(
                        "alarm_superseded",
                        "alarm update was superseded by a newer request",
                    )),
                );
                return;
            }
            if let Some(alarm_ts) = alarm_ts
                && let Err(error) = actor
                    .ctx
                    .at(alarm_ts, INTERNAL_ALARM_ACTION, &cbor_empty_args())
                    .await
            {
                proxy.emit_actor_intent_result(
                    op_id,
                    Err(WireError::new("alarm_set_failed", error.to_string())),
                );
                return;
            }
            tokio::time::sleep(ALARM_TRANSPORT_SETTLEMENT).await;
            if actor.alarm_updates.revision.load(Ordering::Acquire) != revision {
                proxy.emit_actor_intent_result(
                    op_id,
                    Err(WireError::new(
                        "alarm_superseded",
                        "alarm update was superseded by a newer request",
                    )),
                );
                return;
            }
            proxy.emit_actor_intent_result(op_id, Ok(()));
        });
    }

    fn dispatch_sleep_intent(&self, op_id: u64, aid: String, generation: u64) {
        let identity = ActorIdentity::new(aid.clone(), generation);
        let Some(actor) = self.actor_exact(&identity) else {
            self.emit_actor_intent_result(
                op_id,
                Err(WireError::new(
                    "actor_generation_stale",
                    format!("actor {aid} generation {generation} is not active"),
                )),
            );
            return;
        };
        // Sleep is accepted now but applied only after work already admitted
        // through this actor proxy is idle. Hibernatable raw WebSocket work
        // stays admitted until Go has returned its matching FIFO ack; core then
        // persists and acknowledges that exact message index.
        self.emit_actor_intent_result(op_id, Ok(()));
        tokio::spawn(async move {
            actor.operations.wait_idle().await;
            // The result above acknowledges admission of the intent. Once the
            // exact generation and its already-admitted work are fenced, core
            // owns the sleep transition and reports it through ActorStop.
            let _ = actor.ctx.sleep();
        });
    }

    fn emit_actor_intent_result(&self, op_id: u64, result: Result<(), WireError>) {
        let _ = self.events.send(Event::ActorIntentResult {
            op_id,
            error: result.err(),
        });
    }

    fn actor_stop_reason(&self, identity: &ActorIdentity, reason: ShutdownKind) -> String {
        if matches!(reason, ShutdownKind::Destroy)
            && self
                .stop_intents
                .lock()
                .expect("stop intent table poisoned")
                .remove(identity)
        {
            return "stop".to_owned();
        }
        shutdown_reason(reason).to_owned()
    }

    async fn request_action(
        &self,
        identity: &ActorIdentity,
        action: String,
        args: Vec<u8>,
        conn_id: Option<String>,
    ) -> Result<Vec<u8>, WireError> {
        if args.len() > MAX_BODY_CHUNK {
            return Err(WireError::new(
                "action_args_too_large",
                format!("action arguments exceed the {MAX_BODY_CHUNK}-byte boundary maximum"),
            ));
        }
        let (call_id, receiver) = self.correlations.insert(ACTION_RESULT_TIMEOUT);
        if self
            .events
            .send(Event::ActionCall {
                aid: identity.aid.clone(),
                r#gen: identity.generation,
                call_id,
                action,
                timeout_ms: ACTION_RESULT_TIMEOUT
                    .as_millis()
                    .try_into()
                    .expect("M3 action timeout fits u32 milliseconds"),
                args,
                conn_id,
            })
            .is_err()
        {
            self.correlations.resolve(
                call_id,
                rmp_serde::to_vec_named(&ActionResolution {
                    output: None,
                    error: Some(WireError::new("runner_stopped", "Go event queue is closed")),
                })
                .expect("encode action queue error"),
            );
        }
        let payload = receiver
            .await
            .map_err(|_| WireError::new("runner_stopped", "action correlation sender dropped"))?
            .map_err(correlation_wire_error)?;
        let resolution: ActionResolution = rmp_serde::from_slice(&payload)
            .map_err(|error| WireError::new("action_result_invalid", error.to_string()))?;
        match (resolution.output, resolution.error) {
            (Some(output), None) => Ok(output),
            (None, Some(error)) => Err(error),
            _ => Err(WireError::new(
                "action_result_invalid",
                "action result must contain exactly one of output or error",
            )),
        }
    }

    async fn request_http(
        &self,
        identity: &ActorIdentity,
        request: rivetkit_core::Request,
    ) -> Result<Response, WireError> {
        let (method, path, headers, body) = request.to_parts();
        let headers = headers.into_iter().collect::<BTreeMap<_, _>>();
        if headers.len() > MAX_HTTP_HEADERS {
            return Err(WireError::new(
                "http_request_headers_too_large",
                format!("HTTP request has more than {MAX_HTTP_HEADERS} headers"),
            ));
        }
        if headers
            .iter()
            .any(|(name, value)| name.len() > MAX_BODY_CHUNK || value.len() > MAX_BODY_CHUNK)
        {
            return Err(WireError::new(
                "http_request_header_too_large",
                format!("HTTP request header exceeds the {MAX_BODY_CHUNK}-byte boundary maximum"),
            ));
        }
        let (req_id, receiver) = self.correlations.insert(HTTP_RESULT_TIMEOUT);
        self.active_http
            .lock()
            .expect("active HTTP table poisoned")
            .insert(req_id);

        let first_len = body.len().min(MAX_BODY_CHUNK);
        let stream = first_len < body.len();
        if self
            .events
            .send(Event::HttpRequest {
                aid: identity.aid.clone(),
                r#gen: identity.generation,
                req_id,
                method,
                path,
                headers,
                body: body[..first_len].to_vec(),
                stream,
            })
            .is_err()
        {
            self.resolve_http_error(
                req_id,
                WireError::new("runner_stopped", "Go event queue is closed"),
            );
        } else if stream {
            let remaining = &body[first_len..];
            let chunk_count = remaining.len().div_ceil(MAX_BODY_CHUNK);
            for (index, chunk) in remaining.chunks(MAX_BODY_CHUNK).enumerate() {
                if self
                    .events
                    .send(Event::HttpRequestChunk {
                        req_id,
                        body: chunk.to_vec(),
                        finish: index + 1 == chunk_count,
                    })
                    .is_err()
                {
                    self.resolve_http_error(
                        req_id,
                        WireError::new("runner_stopped", "Go event queue is closed"),
                    );
                    break;
                }
            }
        }

        let result = receiver.await;
        self.active_http
            .lock()
            .expect("active HTTP table poisoned")
            .remove(&req_id);
        self.http_responses
            .lock()
            .expect("HTTP response table poisoned")
            .remove(&req_id);
        let payload = match result {
            Ok(Ok(payload)) => payload,
            Ok(Err(error)) => {
                let _ = self.events.send(Event::HttpRequestAbort { req_id });
                return Err(correlation_wire_error(error));
            }
            Err(_) => {
                let _ = self.events.send(Event::HttpRequestAbort { req_id });
                return Err(WireError::new(
                    "runner_stopped",
                    "HTTP correlation sender dropped",
                ));
            }
        };
        let resolution: HttpResolution = rmp_serde::from_slice(&payload)
            .map_err(|error| WireError::new("http_response_invalid", error.to_string()))?;
        if let Some(error) = resolution.error {
            return Err(error);
        }
        Response::from_parts(
            resolution.status,
            resolution.headers.into_iter().collect(),
            resolution.body,
        )
        .map_err(|error| WireError::new("http_response_invalid", format!("{error:#}")))
    }

    async fn request_ws_open(
        &self,
        identity: &ActorIdentity,
        conn: ConnHandle,
        ws: WebSocket,
        request: Option<rivetkit_core::Request>,
    ) -> Result<(), WireError> {
        let ws_id = conn.id().to_owned();
        let (path, headers) = if let Some(request) = request {
            let (_, path, headers, _) = request.to_parts();
            (path, headers.into_iter().collect::<BTreeMap<_, _>>())
        } else {
            ("/".to_owned(), BTreeMap::new())
        };
        if headers.len() > MAX_HTTP_HEADERS {
            return Err(WireError::new(
                "ws_open_headers_too_large",
                format!("WebSocket open has more than {MAX_HTTP_HEADERS} headers"),
            ));
        }
        if headers
            .iter()
            .any(|(name, value)| name.len() > MAX_BODY_CHUNK || value.len() > MAX_BODY_CHUNK)
        {
            return Err(WireError::new(
                "ws_open_header_too_large",
                format!("WebSocket header exceeds the {MAX_BODY_CHUNK}-byte boundary maximum"),
            ));
        }
        let resumed = self
            .restoring_websockets
            .lock()
            .expect("restoring WebSocket table poisoned")
            .remove(&ws_id)
            .is_some_and(|owner| owner == *identity);

        let (outbound, receiver) = tokio::sync::mpsc::channel(WS_OUTBOUND_QUEUE_CAPACITY);
        tokio::spawn(websocket_outbound_loop(ws.clone(), receiver));
        let active = ActiveWebSocket {
            actor: identity.clone(),
            can_hibernate: conn.is_hibernatable(),
            ws: ws.clone(),
            outbound,
            acknowledgements: Arc::new(Mutex::new(WsAcknowledgements::default())),
        };
        {
            let mut websockets = self.websockets.lock().expect("WebSocket table poisoned");
            if websockets.insert(ws_id.clone(), active).is_some() {
                return Err(WireError::new(
                    "ws_duplicate",
                    format!("WebSocket `{ws_id}` is already active"),
                ));
            }
        }

        let message_proxy = self.clone();
        let message_ws_id = ws_id.clone();
        ws.configure_message_event_callback(Some(Arc::new(move |message, msg_index| {
            message_proxy.websocket_message(&message_ws_id, message, msg_index.unwrap_or(0))
        })));
        let close_proxy = self.clone();
        let close_ws_id = ws_id.clone();
        ws.configure_close_event_callback(Some(Arc::new(move |code, reason, _was_clean| {
            close_proxy.websocket_closed(&close_ws_id, Some(code), Some(reason));
            Box::pin(async { Ok(()) })
        })));

        let (correlation_id, receiver) = self.correlations.insert(WS_OPEN_RESULT_TIMEOUT);
        if let Err(error) = self.pending_ws_open.insert(&ws_id, correlation_id) {
            self.close_ws(
                &ws_id,
                Some(1011),
                Some("actor.ws_open_duplicate".to_owned()),
            );
            return Err(WireError::new("ws_open_duplicate", error.to_string()));
        }
        if self
            .events
            .send(Event::WsOpen {
                aid: identity.aid.clone(),
                ws_id: ws_id.clone(),
                path,
                headers,
                can_hibernate: conn.is_hibernatable(),
                resumed,
            })
            .is_err()
        {
            self.pending_ws_open.remove(&ws_id);
            self.close_ws(
                &ws_id,
                Some(1011),
                Some("runner event queue closed".to_owned()),
            );
            self.correlations.resolve(
                correlation_id,
                rmp_serde::to_vec_named(&WsOpenResolution {
                    error: Some(WireError::new("runner_stopped", "Go event queue is closed")),
                })
                .expect("encode WebSocket open queue error"),
            );
        }

        let payload = receiver
            .await
            .map_err(|_| WireError::new("runner_stopped", "WebSocket open result sender dropped"))?
            .map_err(|error| match error {
                CorrelationError::Timeout => WireError::new(
                    "ws_open_timed_out",
                    "Go OnConnect did not complete before the boundary deadline",
                ),
                CorrelationError::Shutdown => {
                    WireError::new("runner_stopped", "runner stopped during WebSocket open")
                }
            })?;
        self.pending_ws_open.remove(&ws_id);
        let resolution: WsOpenResolution = rmp_serde::from_slice(&payload)
            .map_err(|error| WireError::new("ws_open_result_invalid", error.to_string()))?;
        if let Some(error) = resolution.error {
            self.close_ws(&ws_id, Some(1008), Some(format!("actor.{}", error.code)));
            // The raw hibernatable WebSocket adapter in core v2.3.10 closes
            // every failed open callback with 1011. The Go boundary has
            // already translated the actor-authored rejection to its public
            // 1008 close here, so report a handled callback to core and let
            // the queued close preserve that contract. Fatal handler errors
            // independently submit StopIntent from the Go pump.
            return Ok(());
        }
        Ok(())
    }

    fn websocket_message(&self, ws_id: &str, message: WsMessage, msg_index: u16) -> Result<()> {
        let (data, binary) = match message {
            WsMessage::Text(text) => (text.into_bytes(), false),
            WsMessage::Binary(data) => (data, true),
        };
        if data.len() > MAX_BODY_CHUNK {
            self.close_ws(
                ws_id,
                Some(1009),
                Some("message.incoming_too_long".to_owned()),
            );
            return Ok(());
        }
        let (acknowledgements, can_hibernate, actor_identity) = {
            let websockets = self.websockets.lock().expect("WebSocket table poisoned");
            let Some(active) = websockets.get(ws_id) else {
                return Ok(());
            };
            (
                active.acknowledgements.clone(),
                active.can_hibernate,
                active.actor.clone(),
            )
        };
        let _actor_operation = self
            .actor_exact(&actor_identity)
            .and_then(|actor| actor.operations.begin());
        let (completion, completed) = if can_hibernate {
            let (tx, rx) = bounded(1);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        {
            let mut acknowledgements = acknowledgements
                .lock()
                .expect("WebSocket acknowledgement table poisoned");
            if acknowledgements
                .last_received
                .is_some_and(|last| last.wrapping_add(1) != msg_index)
            {
                drop(acknowledgements);
                self.close_ws(ws_id, Some(1008), Some("ws.message_index_skip".to_owned()));
                return Ok(());
            }
            acknowledgements.last_received = Some(msg_index);
            acknowledgements
                .pending
                .push_back(PendingWsAcknowledgement {
                    msg_index,
                    completion,
                });
        }
        self.events
            .send(Event::WsMessage {
                ws_id: ws_id.to_owned(),
                data,
                binary,
                msg_index,
            })
            .map_err(|_| anyhow!("Go event queue is closed"))?;
        if let Some(completed) = completed {
            let waited =
                tokio::task::block_in_place(|| completed.recv_timeout(WS_MESSAGE_ACK_TIMEOUT));
            if waited.is_err() {
                self.close_ws(
                    ws_id,
                    Some(1011),
                    Some("ws.handler_ack_timed_out".to_owned()),
                );
                return Err(anyhow!("Go WebSocket handler acknowledgement timed out"));
            }
        }
        Ok(())
    }

    fn websocket_closed(&self, ws_id: &str, code: Option<u16>, reason: Option<String>) {
        let removed = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .remove(ws_id);
        let Some(removed) = removed else {
            return;
        };
        clear_ws_acknowledgements(&removed);
        if let Some(correlation_id) = self.pending_ws_open.remove(ws_id) {
            self.correlations.resolve(
                correlation_id,
                rmp_serde::to_vec_named(&WsOpenResolution {
                    error: Some(WireError::new(
                        "ws_closed_during_open",
                        "WebSocket closed before OnConnect completed",
                    )),
                })
                .expect("encode WebSocket close during open"),
            );
        }
        let _ = self.events.send(Event::WsClose {
            ws_id: ws_id.to_owned(),
            code,
            reason,
        });
    }

    fn ack_ws_message(&self, ws_id: &str, msg_index: u16) {
        let active = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .get(ws_id)
            .cloned();
        let Some(active) = active else {
            return;
        };
        let acknowledged = {
            let mut acknowledgements = active
                .acknowledgements
                .lock()
                .expect("WebSocket acknowledgement table poisoned");
            if acknowledgements
                .pending
                .front()
                .is_some_and(|pending| pending.msg_index == msg_index)
            {
                acknowledgements.pending.pop_front()
            } else {
                None
            }
        };
        let Some(acknowledged) = acknowledged else {
            self.close_ws(ws_id, Some(1008), Some("ws.ack_out_of_order".to_owned()));
            return;
        };
        if let Some(completion) = acknowledged.completion {
            let _ = completion.send(());
        }
    }

    fn send_ws(&self, ws_id: &str, data: Vec<u8>, binary: bool) {
        let message = if binary {
            WsMessage::Binary(data)
        } else {
            match String::from_utf8(data) {
                Ok(text) => WsMessage::Text(text),
                Err(_) => return,
            }
        };
        self.enqueue_ws(ws_id, WsOutbound::Send(message));
    }

    fn enqueue_ws(&self, ws_id: &str, outbound: WsOutbound) {
        let sender = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .get(ws_id)
            .map(|active| active.outbound.clone());
        let Some(sender) = sender else {
            return;
        };
        match sender.try_send(outbound) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => self.close_ws(
                ws_id,
                Some(WS_BACKPRESSURE_CLOSE_CODE),
                Some("outbound_backpressure".to_owned()),
            ),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.websocket_closed(ws_id, Some(1011), Some("outbound_sender_closed".to_owned()));
            }
        }
    }

    fn close_ws(&self, ws_id: &str, code: Option<u16>, reason: Option<String>) {
        let active = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .remove(ws_id);
        let Some(active) = active else {
            return;
        };
        clear_ws_acknowledgements(&active);
        if let Some(correlation_id) = self.pending_ws_open.remove(ws_id) {
            self.correlations.resolve(
                correlation_id,
                rmp_serde::to_vec_named(&WsOpenResolution {
                    error: Some(WireError::new(
                        "ws_closed_during_open",
                        "WebSocket closed before OnConnect completed",
                    )),
                })
                .expect("encode WebSocket close during open"),
            );
        }
        let close = WsOutbound::Close {
            code,
            reason: reason.clone(),
        };
        if active.outbound.try_send(close).is_err() {
            let ws = active.ws.clone();
            let direct_reason = reason.clone();
            tokio::spawn(async move { ws.close(code, direct_reason).await });
        }
        let _ = self.events.send(Event::WsClose {
            ws_id: ws_id.to_owned(),
            code,
            reason,
        });
    }

    fn hibernate_ws(&self, ws_id: &str) {
        let active = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .remove(ws_id);
        let Some(active) = active else {
            return;
        };
        if !active.can_hibernate {
            self.websockets
                .lock()
                .expect("WebSocket table poisoned")
                .insert(ws_id.to_owned(), active);
            self.close_ws(ws_id, Some(1001), Some("actor sleeping".to_owned()));
            return;
        }
        clear_ws_acknowledgements(&active);
        if let Some(correlation_id) = self.pending_ws_open.remove(ws_id) {
            self.correlations.resolve(
                correlation_id,
                rmp_serde::to_vec_named(&WsOpenResolution {
                    error: Some(WireError::new(
                        "ws_hibernated_during_open",
                        "WebSocket hibernated before OnConnect completed",
                    )),
                })
                .expect("encode WebSocket hibernation during open"),
            );
        }
        // Dropping the outbound sender detaches this generation's callbacks.
        // Core and the engine retain the hibernating gateway request and build
        // a fresh WebSocket transport when the actor wakes.
    }

    fn broadcast(&self, aid: &str, event: &str, payload: Vec<u8>, exclude_conn: Option<&str>) {
        if let Some(actor) = self.actor_current(aid) {
            actor.ctx.broadcast(event, &payload);
        }

        let frame = match crate::wire::encode_actor_connect_event_frame(event, &payload) {
            Ok(frame) => frame,
            Err(_) => return,
        };
        let ids = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .iter()
            .filter(|(ws_id, active)| {
                active.actor.aid == aid && exclude_conn != Some(ws_id.as_str())
            })
            .map(|(ws_id, _)| ws_id.clone())
            .collect::<Vec<_>>();
        for ws_id in ids {
            self.enqueue_ws(&ws_id, WsOutbound::Send(WsMessage::Binary(frame.clone())));
        }
    }

    fn close_actor_websockets(
        &self,
        identity: &ActorIdentity,
        code: Option<u16>,
        reason: Option<String>,
    ) {
        let ids = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .iter()
            .filter(|(_, active)| active.actor == *identity)
            .map(|(ws_id, _)| ws_id.clone())
            .collect::<Vec<_>>();
        for ws_id in ids {
            self.close_ws(&ws_id, code, reason.clone());
        }
    }

    fn hibernate_actor_websockets(&self, identity: &ActorIdentity) {
        let sockets = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .iter()
            .filter(|(_, active)| active.actor == *identity)
            .map(|(ws_id, active)| (ws_id.clone(), active.can_hibernate))
            .collect::<Vec<_>>();
        for (ws_id, can_hibernate) in sockets {
            if can_hibernate {
                self.hibernate_ws(&ws_id);
            } else {
                self.close_ws(&ws_id, Some(1001), Some("actor sleeping".to_owned()));
            }
        }
    }

    fn close_all_websockets(&self, code: Option<u16>, reason: Option<String>) {
        let ids = self
            .websockets
            .lock()
            .expect("WebSocket table poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for ws_id in ids {
            self.close_ws(&ws_id, code, reason.clone());
        }
    }

    fn start_http_response(
        &self,
        req_id: u64,
        status: u16,
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
        stream: bool,
        error: Option<WireError>,
    ) {
        if !self.correlations.contains(req_id) {
            return;
        }
        if let Some(error) = error {
            self.http_responses
                .lock()
                .expect("HTTP response table poisoned")
                .remove(&req_id);
            self.resolve_http_error(req_id, error);
            return;
        }
        if stream {
            let prior = self
                .http_responses
                .lock()
                .expect("HTTP response table poisoned")
                .insert(
                    req_id,
                    HttpResponseAssembly {
                        status,
                        headers,
                        body,
                    },
                );
            if prior.is_some() {
                self.resolve_http_error(
                    req_id,
                    WireError::new(
                        "http_response_invalid",
                        "HTTP response was started more than once",
                    ),
                );
            }
        } else {
            let payload = rmp_serde::to_vec_named(&HttpResolution {
                status,
                headers,
                body,
                error: None,
            })
            .expect("HttpResolution serialization is infallible");
            self.active_http
                .lock()
                .expect("active HTTP table poisoned")
                .remove(&req_id);
            self.correlations.resolve(req_id, payload);
        }
    }

    fn append_http_response(&self, req_id: u64, body: Vec<u8>, finish: bool) {
        if !self.correlations.contains(req_id) {
            return;
        }
        let completed = {
            let mut responses = self
                .http_responses
                .lock()
                .expect("HTTP response table poisoned");
            let Some(response) = responses.get_mut(&req_id) else {
                drop(responses);
                self.resolve_http_error(
                    req_id,
                    WireError::new(
                        "http_response_invalid",
                        "HTTP response chunk arrived before response start",
                    ),
                );
                return;
            };
            response.body.extend_from_slice(&body);
            finish.then(|| responses.remove(&req_id).expect("HTTP response exists"))
        };
        if let Some(response) = completed {
            let payload = rmp_serde::to_vec_named(&HttpResolution {
                status: response.status,
                headers: response.headers,
                body: response.body,
                error: None,
            })
            .expect("HttpResolution serialization is infallible");
            self.active_http
                .lock()
                .expect("active HTTP table poisoned")
                .remove(&req_id);
            self.correlations.resolve(req_id, payload);
        }
    }

    fn resolve_http_error(&self, req_id: u64, error: WireError) {
        self.http_responses
            .lock()
            .expect("HTTP response table poisoned")
            .remove(&req_id);
        self.active_http
            .lock()
            .expect("active HTTP table poisoned")
            .remove(&req_id);
        let payload = rmp_serde::to_vec_named(&HttpResolution {
            status: 0,
            headers: BTreeMap::new(),
            body: Vec::new(),
            error: Some(error),
        })
        .expect("HttpResolution serialization is infallible");
        self.correlations.resolve(req_id, payload);
    }

    fn dispatch_save_state(&self, aid: String, generation: u64, state: Vec<u8>) {
        let identity = ActorIdentity::new(aid.clone(), generation);
        let Some(actor) = self.actor_exact(&identity) else {
            let _ = self.events.send(Event::StatePersisted {
                aid,
                r#gen: generation,
                state_version: 0,
                error: Some(WireError::new(
                    "actor_not_found",
                    format!("actor generation {generation} is not active"),
                )),
            });
            return;
        };
        let Some(operation) = actor.operations.begin() else {
            let _ = self.events.send(Event::StatePersisted {
                aid,
                r#gen: generation,
                state_version: actor.state_version.load(Ordering::Acquire),
                error: Some(WireError::new(
                    "actor_stopping",
                    "actor state cannot be saved after stop acknowledgement begins",
                )),
            });
            return;
        };
        let proxy = self.clone();
        tokio::spawn(async move {
            proxy
                .save_state(actor, operation, aid, generation, state)
                .await;
        });
    }

    async fn save_state(
        &self,
        actor: ActiveActor,
        _operation: ActorOperationGuard,
        aid: String,
        generation: u64,
        state: Vec<u8>,
    ) {
        let result = tokio::time::timeout(
            SAVE_STATE_TIMEOUT,
            actor.ctx.save_state(vec![StateDelta::ActorState(state)]),
        )
        .await;
        let (state_version, error) = match result {
            Ok(Ok(())) => (actor.state_version.fetch_add(1, Ordering::AcqRel) + 1, None),
            Ok(Err(error)) => (
                actor.state_version.load(Ordering::Acquire),
                Some(WireError::new("state_persist_failed", format!("{error:#}"))),
            ),
            Err(_) => (
                actor.state_version.load(Ordering::Acquire),
                Some(WireError::new(
                    "state_persist_timeout",
                    format!(
                        "actor state save did not complete within {} seconds",
                        SAVE_STATE_TIMEOUT.as_secs()
                    ),
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

fn clear_ws_acknowledgements(active: &ActiveWebSocket) {
    active
        .acknowledgements
        .lock()
        .expect("WebSocket acknowledgement table poisoned")
        .pending
        .clear();
}

fn cbor_empty_args() -> Vec<u8> {
    let mut encoded = Vec::new();
    ciborium::into_writer(&Vec::<ciborium::Value>::new(), &mut encoded)
        .expect("empty CBOR argument encoding is infallible");
    encoded
}

fn cbor_null() -> Vec<u8> {
    let mut encoded = Vec::new();
    ciborium::into_writer(&ciborium::Value::Null, &mut encoded)
        .expect("CBOR null encoding is infallible");
    encoded
}

fn correlation_wire_error(error: CorrelationError) -> WireError {
    match error {
        CorrelationError::Timeout => WireError::new(
            "action_timed_out",
            "Go handler did not complete before the core dispatch deadline",
        ),
        CorrelationError::Shutdown => WireError::new(
            "runner_stopped",
            "runner stopped while the Go handler was active",
        ),
    }
}

fn action_wire_error(action: &str, error: &WireError) -> anyhow::Error {
    if error.code == "action_not_found" {
        rivetkit_core::error::action_not_found(action)
    } else {
        actor_wire_error(error)
    }
}

fn actor_wire_error(error: &WireError) -> anyhow::Error {
    anyhow::Error::new(RivetError {
        kind: RivetErrorKind::Dynamic {
            group: "actor".to_owned(),
            code: error.code.clone(),
            default_message: error.message.clone(),
        },
        meta: None,
        message: Some(error.message.clone()),
        actor: None,
    })
}

async fn websocket_outbound_loop(
    ws: WebSocket,
    mut receiver: tokio::sync::mpsc::Receiver<WsOutbound>,
) {
    while let Some(outbound) = receiver.recv().await {
        match outbound {
            WsOutbound::Send(message) => ws.send(message),
            WsOutbound::Close { code, reason } => {
                ws.close(code, reason).await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crossbeam_channel::bounded;

    use super::*;

    #[tokio::test]
    async fn actor_stop_waits_for_reserved_state_operations() {
        let operations = ActorOperations::default();
        let first = operations.begin().expect("first state operation");
        let second = operations.begin().expect("second state operation");
        operations.begin_stop();
        assert!(operations.begin().is_none());

        let waiting = operations.clone();
        let waiter = tokio::spawn(async move { waiting.wait_idle().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(first);
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(second);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("stop waiter timed out")
            .expect("stop waiter task failed");
    }

    #[tokio::test]
    async fn action_result_resolves_its_exact_correlation() {
        let (events, _event_rx) = bounded(4);
        let correlations = CorrelationTable::default();
        let proxy = ActorProxy::new(events, correlations.clone());
        let (call_id, receiver) = correlations.insert(Duration::from_secs(1));

        proxy.handle_command(Command::ActionResult {
            call_id,
            output: Some(vec![0x18, 0x2a]),
            error: None,
        });

        let payload = receiver
            .await
            .expect("action correlation sender")
            .expect("action correlation result");
        let resolution: ActionResolution =
            rmp_serde::from_slice(&payload).expect("decode action resolution");
        assert_eq!(resolution.output, Some(vec![0x18, 0x2a]));
        assert_eq!(resolution.error, None);
        assert_eq!(correlations.len(), 0);
    }

    #[tokio::test]
    async fn websocket_open_correlation_is_removed_before_id_reuse() {
        let pending = WsOpenPending::default();
        let correlations = CorrelationTable::default();
        let (first_id, first_receiver) = correlations.insert(Duration::from_secs(1));
        pending.insert("reused", first_id).expect("first open");
        assert!(pending.resolve("reused", WsOpenResolution { error: None }, &correlations,));
        first_receiver
            .await
            .expect("first open sender")
            .expect("first open result");

        let (second_id, second_receiver) = correlations.insert(Duration::from_secs(1));
        pending.insert("reused", second_id).expect("reused open");
        assert!(pending.resolve("reused", WsOpenResolution { error: None }, &correlations,));
        second_receiver
            .await
            .expect("second open sender")
            .expect("second open result");
        assert_eq!(correlations.len(), 0);
    }

    #[tokio::test]
    async fn unknown_and_expired_action_results_do_not_resolve_a_live_call() {
        let (events, _event_rx) = bounded(4);
        let correlations = CorrelationTable::default();
        let proxy = ActorProxy::new(events, correlations.clone());
        let (expired_id, expired) = correlations.insert(Duration::ZERO);
        let (live_id, live) = correlations.insert(Duration::from_secs(1));

        assert_eq!(correlations.expire(Instant::now()), 1);
        assert_eq!(
            expired.await.expect("expired action correlation sender"),
            Err(CorrelationError::Timeout)
        );
        proxy.handle_command(Command::ActionResult {
            call_id: u64::MAX,
            output: Some(vec![0x01]),
            error: None,
        });
        proxy.handle_command(Command::ActionResult {
            call_id: expired_id,
            output: Some(vec![0x02]),
            error: None,
        });
        assert!(correlations.contains(live_id));

        proxy.handle_command(Command::ActionResult {
            call_id: live_id,
            output: Some(vec![0x03]),
            error: None,
        });
        let payload = live
            .await
            .expect("live action correlation sender")
            .expect("live action correlation result");
        let resolution: ActionResolution =
            rmp_serde::from_slice(&payload).expect("decode live action resolution");
        assert_eq!(resolution.output, Some(vec![0x03]));
        assert_eq!(correlations.len(), 0);
    }

    #[tokio::test]
    async fn streamed_http_commands_assemble_before_core_reply() {
        let (events, event_rx) = bounded(4);
        let correlations = CorrelationTable::default();
        let proxy = ActorProxy::new(events, correlations.clone());
        let (req_id, receiver) = correlations.insert(Duration::from_secs(1));
        proxy
            .active_http
            .lock()
            .expect("active HTTP table")
            .insert(req_id);

        proxy.handle_command(Command::HttpResponseStart {
            req_id,
            status: 201,
            headers: BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
            body: b"first".to_vec(),
            stream: true,
            error: None,
        });
        proxy.handle_command(Command::HttpResponseChunk {
            req_id,
            body: b"-second".to_vec(),
            finish: false,
        });
        proxy.handle_command(Command::HttpResponseChunk {
            req_id,
            body: b"-third".to_vec(),
            finish: true,
        });

        let payload = receiver
            .await
            .expect("HTTP correlation sender")
            .expect("HTTP correlation result");
        let resolution: HttpResolution =
            rmp_serde::from_slice(&payload).expect("decode HTTP resolution");
        assert_eq!(resolution.status, 201);
        assert_eq!(resolution.body, b"first-second-third");
        assert_eq!(resolution.headers["content-type"], "text/plain");
        assert_eq!(resolution.error, None);
        assert_eq!(correlations.len(), 0);
        proxy.sweep_pending();
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn expired_http_correlation_emits_abort() {
        let (events, event_rx) = bounded(4);
        let correlations = CorrelationTable::default();
        let proxy = ActorProxy::new(events, correlations.clone());
        let (req_id, receiver) = correlations.insert(Duration::ZERO);
        proxy
            .active_http
            .lock()
            .expect("active HTTP table")
            .insert(req_id);

        assert_eq!(correlations.expire(Instant::now()), 1);
        proxy.sweep_pending();
        assert_eq!(
            receiver.await.expect("HTTP correlation sender"),
            Err(CorrelationError::Timeout)
        );
        assert_eq!(
            event_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Event::HttpRequestAbort { req_id })
        );
        assert!(
            proxy
                .active_http
                .lock()
                .expect("active HTTP table")
                .is_empty()
        );
        assert!(
            proxy
                .http_responses
                .lock()
                .expect("HTTP response table")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn full_connection_queue_closes_only_that_websocket() {
        let (events, event_rx) = bounded(8);
        let proxy = ActorProxy::new(events, CorrelationTable::default());
        let identity = ActorIdentity::new("actor", 1);
        let (slow_tx, slow_rx) = tokio::sync::mpsc::channel(1);
        slow_tx
            .try_send(WsOutbound::Send(WsMessage::Text("blocked".to_owned())))
            .expect("fill slow WebSocket queue");
        let (fast_tx, mut fast_rx) = tokio::sync::mpsc::channel(1);
        let websocket = |outbound| ActiveWebSocket {
            actor: identity.clone(),
            can_hibernate: false,
            ws: WebSocket::new(),
            outbound,
            acknowledgements: Arc::new(Mutex::new(WsAcknowledgements::default())),
        };
        proxy.websockets.lock().expect("WebSocket table").extend([
            ("slow".to_owned(), websocket(slow_tx)),
            ("fast".to_owned(), websocket(fast_tx)),
        ]);

        proxy.broadcast("actor", "updated", vec![0x81, 0x01], None);

        assert!(
            !proxy
                .websockets
                .lock()
                .expect("WebSocket table")
                .contains_key("slow")
        );
        assert!(
            proxy
                .websockets
                .lock()
                .expect("WebSocket table")
                .contains_key("fast")
        );
        assert!(matches!(fast_rx.recv().await, Some(WsOutbound::Send(_))));
        assert_eq!(
            event_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Event::WsClose {
                ws_id: "slow".to_owned(),
                code: Some(WS_BACKPRESSURE_CLOSE_CODE),
                reason: Some("outbound_backpressure".to_owned()),
            })
        );
        drop(slow_rx);
    }

    #[test]
    fn non_hibernating_message_ack_clears_bookkeeping() {
        let (events, _event_rx) = bounded(4);
        let proxy = ActorProxy::new(events, CorrelationTable::default());
        let (outbound, _receiver) = tokio::sync::mpsc::channel(1);
        let acknowledgements = Arc::new(Mutex::new(WsAcknowledgements {
            last_received: Some(7),
            pending: VecDeque::from([PendingWsAcknowledgement {
                msg_index: 7,
                completion: None,
            }]),
        }));
        proxy.websockets.lock().expect("WebSocket table").insert(
            "ws".to_owned(),
            ActiveWebSocket {
                actor: ActorIdentity::new("actor", 1),
                can_hibernate: false,
                ws: WebSocket::new(),
                outbound,
                acknowledgements: acknowledgements.clone(),
            },
        );

        proxy.handle_command(Command::WsMessageAck {
            ws_id: "ws".to_owned(),
            msg_index: 7,
        });

        assert!(
            acknowledgements
                .lock()
                .expect("WebSocket acknowledgement table")
                .pending
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hibernating_message_waits_for_go_acknowledgement() {
        let (events, event_rx) = bounded(4);
        let proxy = ActorProxy::new(events, CorrelationTable::default());
        let (outbound, _receiver) = tokio::sync::mpsc::channel(1);
        proxy.websockets.lock().expect("WebSocket table").insert(
            "ws".to_owned(),
            ActiveWebSocket {
                actor: ActorIdentity::new("actor", 1),
                can_hibernate: true,
                ws: WebSocket::new(),
                outbound,
                acknowledgements: Arc::new(Mutex::new(WsAcknowledgements::default())),
            },
        );

        let callback_proxy = proxy.clone();
        let callback = tokio::spawn(async move {
            callback_proxy.websocket_message(
                "ws",
                WsMessage::Text("persist before ack".to_owned()),
                1,
            )
        });

        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Event::WsMessage { msg_index: 1, .. })
        ));
        assert!(!callback.is_finished());
        proxy.ack_ws_message("ws", 1);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), callback)
                .await
                .expect("callback completed after acknowledgement")
                .expect("message callback task")
                .is_ok()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hibernating_message_index_wraps_across_generations() {
        let (events, event_rx) = bounded(4);
        let proxy = ActorProxy::new(events, CorrelationTable::default());
        let (outbound, _receiver) = tokio::sync::mpsc::channel(1);
        proxy.websockets.lock().expect("WebSocket table").insert(
            "restored".to_owned(),
            ActiveWebSocket {
                actor: ActorIdentity::new("actor", 1),
                can_hibernate: true,
                ws: WebSocket::new(),
                outbound,
                acknowledgements: Arc::new(Mutex::new(WsAcknowledgements {
                    last_received: Some(u16::MAX - 1),
                    pending: VecDeque::new(),
                })),
            },
        );

        let first_proxy = proxy.clone();
        let first = tokio::spawn(async move {
            first_proxy.websocket_message(
                "restored",
                WsMessage::Text("before hibernation".to_owned()),
                u16::MAX,
            )
        });
        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Event::WsMessage {
                msg_index: u16::MAX,
                ..
            })
        ));
        proxy.ack_ws_message("restored", u16::MAX);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), first)
                .await
                .expect("pre-hibernation callback completed")
                .expect("pre-hibernation callback task")
                .is_ok()
        );

        proxy.hibernate_ws("restored");
        assert!(
            !proxy
                .websockets
                .lock()
                .expect("WebSocket table")
                .contains_key("restored")
        );

        let (outbound, _receiver) = tokio::sync::mpsc::channel(1);
        proxy.websockets.lock().expect("WebSocket table").insert(
            "restored".to_owned(),
            ActiveWebSocket {
                actor: ActorIdentity::new("actor", 2),
                can_hibernate: true,
                ws: WebSocket::new(),
                outbound,
                acknowledgements: Arc::new(Mutex::new(WsAcknowledgements::default())),
            },
        );
        let restored_proxy = proxy.clone();
        let restored = tokio::spawn(async move {
            restored_proxy.websocket_message(
                "restored",
                WsMessage::Text("after hibernation".to_owned()),
                0,
            )
        });
        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Event::WsMessage { msg_index: 0, .. })
        ));
        proxy.ack_ws_message("restored", 0);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), restored)
                .await
                .expect("restored callback completed")
                .expect("restored callback task")
                .is_ok()
        );
    }

    #[test]
    fn websocket_acknowledgements_are_ordered_and_wrap_monotonically() {
        let (events, event_rx) = bounded(8);
        let proxy = ActorProxy::new(events, CorrelationTable::default());
        let (outbound, mut receiver) = tokio::sync::mpsc::channel(4);
        let acknowledgements = Arc::new(Mutex::new(WsAcknowledgements {
            last_received: Some(u16::MAX - 1),
            pending: VecDeque::new(),
        }));
        proxy.websockets.lock().expect("WebSocket table").insert(
            "ws".to_owned(),
            ActiveWebSocket {
                actor: ActorIdentity::new("actor", 1),
                can_hibernate: false,
                ws: WebSocket::new(),
                outbound,
                acknowledgements: acknowledgements.clone(),
            },
        );

        proxy
            .websocket_message("ws", WsMessage::Text("first".to_owned()), u16::MAX)
            .expect("first message");
        proxy
            .websocket_message("ws", WsMessage::Text("wrapped".to_owned()), 0)
            .expect("wrapped message");
        assert!(matches!(
            event_rx.recv(),
            Ok(Event::WsMessage {
                msg_index: u16::MAX,
                ..
            })
        ));
        assert!(matches!(
            event_rx.recv(),
            Ok(Event::WsMessage { msg_index: 0, .. })
        ));

        proxy.ack_ws_message("ws", 0);
        assert!(
            !proxy
                .websockets
                .lock()
                .expect("WebSocket table")
                .contains_key("ws")
        );
        assert!(matches!(
            receiver.try_recv(),
            Ok(WsOutbound::Close {
                code: Some(1008),
                ..
            })
        ));

        let acknowledgement_state = acknowledgements
            .lock()
            .expect("WebSocket acknowledgement table");
        assert_eq!(acknowledgement_state.last_received, Some(0));
        assert!(acknowledgement_state.pending.is_empty());
    }

    #[test]
    fn alarm_handler_timeout_matches_the_core_action_deadline() {
        assert_eq!(ALARM_RESULT_TIMEOUT, ACTION_RESULT_TIMEOUT);
    }

    #[test]
    fn alarm_transport_settlement_covers_two_pinned_signal_polls() {
        assert!(ALARM_TRANSPORT_SETTLEMENT >= Duration::from_millis(2 * 1_500));
    }
}
