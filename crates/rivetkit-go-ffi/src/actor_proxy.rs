//! Callback-free proxy from rivetkit-core actor factories to the Go pump.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::Sender;
use rivet_error::{RivetError, RivetErrorKind};
use rivetkit_core::actor::ShutdownKind;
use rivetkit_core::{
    ActionDefinition, ActorConfig, ActorContext, ActorEvent, ActorFactory, ActorStart,
    CoreRegistry, ListOpts, Response, StateDelta, format_actor_key,
};
use serde::{Deserialize, Serialize};

use crate::correlation::{CorrelationError, CorrelationTable};
use crate::wire::{Command, Event, KvEntry, WireError};

const LIFECYCLE_RESULT_TIMEOUT: Duration = Duration::from_secs(30);
const SAVE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const ACTION_RESULT_TIMEOUT: Duration = Duration::from_secs(60);
const HTTP_RESULT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BODY_CHUNK: usize = 1 << 20;
const MAX_HTTP_HEADERS: usize = 256;
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
    operations: ActorOperations,
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
    active_http: Arc<Mutex<HashSet<u64>>>,
    http_responses: Arc<Mutex<HashMap<u64, HttpResponseAssembly>>>,
}

impl ActorProxy {
    pub(crate) fn new(events: Sender<Event>, correlations: CorrelationTable) -> Self {
        Self {
            events,
            correlations,
            pending: LifecyclePending::default(),
            actors: Arc::new(Mutex::new(HashMap::new())),
            active_http: Arc::new(Mutex::new(HashSet::new())),
            http_responses: Arc::new(Mutex::new(HashMap::new())),
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
                // Sleep/alarm behavior belongs to M5. Keeping M3 actors awake
                // makes lifecycle changes engine-driven and deterministic.
                no_sleep: true,
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
        self.pending.clear();
        self.active_http
            .lock()
            .expect("active HTTP table poisoned")
            .clear();
        self.http_responses
            .lock()
            .expect("HTTP response table poisoned")
            .clear();
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
                    operations: ActorOperations::default(),
                },
            );

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
        persisted_state: Option<Vec<u8>>,
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
                    reply,
                    ..
                } => {
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
                    reply.send(Err(anyhow!("queue requests are not supported before M4")));
                }
                ActorEvent::ConnectionPreflight { reply, .. }
                | ActorEvent::ConnectionOpen { reply, .. } => reply.send(Ok(())),
                ActorEvent::WebSocketOpen { reply, .. }
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
            | Command::ActionResult { .. }
            | Command::HttpResponseStart { .. }
            | Command::HttpResponseChunk { .. }
            | Command::Unknown => {}
        }
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
}
