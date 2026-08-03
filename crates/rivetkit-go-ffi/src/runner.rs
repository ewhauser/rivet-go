use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, Once, mpsc as std_mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use rivetkit_core::registry::CoreEnvoyHandle;
use rivetkit_core::{CoreRegistry, EngineSpawnMode, ServeConfig};
use serde::Deserialize;
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::ErrorPayload;
use crate::actor_proxy::ActorProxy;
use crate::correlation::CorrelationTable;
use crate::wire::{CommandBatch, DrainReport, Event, EventBatch, RunnerConfig};

const EVENT_QUEUE_CAPACITY: usize = 1_024;
const COMMAND_QUEUE_CAPACITY: usize = 1_024;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const STARTUP_RESULT_TIMEOUT: Duration = Duration::from_secs(12);
const REGISTRATION_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(250);
const CORRELATION_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const MAX_POLL_BATCH: usize = 64;

static TRACING_INIT: Once = Once::new();

enum StartupResult {
    Ready,
    Failed(ErrorPayload),
}

pub(crate) struct RunnerInner {
    events: Receiver<Event>,
    commands: mpsc::Sender<CommandBatch>,
    shutdown: watch::Sender<Option<u32>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    polling: AtomicBool,
    shutdown_started: AtomicBool,
    next_seq: AtomicU64,
    correlations: CorrelationTable,
}

impl RunnerInner {
    pub(crate) fn start(config: RunnerConfig) -> Result<Box<Self>, ErrorPayload> {
        validate_config(&config)?;
        init_tracing(&config.log_level);

        let (event_tx, event_rx) = bounded(EVENT_QUEUE_CAPACITY);
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(None);
        let (startup_tx, startup_rx) = std_mpsc::sync_channel(1);
        let correlations = CorrelationTable::default();
        let thread_correlations = correlations.clone();
        let thread = thread::Builder::new()
            .name("rivet-go-runner".to_owned())
            .spawn(move || {
                runner_thread(
                    config,
                    event_tx,
                    command_rx,
                    shutdown_rx,
                    startup_tx,
                    thread_correlations,
                )
            })
            .map_err(|error| ErrorPayload::new("runtime_start_failed", error.to_string()))?;

        match startup_rx.recv_timeout(STARTUP_RESULT_TIMEOUT) {
            Ok(StartupResult::Ready) => Ok(Box::new(Self {
                events: event_rx,
                commands: command_tx,
                shutdown: shutdown_tx,
                thread: Mutex::new(Some(thread)),
                polling: AtomicBool::new(false),
                shutdown_started: AtomicBool::new(false),
                next_seq: AtomicU64::new(1),
                correlations,
            })),
            Ok(StartupResult::Failed(error)) => {
                let _ = thread.join();
                Err(error)
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                let _ = shutdown_tx.send(Some(0));
                let _ = thread.join();
                Err(ErrorPayload::new(
                    "connection_failed",
                    format!(
                        "runner did not register with the engine within {} seconds",
                        STARTUP_RESULT_TIMEOUT.as_secs()
                    ),
                ))
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                let _ = thread.join();
                Err(ErrorPayload::new(
                    "runtime_start_failed",
                    "runner runtime exited before reporting startup",
                ))
            }
        }
    }

    pub(crate) fn poll(&self, timeout_ms: u32) -> Result<Vec<u8>, ErrorPayload> {
        let _permit = PollPermit::acquire(&self.polling)?;
        let mut events = Vec::new();
        match self
            .events
            .recv_timeout(Duration::from_millis(u64::from(timeout_ms)))
        {
            Ok(event) => {
                events.push(event);
                while events.len() < MAX_POLL_BATCH {
                    match self.events.try_recv() {
                        Ok(event) => events.push(event),
                        Err(_) => break,
                    }
                }
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
        }

        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        EventBatch { seq, events }
            .encode()
            .map_err(|error| ErrorPayload::new("encode_failed", error.to_string()))
    }

    pub(crate) fn submit(&self, bytes: &[u8]) -> Result<(), ErrorPayload> {
        let batch = CommandBatch::decode(bytes).map_err(|error| {
            ErrorPayload::new(
                "invalid_command_batch",
                format!("decode MessagePack CommandBatch: {error}"),
            )
        })?;
        if batch.contains_unknown() {
            return Err(ErrorPayload::new(
                "unknown_command",
                "CommandBatch contains a command not supported by M2",
            ));
        }
        batch
            .validate()
            .map_err(|message| ErrorPayload::new("invalid_command_batch", message))?;

        self.commands.try_send(batch).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => ErrorPayload::new(
                "backpressure",
                format!("runner command queue is full (capacity {COMMAND_QUEUE_CAPACITY})"),
            ),
            mpsc::error::TrySendError::Closed(_) => {
                ErrorPayload::new("runner_stopped", "runner command queue is closed")
            }
        })
    }

    pub(crate) fn shutdown(&self, deadline_ms: u32) -> Result<(), ErrorPayload> {
        if !self.shutdown_started.swap(true, Ordering::AcqRel) {
            self.shutdown
                .send(Some(deadline_ms))
                .map_err(|_| ErrorPayload::new("runner_stopped", "runner runtime has stopped"))?;
        }
        Ok(())
    }

    pub(crate) fn close(&self) {
        if !self.shutdown_started.swap(true, Ordering::AcqRel) {
            let _ = self.shutdown.send(Some(0));
        }
        self.correlations.drain_shutdown();
        if let Some(thread) = self
            .thread
            .lock()
            .expect("runner thread mutex poisoned")
            .take()
        {
            // Free is permitted without a prior shutdown/poll drain. Keep the
            // bounded event queue moving while the runtime emits its final
            // state so a parked sender cannot make ownership reclamation hang.
            while !thread.is_finished() {
                while self.events.try_recv().is_ok() {}
                thread::sleep(Duration::from_millis(1));
            }
            let _ = thread.join();
        }
    }
}

fn init_tracing(log_level: &str) {
    let level = match log_level {
        "trace" => tracing_subscriber::filter::LevelFilter::TRACE,
        "debug" => tracing_subscriber::filter::LevelFilter::DEBUG,
        "info" => tracing_subscriber::filter::LevelFilter::INFO,
        "warn" => tracing_subscriber::filter::LevelFilter::WARN,
        "error" => tracing_subscriber::filter::LevelFilter::ERROR,
        _ => return,
    };
    TRACING_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(level)
            .with_target(false)
            .with_ansi(false)
            .try_init();
    });
}

#[derive(Debug)]
struct PollPermit<'a> {
    flag: &'a AtomicBool,
}

impl<'a> PollPermit<'a> {
    fn acquire(flag: &'a AtomicBool) -> Result<Self, ErrorPayload> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                ErrorPayload::new(
                    "poll_in_progress",
                    "another caller is already polling this runner",
                )
            })?;
        Ok(Self { flag })
    }
}

impl Drop for PollPermit<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

fn validate_config(config: &RunnerConfig) -> Result<(), ErrorPayload> {
    let endpoint = Url::parse(&config.engine_endpoint).map_err(|error| {
        ErrorPayload::new(
            "invalid_config",
            format!("invalid engine_endpoint: {error}"),
        )
    })?;
    if !matches!(endpoint.scheme(), "http" | "https") {
        return Err(ErrorPayload::new(
            "invalid_config",
            "engine_endpoint must use http or https",
        ));
    }
    for (name, value) in [
        ("namespace", config.namespace.as_str()),
        ("runner_name", config.runner_name.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ErrorPayload::new(
                "invalid_config",
                format!("{name} must not be empty"),
            ));
        }
    }
    if config.version == 0 {
        return Err(ErrorPayload::new(
            "invalid_config",
            "version must be greater than zero",
        ));
    }
    if config.total_slots == 0 {
        return Err(ErrorPayload::new(
            "invalid_config",
            "total_slots must be greater than zero",
        ));
    }
    if config.actor_names.len() > 1_024 {
        return Err(ErrorPayload::new(
            "invalid_config",
            "actor_names must contain at most 1024 entries",
        ));
    }
    let mut seen_actor_names = BTreeSet::new();
    for name in &config.actor_names {
        if name.trim().is_empty() {
            return Err(ErrorPayload::new(
                "invalid_config",
                "actor_names entries must not be empty",
            ));
        }
        if !seen_actor_names.insert(name) {
            return Err(ErrorPayload::new(
                "invalid_config",
                format!("actor_names contains duplicate `{name}`"),
            ));
        }
    }
    if !matches!(
        config.log_level.as_str(),
        "trace" | "debug" | "info" | "warn" | "error"
    ) {
        return Err(ErrorPayload::new(
            "invalid_config",
            "log_level must be one of trace, debug, info, warn, or error",
        ));
    }
    Ok(())
}

fn runner_thread(
    config: RunnerConfig,
    events: Sender<Event>,
    commands: mpsc::Receiver<CommandBatch>,
    shutdown: watch::Receiver<Option<u32>>,
    startup: std_mpsc::SyncSender<StartupResult>,
    correlations: CorrelationTable,
) {
    let runtime = match Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("rivet-go-tokio")
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = startup.send(StartupResult::Failed(ErrorPayload::new(
                "runtime_start_failed",
                error.to_string(),
            )));
            return;
        }
    };

    runtime.block_on(run_runner(
        config,
        events,
        commands,
        shutdown,
        startup,
        correlations,
    ));
}

async fn run_runner(
    config: RunnerConfig,
    events: Sender<Event>,
    commands: mpsc::Receiver<CommandBatch>,
    mut shutdown: watch::Receiver<Option<u32>>,
    startup: std_mpsc::SyncSender<StartupResult>,
    correlations: CorrelationTable,
) {
    let cancellation = CancellationToken::new();
    let (handle_tx, handle_rx) = oneshot::channel();
    let actor_proxy = ActorProxy::new(events.clone(), correlations.clone());
    let mut registry = CoreRegistry::new();
    actor_proxy.register(&mut registry, &config.actor_names);
    let serve_config = ServeConfig {
        version: config.version,
        endpoint: config.engine_endpoint.clone(),
        token: Some("dev".to_owned()),
        namespace: config.namespace.clone(),
        pool_name: config.runner_name.clone(),
        engine_spawn: EngineSpawnMode::Never,
        engine_auto_download: false,
        handle_inspector_http_in_runtime: false,
        serverless_package_version: env!("CARGO_PKG_VERSION").to_owned(),
        serverless_validate_endpoint: true,
        serverless_max_start_payload_bytes: 1_048_576,
        serverless_cache_envoy: true,
        ..Default::default()
    };
    let serve_cancellation = cancellation.clone();
    let mut serve = tokio::spawn(async move {
        registry
            .serve_with_config_and_handle_observer(
                serve_config,
                serve_cancellation,
                move |handle| {
                    let _ = handle_tx.send(handle);
                },
            )
            .await
    });

    let handle = tokio::select! {
        result = handle_rx => match result {
            Ok(handle) => handle,
            Err(_) => {
                let error = startup_error_from_serve(&mut serve).await;
                let _ = startup.send(StartupResult::Failed(error));
                correlations.drain_shutdown();
                return;
            }
        },
        result = &mut serve => {
            let error = serve_error("runner stopped before registration", result);
            let _ = startup.send(StartupResult::Failed(error));
            correlations.drain_shutdown();
            return;
        }
        _ = tokio::time::sleep(STARTUP_TIMEOUT) => {
            cancellation.cancel();
            serve.abort();
            let _ = serve.await;
            let _ = startup.send(StartupResult::Failed(ErrorPayload::new(
                "connection_failed",
                format!("could not connect to {} and register within {} seconds", config.engine_endpoint, STARTUP_TIMEOUT.as_secs()),
            )));
            correlations.drain_shutdown();
            return;
        }
    };

    let registration = match lookup_registration(&config).await {
        Ok(registration) => registration,
        Err(error) => {
            cancellation.cancel();
            serve.abort();
            let _ = serve.await;
            let _ = startup.send(StartupResult::Failed(error));
            correlations.drain_shutdown();
            return;
        }
    };
    let connected_event = connected_event(&config, &registration);
    if events.send(connected_event.clone()).is_err() {
        cancellation.cancel();
        serve.abort();
        let _ = serve.await;
        let _ = startup.send(StartupResult::Failed(ErrorPayload::new(
            "runtime_start_failed",
            "event queue closed during startup",
        )));
        correlations.drain_shutdown();
        return;
    }
    let _ = startup.send(StartupResult::Ready);

    let mut health = tokio::time::interval(HEALTH_CHECK_INTERVAL);
    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut correlation_sweep = tokio::time::interval(CORRELATION_SWEEP_INTERVAL);
    correlation_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut seen_healthy = handle.status().ping_healthy;
    let mut connected = true;
    let command_cancellation = CancellationToken::new();
    let mut command_task = tokio::spawn(command_loop(
        commands,
        actor_proxy.clone(),
        command_cancellation.clone(),
    ));

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                let deadline_ms = if changed.is_ok() {
                    (*shutdown.borrow()).unwrap_or(0)
                } else {
                    0
                };
                finish_shutdown(
                    deadline_ms,
                    &handle,
                    cancellation,
                    &mut serve,
                    &events,
                    &correlations,
                    &actor_proxy,
                    command_cancellation,
                    &mut command_task,
                ).await;
                return;
            }
            result = &mut serve => {
                let reason = serve_error("runner runtime stopped unexpectedly", result).message;
                let _ = events.send(Event::RunnerDisconnected { reason });
                let _ = events.send(Event::RunnerStopped {
                    drain_report: DrainReport {
                        graceful: false,
                        elapsed_ms: 0,
                        actors_stopped: 0,
                        actors_remaining: handle.status().active_actor_count as u32,
                    },
                });
                command_cancellation.cancel();
                let _ = command_task.await;
                actor_proxy.drain_shutdown();
                correlations.drain_shutdown();
                return;
            }
            _ = health.tick() => {
                let healthy = handle.status().ping_healthy;
                if healthy {
                    seen_healthy = true;
                    if !connected {
                        connected = true;
                        let _ = events.send(connected_event.clone());
                    }
                } else if seen_healthy && connected {
                    connected = false;
                    let _ = events.send(Event::RunnerDisconnected {
                        reason: "engine connection became unhealthy; rivetkit-core is reconnecting".to_owned(),
                    });
                }
            }
            _ = correlation_sweep.tick() => {
                correlations.expire(Instant::now());
                actor_proxy.sweep_pending();
            }
        }
    }
}

async fn command_loop(
    mut commands: mpsc::Receiver<CommandBatch>,
    actor_proxy: ActorProxy,
    cancellation: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            batch = commands.recv() => {
                let Some(batch) = batch else {
                    return;
                };
                for command in batch.commands {
                    actor_proxy.handle_command(command);
                }
            }
        }
    }
}

async fn startup_error_from_serve(
    serve: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
) -> ErrorPayload {
    match tokio::time::timeout(Duration::from_millis(100), serve).await {
        Ok(result) => serve_error("runner stopped before registration", result),
        Err(_) => ErrorPayload::new(
            "connection_failed",
            "rivetkit-core did not expose a registration handle",
        ),
    }
}

fn serve_error(
    context: &str,
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> ErrorPayload {
    let message = match result {
        Ok(Ok(())) => context.to_owned(),
        Ok(Err(error)) => format!("{context}: {error:#}"),
        Err(error) => format!("{context}: {error}"),
    };
    ErrorPayload::new("connection_failed", message)
}

#[allow(clippy::too_many_arguments)]
async fn finish_shutdown(
    deadline_ms: u32,
    handle: &CoreEnvoyHandle,
    cancellation: CancellationToken,
    serve: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    events: &Sender<Event>,
    correlations: &CorrelationTable,
    actor_proxy: &ActorProxy,
    command_cancellation: CancellationToken,
    command_task: &mut tokio::task::JoinHandle<()>,
) {
    let started = Instant::now();
    let actors_before = handle.status().active_actor_count as u32;
    cancellation.cancel();
    let graceful = match tokio::time::timeout(
        Duration::from_millis(u64::from(deadline_ms)),
        &mut *serve,
    )
    .await
    {
        Ok(Ok(Ok(()))) => true,
        Ok(_) => false,
        Err(_) => {
            serve.abort();
            let _ = serve.await;
            false
        }
    };
    let actors_remaining = handle.status().active_actor_count as u32;
    command_cancellation.cancel();
    let _ = command_task.await;
    let _ = events.send(Event::RunnerStopped {
        drain_report: DrainReport {
            graceful,
            elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            actors_stopped: actors_before.saturating_sub(actors_remaining),
            actors_remaining,
        },
    });
    actor_proxy.drain_shutdown();
    correlations.drain_shutdown();
}

#[derive(Debug, Deserialize)]
struct EnvoysResponse {
    envoys: Vec<EnvoyRegistration>,
}

#[derive(Clone, Debug, Deserialize)]
struct EnvoyRegistration {
    envoy_key: String,
    pool_name: String,
    metadata: Option<serde_json::Value>,
    stop_ts: Option<i64>,
}

async fn lookup_registration(config: &RunnerConfig) -> Result<EnvoyRegistration, ErrorPayload> {
    let mut url = Url::parse(&config.engine_endpoint)
        .map_err(|error| ErrorPayload::new("invalid_config", error.to_string()))?;
    url.set_path("/envoys");
    url.set_query(None);
    url.query_pairs_mut()
        .append_pair("namespace", &config.namespace)
        .append_pair("name", &config.runner_name);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .map_err(|error| ErrorPayload::new("registration_failed", error.to_string()))?;
    let deadline = tokio::time::Instant::now() + REGISTRATION_LOOKUP_TIMEOUT;
    let mut last_error = "engine management API returned no matching active envoy".to_owned();
    loop {
        match client.get(url.clone()).bearer_auth("dev").send().await {
            Ok(response) if response.status().is_success() => {
                match response.json::<EnvoysResponse>().await {
                    Ok(response) => {
                        if let Some(registration) = response.envoys.into_iter().find(|envoy| {
                            envoy.pool_name == config.runner_name && envoy.stop_ts.is_none()
                        }) {
                            return Ok(registration);
                        }
                    }
                    Err(error) => last_error = format!("decode /envoys response: {error}"),
                }
            }
            Ok(response) => last_error = format!("GET /envoys returned {}", response.status()),
            Err(error) => last_error = format!("GET /envoys failed: {error}"),
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ErrorPayload::new(
                "registration_failed",
                format!(
                    "rivetkit-core connected but the engine did not list runner `{}` through /envoys: {last_error}",
                    config.runner_name
                ),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn connected_event(config: &RunnerConfig, registration: &EnvoyRegistration) -> Event {
    let mut metadata = BTreeMap::from([
        ("management_resource".to_owned(), "/envoys".to_owned()),
        ("protocol".to_owned(), "envoy-v6".to_owned()),
        ("runner_name".to_owned(), config.runner_name.clone()),
        ("log_level".to_owned(), config.log_level.clone()),
    ]);
    if let Some(engine_metadata) = &registration.metadata {
        metadata.insert("engine_metadata".to_owned(), engine_metadata.to_string());
    }
    Event::RunnerConnected {
        runner_id: registration.envoy_key.clone(),
        metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> RunnerConfig {
        RunnerConfig {
            engine_endpoint: "http://127.0.0.1:6420".to_owned(),
            namespace: "default".to_owned(),
            runner_name: "rivet-go-test".to_owned(),
            version: 1,
            total_slots: 1,
            actor_names: Vec::new(),
            log_level: "info".to_owned(),
        }
    }

    #[test]
    fn validates_m2_config() {
        assert!(validate_config(&valid_config()).is_ok());
        let mut config = valid_config();
        config.actor_names.push("actor".to_owned());
        assert!(validate_config(&config).is_ok());
        config.actor_names.push("actor".to_owned());
        assert_eq!(
            validate_config(&config)
                .expect_err("duplicate actor name")
                .code,
            "invalid_config"
        );
    }

    #[test]
    fn second_poll_permit_is_rejected() {
        let flag = AtomicBool::new(false);
        let first = PollPermit::acquire(&flag).expect("first poll permit");
        let error = PollPermit::acquire(&flag).expect_err("second poll must fail");
        assert_eq!(error.code, "poll_in_progress");
        drop(first);
        assert!(PollPermit::acquire(&flag).is_ok());
    }

    #[test]
    fn submit_accepts_empty_rejects_unknown_and_reports_backpressure() {
        let (event_tx, event_rx) = bounded(1);
        drop(event_tx);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (shutdown_tx, _shutdown_rx) = watch::channel(None);
        let runner = RunnerInner {
            events: event_rx,
            commands: command_tx,
            shutdown: shutdown_tx,
            thread: Mutex::new(None),
            polling: AtomicBool::new(false),
            shutdown_started: AtomicBool::new(false),
            next_seq: AtomicU64::new(1),
            correlations: CorrelationTable::default(),
        };
        let empty = rmp_serde::to_vec_named(&CommandBatch {
            commands: Vec::new(),
        })
        .expect("encode empty batch");
        assert!(runner.submit(&empty).is_ok());
        assert_eq!(
            runner
                .submit(&empty)
                .expect_err("full queue must fail")
                .code,
            "backpressure"
        );

        let unknown = rmp_serde::to_vec_named(&serde_json::json!({
            "commands": [{"kind": "future_command"}]
        }))
        .expect("encode unknown command");
        assert_eq!(
            runner
                .submit(&unknown)
                .expect_err("unknown command must fail")
                .code,
            "unknown_command"
        );
    }

    #[test]
    fn unreachable_endpoint_fails_within_startup_bound() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve endpoint");
        let address = listener.local_addr().expect("endpoint address");
        drop(listener);
        let mut config = valid_config();
        config.engine_endpoint = format!("http://{address}");
        let started = Instant::now();
        let error = match RunnerInner::start(config) {
            Ok(_) => panic!("unreachable endpoint must fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, "connection_failed");
        assert!(
            started.elapsed() <= STARTUP_RESULT_TIMEOUT,
            "startup exceeded {:?}: {:?}",
            STARTUP_RESULT_TIMEOUT,
            started.elapsed()
        );
    }

    #[test]
    fn close_without_shutdown_drains_a_parked_event_sender() {
        let (event_tx, event_rx) = bounded(1);
        event_tx
            .send(Event::RunnerDisconnected {
                reason: "fill queue".to_owned(),
            })
            .expect("fill event queue");
        let thread = thread::spawn(move || {
            event_tx
                .send(Event::RunnerDisconnected {
                    reason: "parked sender".to_owned(),
                })
                .expect("send after queue drain");
        });
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (shutdown_tx, _shutdown_rx) = watch::channel(None);
        let runner = RunnerInner {
            events: event_rx,
            commands: command_tx,
            shutdown: shutdown_tx,
            thread: Mutex::new(Some(thread)),
            polling: AtomicBool::new(false),
            shutdown_started: AtomicBool::new(false),
            next_seq: AtomicU64::new(1),
            correlations: CorrelationTable::default(),
        };

        runner.close();
        assert!(runner.shutdown_started.load(Ordering::Acquire));
    }
}
