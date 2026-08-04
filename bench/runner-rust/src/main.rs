use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Result, bail};
use async_trait::async_trait;
use rivetkit::prelude::*;
use rivetkit::{Action, Handles, Request, WebSocket, WsMessage, action};
use serde::{Deserialize, Serialize};

type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

struct Counter {
    persist: bool,
    volatile_count: AtomicI64,
}

#[derive(Default, Serialize, Deserialize)]
struct CounterState {
    count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
struct Increment(i64);

impl Action for Increment {
    type Output = i64;
    const NAME: &'static str = "increment";
}

#[derive(Debug, Serialize, Deserialize)]
struct Get;

impl Action for Get {
    type Output = i64;
    const NAME: &'static str = "get";
}

#[async_trait]
impl Actor for Counter {
    type State = CounterState;
    type Input = ();
    type Actions = (Increment, Get);
    type Events = ();
    type Queue = ();
    type ConnParams = ();
    type ConnState = ();
    type Action = action::Raw;
    // At this pin, the standalone git dependency enables sqlite-remote but
    // Registry only selects that backend when HAS_DATABASE is true. Actor state
    // persistence itself needs the backend even though this app issues no SQL.
    const HAS_DATABASE: bool = true;

    async fn create_state(_ctx: &Ctx<Self>, _input: Self::Input) -> Result<Self::State> {
        Ok(CounterState::default())
    }

    async fn create(_ctx: &Ctx<Self>) -> Result<Self> {
        let mode = std::env::var("BENCH_PERSIST_MODE").unwrap_or_else(|_| "persist".to_owned());
        let persist = match mode.as_str() {
            "persist" => true,
            "no-persist" => false,
            _ => bail!("unsupported BENCH_PERSIST_MODE {mode:?}"),
        };
        Ok(Self {
            persist,
            volatile_count: AtomicI64::new(0),
        })
    }
}

impl Handles<Increment> for Counter {
    type Future = BoxFuture<i64>;

    fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: Increment) -> Self::Future {
        Box::pin(async move {
            if self.persist {
                let count = {
                    let mut state = ctx.state_mut();
                    state.count += action.0;
                    state.count
                };
                let delta = ctx.encode_state_delta()?;
                ctx.save_state(vec![delta]).await?;
                ctx.clear_state_dirty();
                Ok(count)
            } else {
                Ok(self.volatile_count.fetch_add(action.0, Ordering::SeqCst) + action.0)
            }
        })
    }
}

impl Handles<Get> for Counter {
    type Future = BoxFuture<i64>;

    fn handle(self: Arc<Self>, ctx: Ctx<Self>, _action: Get) -> Self::Future {
        Box::pin(async move {
            if self.persist {
                Ok(ctx.state().count)
            } else {
                Ok(self.volatile_count.load(Ordering::SeqCst))
            }
        })
    }
}

struct Echo;

#[async_trait]
impl Actor for Echo {
    type State = ();
    type Input = ();
    type Actions = ();
    type Events = ();
    type Queue = ();
    type ConnParams = ();
    type ConnState = ();
    type Action = action::Raw;
    const HAS_DATABASE: bool = true;

    async fn create_state(_ctx: &Ctx<Self>, _input: Self::Input) -> Result<Self::State> {
        Ok(())
    }

    async fn create(_ctx: &Ctx<Self>) -> Result<Self> {
        Ok(Self)
    }

    async fn on_websocket(
        self: Arc<Self>,
        _ctx: Ctx<Self>,
        websocket: WebSocket,
        _request: Request,
    ) -> Result<()> {
        let sender = websocket.clone();
        websocket.configure_message_event_callback(Some(Arc::new(
            move |message: WsMessage, _message_index| {
                sender.send(message);
                Ok(())
            },
        )));
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error")),
        )
        .with_writer(std::io::stderr)
        .init();

    let mut registry = Registry::new();
    registry.register_actor::<Counter>("counter");
    registry.register_actor::<Echo>("echo");
    registry.start().await
}
