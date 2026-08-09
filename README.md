# rivet-go

A Go SDK for running actors on [Rivet Engine](https://github.com/rivet-dev/rivet).

Define actor state, actions, HTTP handlers, and WebSocket handlers in Go.
`rivet-go` handles registration, persistence, scheduling, and graceful
shutdown. It uses a prebuilt native adapter, so applications do not need cgo or
a C toolchain.

The current release is `v0.1.0` and targets Rivet Engine `v2.3.10`.

## Install

```sh
go get github.com/ewhauser/rivet-go/rivet@v0.1.0
```

The first call to `rivet.Serve` downloads the native adapter for the current
platform, verifies its checksum, and stores it in the user cache directory.

Prebuilt adapters are available for:

- macOS arm64
- Linux amd64 and arm64
- Windows amd64

For offline environments, populate the cache ahead of time:

```sh
go run github.com/ewhauser/rivet-go/cmd/rivet-go-fetch@v0.1.0
```

## Define an actor

```go
package main

import (
	"log"

	"github.com/ewhauser/rivet-go/rivet"
)

type Counter struct {
	Value int `json:"value"`
}

type IncrementArgs struct {
	Amount int `json:"amount"`
}

func main() {
	registry := rivet.NewRegistry()

	err := rivet.Register(registry, "counter", rivet.Actor[Counter]{
		Actions: rivet.Actions[Counter]{
			"increment": rivet.Action(
				func(ctx *rivet.Context[Counter], args IncrementArgs) (int, error) {
					ctx.State().Value += args.Amount
					return ctx.State().Value, nil
				},
			),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := rivet.Serve(registry); err != nil {
		log.Fatal(err)
	}
}
```

Each actor has typed state and processes work serially. State changed by a
successful action is persisted automatically.

The SDK also supports:

- lifecycle hooks, durable alarms, and typed one-shot action schedules
- durable queues and generation-owned background work
- external and actor-scoped clients for actor creation, resolution, and actions
- raw HTTP handlers
- raw WebSocket handlers and broadcast
- actor sleep and wake
- actor identity and low-level actor KV
- per-actor SQLite databases

State changed from an HTTP or WebSocket handler must be persisted explicitly
with `ctx.Save`.

## Call actors from Go

Create one client and share it across goroutines:

```go
client, err := rivet.NewClient(rivet.ClientConfig{
	Endpoint:   "http://127.0.0.1:6420",
	Namespace:  "default",
	RunnerName: "counter-example",
	Token:      "dev",
})
if err != nil {
	log.Fatal(err)
}

counter, _, err := client.GetOrCreate(
	context.Background(),
	"counter",
	[]string{"account-123"},
	rivet.CreateOptions{},
)
if err != nil {
	log.Fatal(err)
}

count, err := rivet.Call[int](
	context.Background(),
	counter,
	"increment",
	IncrementArgs{Amount: 3},
)
if err != nil {
	log.Fatal(err)
}
fmt.Println(count)
```

`Create`, `Get`, `GetByKey`, and `GetOrCreate` return immutable actor handles.
`rivet.Call[T]` decodes typed action results; `ActorHandle.Call` and `CallRaw`
provide raw JSON output and exact JSON argument-array control. Creation input
is an opaque byte slice delivered unchanged by `Context.Input`.

Actors use `ctx.Client()` for the same operations. The actor-scoped client
inherits endpoint, namespace, runner name, token, headers, and HTTP transport
from `rivet.Config`. A direct call back to the current actor returns
`rivet.ErrSelfCall`, because waiting for work queued behind the current action
would deadlock.

## Run the counter example

Local development requires Go 1.26. The commands below use Python 3 to read the
created actor ID. The development launcher also needs Git and Rust 1.97 the
first time it builds the pinned engine.

Clone the repository and start Rivet Engine:

```sh
git clone https://github.com/ewhauser/rivet-go.git
cd rivet-go
go run ./cmd/rivet-go-dev
```

In another terminal, start the example runner:

```sh
go run ./examples/counter
```

Create a counter actor:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"counter","runner_name_selector":"counter-example","crash_policy":"destroy"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Call its `increment` action:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/increment" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"amount":3}]}'
```

The response is:

```json
{"output":3}
```

## More examples

- [WebSocket chat](examples/chat): raw WebSockets, durable message sequencing,
  hibernation, and graceful drain
- [SQLite todo list](examples/todo-sqlite): migrations, CRUD, result decoding,
  and transactions
- [Durable reminder](examples/reminder): alarms, cancellation, actor sleep, and
  alarm-driven wake
- [Durable action scheduling](examples/scheduling): multiple typed action
  schedules, stable IDs, inspection, independent cancellation, and sleep/wake
- [Raw HTTP counter](examples/http-counter): a `net/http` actor API with
  explicit state persistence
- [Per-tenant company database](examples/per-tenant-database): keyed actors,
  isolated durable state, identity metadata, and events
- [Actor KV](examples/actor-kv): text and binary values, prefix listing,
  reverse scans, limits, and deletion
- [Connection admin](examples/connection-admin): enumerate, message, and
  disconnect live raw gateway WebSockets
- [Actor actions](examples/actor-actions): typed action calls, creation input,
  actor resolution, and company-created employee actors
- [Cross-actor actions](examples/cross-actor-actions): checkout and inventory
  actors coordinating through actor-scoped clients
- [Collaborative cursors](examples/cursors): ActorConnect caller context,
  typed per-connection state, presence events, and hibernation-safe cursors
- [Durable AI agent](examples/ai-agent): durable prompt processing, explicit
  retry/completion, lifecycle-managed model work, and a replaceable provider
  interface

## RivetKit feature compatibility

This table compares the public Go API with the RivetKit actor surface at the
[pinned Rivet Engine version](docs/PINNED-VERSION.md). "Supported" means the
feature has a public Go API and real-engine conformance coverage. "Partial"
means the available portion is public and covered, but some upstream behavior
is still missing.

| RivetKit feature | Status | Go behavior |
|---|---|---|
| Typed actor state and actions | Supported | Actions are typed, serialized per actor, and persist the complete state after success. HTTP and WebSocket handlers call `Save` explicitly. |
| Lifecycle hooks | Partial | `OnStart`, `OnStop`, `OnAlarm`, and generation-owned `Run` are available. There are no distinct create, wake, sleep, destroy, or state-change hooks. |
| Actor input and identity | Partial | Context exposes raw creation input, actor ID, generation, actor name, and the engine-formatted key. Individual key segments, creation time, and region are not public. |
| Actor-to-actor and external action clients | Supported | Concurrency-safe clients expose `Get`, `GetByKey`, `GetOrCreate`, `Create`, regional creation, exact creation input, typed/raw action calls, structured errors, cancellation, and actor-scoped clients with direct self-call rejection. |
| ActorConnect and WebSocket clients | Partial | Actors support ActorConnect caller context, parameters, lifecycle hooks, state, enumeration, broadcast, and hibernation. The Go client does not yet expose the ActorConnect protocol, subscriptions, client events, or reconnect. Raw gateway WebSocket handlers remain supported. |
| Actions and structured errors | Supported | Typed and raw CBOR actions, cooperative deadlines, panic isolation, stable error codes, and actor-local rejection of results over the 1 MiB boundary limit are covered. |
| Events and broadcast | Supported | Actors can broadcast named CBOR events to actor-connect and raw WebSocket clients, including exclusion of one raw connection. |
| Raw HTTP handlers | Partial | Standard `net/http` request and response handling works, but the SDK buffers complete responses with a 16 MiB cap, has no `http.Flusher`, and cannot represent repeated response-header values. |
| Raw WebSocket handlers | Supported | Text and binary messages, connect/disconnect hooks, targeted sends, close frames, broadcast, and bounded backpressure are available. |
| Connection state and enumeration | Supported | `CurrentConnection` exposes an ActorConnect caller, `Connections` returns sorted live ActorConnect and raw WebSocket connections, and `NewConnectionState` provides typed ActorConnect state that persists across sleep/wake. Encoded connection state is limited to 1 MiB and oversized state rejects only the current operation. Connections expose IDs, CBOR parameters, request metadata, hibernation capability, and resume status. |
| Actor sleep and WebSocket hibernation | Supported | Actors can request sleep; opted-in raw WebSockets remain connected and can wake the actor with a message. |
| Durable scheduling | Supported | `Context.Schedules` exposes durable one-shot `After` and `At` action schedules with typed payloads, stable IDs, get/list inspection, and independent cancellation. The original replaceable `Schedule`/`OnAlarm` API remains available for compatibility. |
| Cron schedules | Not implemented | Recurring cron expressions must be modeled by scheduling the next one-shot action. |
| Actor KV | Supported | The byte-oriented `KV` handle supports get, list, put, and delete. It is retained for RivetKit compatibility; prefer typed state or SQLite for new actors. |
| Per-actor SQLite | Supported | Opt-in raw SQL, typed values, queries, transactions, isolation, sleep/wake, and durability are covered. Result limits are documented in [Operations](docs/OPERATIONS.md). |
| Durable queues | Supported | Actor queues support typed CBOR send, filtered blocking receive, immediate poll, completable messages, explicit completion responses and retry, cancellation, sleep/wake, and stable capacity/size errors. External actor handles expose JSON `Send` and `SendAndWait`. |
| Durable workflows | Not implemented | Workflow definitions, replay, steps, rollback, and history are not exposed. |
| Actor-scoped background work | Supported | `Actor.Run` restarts with each generation and serializes actor-state access while queue waits and `RunContext.KeepAwake` yield the actor turn. `Context.KeepAwake` and restricted detached `WaitUntil` work are registered with core and drained during shutdown. |
| Actor destruction | Supported | `Context.Destroy` sends the current callback response, drains admitted work, fences SQLite leases, closes WebSockets, and permanently destroys the generation. Calls during startup or teardown return stable errors. |
| Dynamic actors | Not implemented | Loading actor definitions from generated or user-provided source is not supported. |
| Custom inspector tabs | Not implemented | The inspector/devtools extension protocol and actor-supplied tab assets are not exposed. |
| Serverless and WebAssembly runners | Not implemented | The SDK hosts long-running native runner processes; Cloudflare Workers, Supabase Functions, and custom serverless runtimes are outside the current target. |
| Process drain, logging, and metrics | Supported | Signal-aware graceful drain, structured `slog` output, stable metrics hooks, and strict soak coverage are included. |

## Configuration

Pass a `rivet.Config` to `Serve` to configure the engine endpoint, namespace,
runner identity, actor-client authentication and transport, logging, metrics
hooks, SQLite transport, and graceful shutdown deadline:

```go
err := rivet.Serve(registry, rivet.Config{
	Endpoint:   "http://127.0.0.1:6420",
	Namespace:  "default",
	RunnerName: "my-runner",
	Token:      os.Getenv("RIVET_TOKEN"),
})
```

`Config.Headers` and `Config.HTTPClient` are also inherited by every
actor-scoped client. `NewClient` accepts the same fields independently for
clients outside a runner.

`Serve` listens for `SIGINT` and `SIGTERM` and drains admitted work before
returning. Services that already manage process signals can use
`Registry.Serve` with their own context.

## Documentation

- [Operations](docs/OPERATIONS.md): deployment, shutdown, SQLite, logging,
  metrics, and soak testing
- [WebSocket hibernation](docs/WEBSOCKET-HIBERNATION.md): keeping connections
  open while actors sleep
- [FFI boundary](docs/FFI-BOUNDARY.md): native-library ownership and threading
- [Pinned Rivet version](docs/PINNED-VERSION.md): upstream version and build
  details
- [Conformance tests](conformance/README.md): behavior tested against the real
  engine

## Development

Run unit tests without starting Rivet Engine:

```sh
go test -short ./...
cargo test --workspace
```

Run the complete race-enabled conformance suite:

```sh
go test -race -count=1 ./...
```

## License

[MIT](LICENSE)
