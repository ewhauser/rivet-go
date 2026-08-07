# rivet-go

`rivet-go` hosts typed Go actors on Rivet Engine. It embeds a pinned
`rivetkit-core` adapter and loads it with
[purego](https://github.com/ebitengine/purego), so applications need neither
cgo nor a C toolchain at build time.

The SDK includes typed state and actions, raw HTTP and WebSocket handlers,
durable alarms, actor sleep and wake, graceful process drain, structured
logging, and dependency-free metrics hooks. Rivet Engine is pinned to
`v2.3.10` (`957d4e482f404913ca1955d8ecc357533f6fd081`).

## Quickstart

Prerequisites are Go 1.26, Git, Python 3, and Rust 1.97. Rust is needed only when the
development launcher must build the pinned local engine; applications do not
compile Rust. The SDK's native adapter is not stored in the module: on first
use the loader downloads this platform's library from the pinned GitHub
release, verifies it against the SHA-256 recorded in
`internal/ffi/checksums.txt`, and caches it under the user cache directory
(about 13 MB, once per version). To pre-seed that cache — for CI images or
machines that will run offline — run:

```sh
go run github.com/ewhauser/rivet-go/cmd/rivet-go-fetch
```

Alternatively download the release asset yourself and point `RIVET_GO_FFI_LIB`
at it; `RIVET_GO_FFI_BASE_URL` overrides the asset host for mirrors.

Install the repository dependencies:

```sh
git clone https://github.com/ewhauser/rivet-go.git
cd rivet-go
go mod download
```

In terminal 1, acquire and run the exact engine used by conformance:

```sh
go run ./cmd/rivet-go-dev
```

The first run may build Engine `v2.3.10` from its exact tag and cache the
verified binary under `~/.cache/rivet-go/engine-v2.3.10`. The launcher stores
local actor data in `.rivet-go/` and prints this readiness line:

```text
Rivet Engine 2.3.10 is ready at http://127.0.0.1:6420
```

In terminal 2, start the counter runner:

```sh
go run ./examples/counter
```

In terminal 3, create one counter actor and call it through the real gateway:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"counter","runner_name_selector":"counter-example","crash_policy":"destroy"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"

curl -fsS -X POST "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/increment" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"amount":3}]}'
printf '\n'

curl -fsS "http://127.0.0.1:6420/gateway/$ACTOR_ID/request/current" \
  -H 'Authorization: Bearer dev'
```

The transcript is:

```text
{"output":3}
3
```

Stop a runner with Ctrl-C during development, or send `SIGTERM` to its built
executable. Admitted actor work drains, WebSockets receive code 1001, and the
engine observes the runner leave. A built runner exits successfully after a
clean drain; the `go run` wrapper may still report its own interrupt status
after Ctrl-C even when the runner log records a completed drain.

## SDK shape

The counter example is built from the public API:

```go
type Counter struct {
    Count int `json:"count"`
}

type Increment struct {
    Amount int `json:"amount"`
}

registry := rivet.NewRegistry()
err := rivet.Register(registry, "counter", rivet.Actor[Counter]{
    Actions: rivet.Actions[Counter]{
        "increment": rivet.Action(func(ctx *rivet.Context[Counter], in Increment) (int, error) {
            ctx.State().Count += in.Amount
            return ctx.State().Count, ctx.Broadcast("countChanged", ctx.State().Count)
        }),
    },
})
if err != nil {
    log.Fatal(err)
}
if err := rivet.Serve(registry); err != nil {
    log.Fatal(err)
}
```

[`examples/chat`](examples/chat) is a runnable raw-WebSocket actor with durable
message sequencing and broadcast. Raw WebSocket hibernation is opt-in with
`Actor.HibernateWebSockets`: it preserves connections across actor sleep but
adds a per-message engine acknowledgement. The default is false and closes
connections when the actor sleeps, matching rivetkit v2.3.10. See
[WebSocket hibernation](docs/WEBSOCKET-HIBERNATION.md) for the tradeoff.
[`examples/counter`](examples/counter) can also expose SDK metrics through
`expvar`:

```sh
go run ./examples/counter -metrics-address 127.0.0.1:6060
curl -fsS http://127.0.0.1:6060/debug/vars
```

Go-side structured logging uses a configurable `*slog.Logger`; nil discards
SDK logs. `Config.LogLevel` independently controls native logging. The
`rivet.Hooks` interface reports counters, gauges, and poll durations without a
Prometheus or telemetry dependency.

## Development

Fast tests do not start the engine:

```sh
go test -short ./...
cargo test --workspace
```

The real-engine suite uses the same acquisition path as `rivet-go-dev`:

```sh
go test -race -count=1 ./...
```

See [Operations](docs/OPERATIONS.md) for soak, drain, metrics, platform, and
engine-upgrade procedures; [FFI boundary](docs/FFI-BOUNDARY.md) for ownership
and threading; and [Pinned version](docs/PINNED-VERSION.md) for the exact
upstream dependency.
