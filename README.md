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

Actor definitions can also provide:

- lifecycle hooks and durable alarms
- raw HTTP handlers
- raw WebSocket handlers and broadcast
- actor sleep and wake
- per-actor SQLite databases

State changed from an HTTP or WebSocket handler must be persisted explicitly
with `ctx.Save`.

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
- [Raw HTTP counter](examples/http-counter): a `net/http` actor API with
  explicit state persistence

## Configuration

Pass a `rivet.Config` to `Serve` to configure the engine endpoint, namespace,
runner identity, logging, metrics hooks, SQLite transport, and graceful
shutdown deadline:

```go
err := rivet.Serve(registry, rivet.Config{
	Endpoint:   "http://127.0.0.1:6420",
	Namespace:  "default",
	RunnerName: "my-runner",
})
```

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
