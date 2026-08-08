# Raw HTTP counter

This example exposes an actor as a small HTTP API using `OnFetch` and the Go
standard library. Unlike actions, HTTP handlers must call `Context.Save`
explicitly after mutating actor state.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

In another terminal, start the example:

```sh
go run ./examples/http-counter
```

Create an HTTP counter actor:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"http-counter","runner_name_selector":"http-counter-example","crash_policy":"destroy"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Call the actor through its raw HTTP gateway:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/request/increment" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"amount":3}'

curl -fsS \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/request/count" \
  -H 'Authorization: Bearer dev'
```

Both requests return `{"count":3}`. `POST /reset` sets the count back to zero.
