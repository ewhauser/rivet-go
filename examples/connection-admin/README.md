# Raw WebSocket connection admin

This example demonstrates snapshots of live raw gateway WebSockets. Actions
can list connections, send a text frame to one connection, or disconnect one
connection by ID. It uses raw WebSockets intentionally: RivetKit's
ActorConnect per-connection state is not part of the Go API yet.

Start Rivet Engine and the example:

```sh
go run ./cmd/rivet-go-dev
```

```sh
go run ./examples/connection-admin
```

Create its actor:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"connection-admin","runner_name_selector":"connection-admin-example","crash_policy":"destroy"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Connect clients to this URL, setting either the `x-client-label` header or the
`client` query parameter:

```text
ws://127.0.0.1:6420/gateway/ACTOR_ID/websocket/admin?client=alpha
```

The gateway client must offer these WebSocket subprotocols:

```text
rivet
rivet_target.actor
rivet_actor.ACTOR_ID
rivet_token.dev
```

List active connections:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/listConnections" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{}]}'
```

Pass an ID from that response to `send`:

```json
{"args":[{"connectionId":"CONNECTION_ID","message":"private message"}]}
```

The `disconnect` action accepts the same `connectionId` plus an optional
application close code from 3000 through 4999 and a reason.
