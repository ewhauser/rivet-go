# Collaborative cursors

This is the Go port of Rivet's `cursors` actor. Each live ActorConnect
connection owns its cursor position, while text labels live in durable actor
state. Cursor and text changes are broadcast as named events.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

Then start the example runner:

```sh
go run ./examples/cursors
```

The actor is registered as `cursor-room` on runner `cursors-example`. Connect
through the ActorConnect WebSocket protocol and call:

- `updateCursor` with `{ "userId": "ada", "x": 120, "y": 80 }`
- `updateText` with `{ "id": "label-1", "userId": "ada", "text": "hello", "x": 120, "y": 80 }`
- `removeText` with `{ "id": "label-1" }`
- `getRoomState` with `{}`

Use `updateCursor` through a long-lived ActorConnect connection. Its state
survives actor hibernation, and disconnecting broadcasts `cursorRemoved` to
the remaining clients.
