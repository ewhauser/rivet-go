# Durable AI agent

This example ports RivetKit's AI-agent actor pattern: actions enqueue prompts,
a generation-owned `Run` loop consumes them from a durable queue, and
`KeepAwake` keeps the actor alive while a model provider is running. Conversation
state and completed replies are persisted before each queue message is
acknowledged, so an interrupted completion can be retried without duplicating
the committed reply.

The included `echoProvider` keeps the example runnable without credentials. It
implements the small `Provider` interface used by the actor; replace it with an
adapter for the model SDK your application uses.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

In another terminal, start the example:

```sh
go run ./examples/ai-agent
```

Create an actor:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"ai-agent","runner_name_selector":"ai-agent-example","crash_policy":"restart"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Enqueue a prompt through the typed action API:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/sendMessage" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"content":"Explain durable actors in one sentence."}]}'
```

Call `getMessages` to read the persisted conversation. External Go callers can
also use `ActorHandle.Queue().SendAndWait` with queue name `prompts` and a
`{"id":"...","content":"..."}` body to wait directly for the assistant
reply.
