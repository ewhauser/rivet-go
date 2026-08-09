# Durable action scheduling

This is a Go port of RivetKit's scheduling example. One actor can hold multiple
pending reminders, each with its own durable ID, action argument, timestamp,
and cancellation lifecycle. Schedules survive actor sleep and runner restarts.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

In another terminal, start the example:

```sh
go run ./examples/scheduling
```

Create an actor:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"scheduled-reminders","runner_name_selector":"scheduling-example","crash_policy":"restart"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Schedule two independent reminders:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/scheduleReminder" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"message":"check the deploy","delayMilliseconds":30000}]}'

curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/scheduleReminder" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"message":"write the changelog","delayMilliseconds":60000}]}'
```

Call `getPendingSchedules` to inspect core's pending records, `cancelReminder`
with `{"args":[{"id":"..."}]}` to cancel one by ID, or `sleep` to evict the
actor while its schedules remain durable. Due schedules wake the actor and run
`triggerReminder` through the ordinary typed action path.
