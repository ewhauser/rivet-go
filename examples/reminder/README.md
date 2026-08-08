# Durable reminder

This example schedules one durable reminder per actor. It demonstrates
`Schedule`, `ClearSchedule`, `OnAlarm`, and actor sleep and wake. Scheduling a
new reminder replaces the actor's previous reminder.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

In another terminal, start the example:

```sh
go run ./examples/reminder
```

Create a reminder actor:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"reminder","runner_name_selector":"reminder-example","crash_policy":"restart"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Schedule a reminder, then let the actor sleep:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/schedule" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"message":"check the deploy","delayMilliseconds":5000}]}'

curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/sleep" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{}]}'
```

Rivet Engine wakes the actor and calls `OnAlarm`. Delivery follows the
engine's alarm polling interval, so it may occur after the requested time. Call
the `status` action to inspect `pending`, `dueAtMs`, and `triggeredAtMs`, or call
`cancel` before delivery to clear the alarm.
