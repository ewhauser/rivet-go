# Actor KV

This example consolidates Rivet's actor-KV documentation samples into one
runnable Go actor. It demonstrates text and binary values, prefix listing,
reverse and limited scans, missing-key detection, and deletion.

Actor KV is available for compatibility with RivetKit. Prefer typed state or
per-actor SQLite for new application data unless byte-oriented KV is the right
fit.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

In another terminal, start the example and create its actor:

```sh
go run ./examples/actor-kv
```

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"kv-store","runner_name_selector":"actor-kv-example","crash_policy":"destroy"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Store and read a greeting:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/putText" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"key":"greeting:ada","value":"hello"}]}'

curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/getText" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"key":"greeting:ada"}]}'
```

`listText` accepts `prefix`, `reverse`, and `limit`. `roundtripBytes` accepts
integer byte values such as `{"key":"avatar","values":[0,127,255]}`. The
`delete` action reports whether the key existed.
