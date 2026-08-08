# SQLite todo list

This example stores a todo list in an actor-local SQLite database. It shows
schema migration during `OnStart`, CRUD actions, result decoding, and an
explicit transaction.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

In another terminal, start the example:

```sh
go run ./examples/todo-sqlite
```

Create a todo-list actor:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"todo-list","runner_name_selector":"todo-sqlite-example","crash_policy":"destroy"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Add and list todos:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/add" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"title":"port the SQLite example"}]}'

curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/list" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{}]}'
```

The `toggle` and `delete` actions accept `{"id":1}`. Each actor gets an
isolated database; creating a second `todo-list` actor creates a separate list.
