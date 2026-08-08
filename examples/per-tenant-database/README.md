# Per-tenant company database

This ports Rivet's `per-tenant-database` example. Each actor key identifies a
company and gets isolated durable state for employees and projects. The
`getCompany` action also demonstrates `Context.Name`, `Context.Key`, and
`Context.ActorID`.

The upstream example calls this a database because each keyed actor owns its
own persistent dataset; it uses actor state rather than SQLite.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

In another terminal, start the example:

```sh
go run ./examples/per-tenant-database
```

Create the actor for one company:

```sh
ACTOR_ID="$(
  curl -fsS -X POST 'http://127.0.0.1:6420/actors?namespace=default' \
    -H 'Authorization: Bearer dev' \
    -H 'Content-Type: application/json' \
    -d '{"name":"company-database","key":"acme","runner_name_selector":"per-tenant-database-example","crash_policy":"destroy"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["actor"]["actor_id"])'
)"
```

Add an employee and inspect the company:

```sh
curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/addEmployee" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{"name":"Ada","role":"Engineer"}]}'

curl -fsS -X POST \
  "http://127.0.0.1:6420/gateway/$ACTOR_ID/action/getCompany" \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"args":[{}]}'
```

`addProject`, `listProjects`, `listEmployees`, and `getStats` expose the rest
of the example. Create another actor with a different key to get an independent
company dataset.
