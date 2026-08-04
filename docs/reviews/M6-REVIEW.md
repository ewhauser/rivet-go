# M6 review — production hardening

## Result

M6 is accepted. Graceful process drain is exercised through a real
`SIGTERM`; the dependency-free operability surface is wired to the pump; the
counter and chat examples build and run through the pinned engine; every Go
test package has a goroutine leak gate; and the strict mixed-workload chaos
soak completed its required bounded profile without a convergence, delivery,
sequence, activation, goroutine, or native-handle failure.

The 24-hour release soak remains an operations runbook item. It is not claimed
as evidence from this implementation session.

## Hardening findings and disposition

1. **Runner shutdown previously conflated process drain and actor sleep.**
   M5 correctly hibernated eligible sockets during ordinary sleep, but applying
   that behavior to a terminating Go process left connection state with no
   process capable of resuming it. The native proxy now marks process drain
   explicitly and closes those sockets with code 1001 and reason `runner
   shutting down`. Ordinary sleep remains hibernating.
2. **The public serve path did not own process signals or require a terminal
   native event.** `rivet.Serve` now handles `SIGINT`, `SIGTERM`, and context
   cancellation, rejects new work, drains already-admitted work under a
   configurable deadline, waits for `RunnerStopped`, and returns an error for
   a non-graceful report.
3. **Runtime behavior had no stable dependency-free observation surface.**
   `rivet.Hooks` exports counter, gauge, and duration observations for poll,
   submit, backpressure, actor lifecycle and panic, live actors/connections,
   and poll latency. `Config.Logger` accepts `log/slog`; nil discards. Hook
   panics are contained and logged. The counter example supplies an `expvar`
   adapter without making Prometheus or another backend an SDK dependency.
4. **A duration-only soak could pass without exercising failure paths.** The
   M6 harness has independent nonzero activation guards for engine
   replacement, client disconnect, sleep/wake, stalled clients, and action
   panic. Workload counters are also nonzero-gated. The final summary is
   emitted only after strict convergence and leak checks pass.
5. **Count-only state or aggregate broadcast assertions would hide corruption.**
   Counter, chat, and alarm actors have independent Go truth models compared
   field-by-field with engine-persisted actor state. Every live chat client has
   an ordered receipt ledger, so a duplicate, omission, wrong sender/body/seq,
   or sequence regression fails at the first mismatch. Final goroutines and
   native runner/error/buffer handles must return to their starting values.

## Process-drain evidence

`TestRunnableExamplesAndSIGTERMDrain` compiles both examples into subprocesses.
It calls the counter through the gateway, then sends `SIGTERM` and requires a
zero exit. It starts the chat example, opens a real gateway WebSocket, starts a
1.5-second action, waits until the handler is in flight, and sends `SIGTERM`.
The action must complete, the socket must receive code 1001 with the documented
reason, the engine must stop listing the runner, and the process must exit
zero. This passed inside the complete race-enabled suite.

## Soak evidence

Command:

```sh
go run ./cmd/soak -duration=15m -intensity=8
```

Final summary:

```text
SOAK PASS {"duration":"15m11.416s","seed":1785807299393930000,"counter_operations":4365,"chat_messages":2161,"expected_receipts":8609,"received_receipts":8609,"alarm_fires":36,"chaos":{"action_panics":24,"client_disconnects":47,"engine_restarts":11,"sleep_wakes":35,"stalled_clients":24},"metrics":{"actor_panics_total":24,"actor_starts_total":109,"actor_stops_total":85,"commands_submitted_total":15979,"events_polled_total":13820},"goroutines_before":3,"goroutines_after":3,"data_dir":"/var/folders/w1/9twq581x5xn7hg5sqflql2y00000gn/T/rivet-go-soak-321342981"}
```

All five mandatory chaos activations are nonzero in that record. The printed
duration includes final convergence, process drain, and bounded leak
settlement after the 15-minute workload window.

## README and examples

The README was followed from its engine command through its counter create and
action transcript against a fresh local engine process. The observed action
output was `{"output":3}` and the HTTP handler then returned `3`.
`examples/counter` and `examples/chat` are also compiled and exercised by real
engine conformance; the counter's optional metrics endpoint demonstrates the
complete `expvar` hook adapter.

## Decode surfaces

M6 does not add an SDK or FFI MessagePack shape and does not change ABI 5. New
tooling-only decode sites are:

- `cmd/soak` management and action JSON from the pinned local engine, with
  response bodies capped before decode;
- `cmd/soak` WebSocket reads of the existing v2.3.10 CBOR actor-connect event
  envelope, under a 1 MiB read limit; and
- subprocess conformance reads of that same existing CBOR envelope.

No fuzz test, deliberately malformed-input test, or raw binary-payload test was
added. The soak does not compare SQL- or engine-ordered result lists; its
unordered Go label sets are copied and sorted before traversal. Assertions use
derived bounded waits; no bare `time.Sleep` is an assertion gate.

## Verification

- `cargo test --workspace` — pass, 29 tests.
- `cargo clippy --workspace --all-targets -- -D warnings` — pass.
- `go vet ./...` — pass.
- `go test -race -count=1 ./...` — pass; real-engine conformance passed in
  533.840 seconds.
- `go test -short -count=1 ./...` — pass, including `goleak` in all six test
  packages without starting the real engine or soak.
- README quickstart transcript — pass against the shared pinned-engine path.
- All six `scripts/build-ffi.sh` targets — pass. A second complete build
  produced the identical `internal/ffi/checksums.txt` digest
  `41441579bb6597b555aa90aeffa4205edb654dd30358f11ee351b6b9f9195760`.
- `cargo fmt --all --check`, `git diff --check`, and the code TODO sweep —
  pass; no unresolved code TODO remains.

No GitHub operation, push, upstream contact, engine pin change, or fuzz-file
change was made.
