# M4 adversarial review

## Summary

| Severity | Found | Fixed | Outstanding |
| --- | ---: | ---: | ---: |
| Blocker | 0 | 0 | 0 |
| Major | 4 | 4 | 0 |
| **Total** | **4** | **4** | **0** |

Review baseline: the requested M4 commits `737f7fb`, `1bedee9`, `3d2d4c5`,
and `869d5b2`. The supervisor fuzz-seed commit `2d3f2e4` was preserved, no fuzz
command was run, and no fuzz file was changed. Protocol and boundary fixes are
in `42cb4a4`; expanded real-engine conformance is in `637613a`; all six native
artifacts were regenerated in `643038b`. The final full-suite index-origin
correction is in `a94d2eb`, with its reproducible artifacts in `f22a47c`.

## Findings

1. **Major — raw broadcast frames were incompatible with the pinned Rivet
   client protocol.**

   Evidence: the M4 FFI encoded a self-consistent CBOR map with `event` and
   `args`, and conformance decoded that same invented map. At pin `957d4e48`,
   `rivetkit-core/src/registry/actor_connect.rs:294-381` writes a one-field
   envelope whose body is the tagged `Event` variant with `name` and `args`.
   `rivetkit-rust/packages/client/src/protocol/codec.rs:266-277` decodes that
   same event variant. A real rivetkit client would not recognize the M4 raw
   frame.

   Fix: `42cb4a4` centralizes the official actor-connect event encoding and
   uses it for command validation and raw-client broadcast delivery. The
   complete encoded frame, not only its argument bytes, is limited to 1 MiB.
   The Rust test decodes the result through the pinned shape, and
   `637613a` makes every public-gateway broadcast assertion decode that shape.

2. **Major — the documented stalled-client close policy was not exercised by
   the real engine and the original public test could not overflow it.**

   Evidence: the original test sent 16 sequential broadcasts and asserted only
   that a reading peer progressed; it explicitly deferred overflow to a Rust
   registry unit. A focused baseline run sent 4096 sequential targeted
   messages to a gateway client with no read loop and never observed the
   documented close. The reason is visible at the pin in
   `engine/sdks/rust/envoy-client/src/config.rs:195-230`: accepted sends enter
   an unbounded downstream channel, so one-command FFI submissions let the
   64-entry admission queue drain immediately.

   Fix: `42cb4a4` batches concurrent Go submissions up to the existing
   1024-command boundary ceiling, making the per-connection admission limit
   load-bearing under a burst. `637613a` stalls a real gateway client, submits
   512 concurrent targeted sends, requires close code 1013 with reason
   `outbound_backpressure` at both the hook and gateway, proves a reading peer
   remains usable, and runs `goleak` after teardown. The deterministic Rust
   isolation test remains in place.

3. **Major — WebSocket message acknowledgements had no ordering invariant.**

   Evidence: each connection stored pending indexes in a `HashSet`; any later
   `WsMessageAck` removed its number regardless of gaps or order. The receive
   side accepted any later index without comparing it to its predecessor. This
   could hide a missed acknowledgement until M5 attempted hibernation replay.

   Fix: `42cb4a4` replaces the set with a per-connection FIFO, requires later
   receives to advance with wrapping-u16 arithmetic, and requires
   acknowledgements to match the FIFO head. A receive gap closes only that
   connection with code 1008 and `ws.message_index_skip`; an out-of-order
   acknowledgement uses code 1008 and `ws.ack_out_of_order`. Unit coverage
   proves ordered removal, wraparound, gap handling, and cleanup on connection
   ID reuse. The first final integration run showed that the pinned gateway
   can assign a nonzero first application-frame index; `a94d2eb` therefore
   establishes the origin from the first observed frame instead of assuming
   zero.

4. **Major — the original boundary checks and real-engine suite left most M4
   failure and lifecycle semantics unproved.**

   Evidence: the one real-engine test had good per-client assertions for one
   50-client broadcast, `BroadcastExcept`, targeted sends, a 1 MiB binary echo,
   action broadcast, and ordinary client/actor close. It did not cover two
   concurrent broadcasts, real overflow, message or broadcast ordering,
   rejection, disconnect races, actor/runner stop, any hook panic, oversize or
   empty frames, state persistence from `OnMessage`, zero-connection hook
   broadcasts, or correlation reuse. `validateEvent` checked WebSocket IDs and
   text UTF-8 but did not enforce the 1 MiB incoming cap or close-code/reason
   rules. The M5 hibernation fields existed in goldens only with false values.

   Fix: `42cb4a4` aligns event validation with public close and frame
   enforcement and changes Rust-generated goldens so Go must preserve true
   `can_hibernate` and `hibernate` markers. `637613a` adds the missing cases
   through real gateway reads and closes. During repeat execution, an asserted
   `OnStop` delivery exposed the pinned engine's shutdown race; the contract is
   now explicit and tested: the hook submission succeeds, delivery is
   best-effort once gateway drain begins, a delivered event precedes close
   1001, and otherwise the client observes that close directly.

## Checked, clean

1. **Fanout theater:** all 50 clients individually receive one single-action
   event and no duplicate. Two action calls then broadcast concurrently; every
   client individually receives both distinct values exactly once and no third
   frame. Assertions consume real gateway WebSocket frames.
2. **Stalled-client policy:** a real no-read gateway client overflows its
   64-entry native admission queue and observes code 1013 with
   `outbound_backpressure`; a reading peer remains usable. Rust checks queue
   isolation, and the real test finishes with `goleak`.
3. **Ordering:** 100 client frames reach one actor in order. Two broadcasts
   initiated serially by one `OnMessage` arrive in order at each client.
   Receive and acknowledgement indexes are FIFO, establish their origin from
   the first frame, and wrap as `u16` without accepting later gaps.
4. **Lifecycle races:** conformance covers disconnect during `OnConnect`,
   client disconnect racing actor `Close`, actor stop with two live clients,
   runner shutdown with a live client, exactly one `OnDisconnect` for each,
   and the documented code-1001 stop behavior. Unit coverage separately proves
   connection-ID correlation cleanup and reuse.
5. **Rejection semantics:** `OnConnect` rejection produces either a gateway
   close with code 1008 and `actor.handler_error` after upgrade or a proper
   upgrade failure. It does not hang, a later client write is rejected, and
   `OnMessage` is not invoked for the rejected connection.
6. **Frame fidelity:** text and binary messages retain their frame kinds in
   both directions; empty frames of both kinds are legal, and an exact 1 MiB
   binary frame remains complete. A client frame over 1 MiB closes that
   connection with code 1009 and `message.incoming_too_long` without running
   `OnMessage`. An oversized actor send returns an error, emits no partial
   frame, and leaves the connection usable.
7. **Broadcast surface integrity:** `BroadcastExcept` excludes exactly its
   named raw connection; action broadcasts reach public gateway clients; a
   zero-connection `OnStart` broadcast is a successful no-op. `OnStop`
   submission and drain behavior are defined and tested. Raw events use the
   official pinned actor-connect envelope, while core retains native
   subscription delivery.
8. **Panic containment:** real `OnConnect`, `OnMessage`, and `OnDisconnect`
   panics structurally stop only their actor. Gateway clients close instead of
   hanging, and a peer actor continues serving actions after every case.
9. **Integration regressions:** the M3 HTTP/action conformance files are
   unchanged and green. M2 restart persistence remains green. A new test saves
   state inside `OnMessage`, replaces the engine process while the runner
   reconnects, and requires the value in the higher actor generation. The
   action-broadcast assertion reads the real gateway event.
10. **Envelope hygiene:** ABI 4 remains single-sourced from Rust through the
    generated C header and Go constant, and the ABI-1 loader fixture remains
    rejected. Rust produces the M4 goldens, Go re-encodes commands
    byte-identically, the M5 marker booleans are true in those goldens, and
    `validateEvent` now enforces frame size, UTF-8, close-code ranges, and the
    123-byte close-reason limit consistently with command/public enforcement.

## Decode surface notes

No fuzz command was invoked, no fuzz test or corpus entry was written, and no
raw binary payload is reproduced here. This review did not add a malformed
MessagePack or CBOR decode test. The acknowledgement invariant tests exercise
typed state-machine transitions, including a skipped sequence number, without
constructing malformed envelope bytes. The mandatory all-package command
continues to execute the repository's ordinary pre-existing test corpus
without a `-fuzz` run.

## Final verification

The final worktree verification included:

- `go test -race -count=1 ./...` with the real pinned engine; conformance
  completed in 261.465 seconds.
- `cargo test --workspace` — 25 Rust tests plus doc tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `go vet ./...`, `cargo fmt --all --check`, and `git diff --check`.
- `scripts/build-ffi.sh` for all six targets, twice. Hashes for all six
  libraries, the checksum manifest, header, and generated ABI constant were
  identical between complete passes. All six SHA-256 manifest entries verify.

No GitHub operation, push, upstream contact, dependency pin change, fuzz run,
or deliberately malformed-input test was performed. The Rivet pin remains
v2.3.10 at `957d4e48`.
