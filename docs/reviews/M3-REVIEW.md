# M3 adversarial review

## Summary

| Severity | Found | Fixed | Outstanding |
| --- | ---: | ---: | ---: |
| Blocker | 0 | 0 | 0 |
| Major | 4 | 4 | 0 |
| **Total** | **4** | **4** | **0** |

Review baseline: the requested M3 commits `eaefb28`, `bb5bfca`, and
`da30a75`, with the concurrent supervisor fuzz-seed commit `4ea3855` preserved.
No fuzz command was run and no fuzz file was changed. Fixes are in `4d6525d`;
the expanded real-engine conformance and restart coverage is in `ba6d8f7`.

## Findings

1. **Major — the M3 response writer was racy and advertised flushing that the
   pinned core cannot provide.**

   Evidence: a focused baseline run of
   `TestResponseWriterDoesNotAdvertiseUnavailableFlush` failed because
   `responseWriter` implemented `http.Flusher`, even though v2.3.10 accepts
   only a buffered `Response<Vec<u8>>`. In the same run,
   `TestResponseWriterConcurrentWritesAreSafe` produced race-detector reports
   on `started` and `err`. `finish` also submitted the final chunk before
   marking the writer finished, so a handler-owned goroutine could race a
   write after `OnFetch` returned.

   Fix: `4d6525d` serializes `Write`, `WriteHeader`, and finalization; locks the
   first status and header snapshot; marks the writer finished before the final
   submission; and rejects later writes. It removes the false `http.Flusher`
   implementation and documents the buffered limitation. Unit and public
   gateway tests cover concurrent writes, header/status locking, and a delayed
   write after handler return.

2. **Major — the core action deadline was not propagated to Go handlers.**

   Evidence: Rust used a private 60-second correlation timeout, while the M3
   `ActionCall` event carried no deadline. The actor worker invoked actions with
   its generation-long background context, and both public action adapters
   ignored that context. A Go action therefore had no cooperative way to stop
   when the client had already received core's timeout.

   Fix: `4d6525d` makes the Rust action duration the single source for
   `ActorConfig.action_timeout`, correlation expiry, and the new
   `ActionCall.timeout_ms` field. Go applies that field to every invocation and
   exposes it through `ActionWithContext` and `RawActionWithContext`; nil
   invocation contexts fail structurally. Real-gateway conformance waits for
   the actual 60-second deadline, requires HTTP 408 with
   `actor/action_timed_out`, then proves the same actor serves a later action.
   Rust coverage also proves unknown and expired results cannot consume a live
   call's correlation.

3. **Major — HTTP response and backpressure edge cases could yield incoherent
   or corrupt client-visible behavior.**

   Evidence: the baseline writer did not check `Content-Length`, retried native
   backpressure without its own bound, and joined every repeated response
   field with a comma. The last behavior corrupts multiple `Set-Cookie` fields,
   whose values cannot be represented by the pinned core's one-value header
   map. Header byte sizes were not rejected before crossing the Go scanner's
   1 MiB per-blob ceiling.

   Fix: `4d6525d` enforces declared response length, caps backpressure retry at
   30 seconds, retains the 256-name schema cap, and rejects over-1 MiB header
   names or values structurally. Combinable repeated fields remain joined, but
   multiple `Set-Cookie` values return
   `http_response_repeated_header_unsupported`. Public-gateway coverage proves
   exact `Content-Length`, structured mismatch and response-header errors, a
   16 KiB Cookie value, and the gateway's own deterministic HTTP 431 rejection
   before dispatch when an incoming request has 257 header names.

4. **Major — the original real-engine test did not establish several M3 exit
   conditions.**

   Evidence: the baseline did use the public `/gateway` routes and did assert
   the returned HTTP bodies, so it was not gateway theater. A response over
   2 MiB also necessarily crossed the writer's 1 MiB chunk splitter. However,
   there was no public proof of same-actor action ordering and call-result
   correlation, typed wrong-arity or wrong-type errors, result encode failure,
   `RawAction`, request-body chunk delivery, method/path/query/header fidelity,
   action timeout recovery, or action-state persistence across process restart.
   The large response could only prove boundary chunking, not incremental
   client arrival, because the documented core seam buffers the complete reply.

   Fix: `ba6d8f7` adds all of those cases through the real engine gateway. The
   ordering case queues two calls on one actor and checks both returned values
   while a peer actor completes first. The request case sends and verifies a
   body over 2 MiB plus method, path, query, Host, repeated-header reduction,
   Cookie length, and digest. A separate test mutates state only inside an
   action, kills and replaces the engine process while keeping the runner
   reconnecting, and requires the value in a higher actor generation before a
   public `get` call returns it.

## Checked, clean

1. **Gateway path and chunking:** every M3 conformance call uses
   `/gateway/{actor_id}/action/...` or `/gateway/{actor_id}/request/...` and
   asserts the actual HTTP response. Both request and response cases exceed
   2 MiB, so each necessarily exercises at least three 1 MiB boundary chunks.
   Incremental response arrival is unavailable and is no longer implied:
   v2.3.10 buffers the reply in Rust.
2. **ResponseWriter contract:** first-write locking, concurrent writes,
   post-return writes, exact and mismatched `Content-Length`, and absence of
   `http.Flusher` are covered. Backpressure retry has a 30-second production
   bound and a short regression bound. Client disconnect cannot cancel a Go
   download at this pin because core exposes no disconnect event; the
   limitation remains explicit rather than simulated.
3. **Action dispatch integrity:** one actor worker serializes actions; real
   responses prove order and correlation while a slow actor does not block a
   peer. The Rust table ignores unknown, duplicate, and expired IDs without
   disturbing a live entry. The per-event core deadline reaches Go, the client
   receives a structured timeout, and the actor is healthy after cooperative
   handler exit.
4. **Typed decode edges:** valid CBOR arrays with wrong arity and wrong type
   return `actor/action_decode_failed` through the gateway. Typed result encode
   failure returns `action_encode_failed`. `RawAction` preserves its valid CBOR
   bytes, and nil invocation contexts return a structured error instead of
   panicking.
5. **Panic containment:** direct action and `OnFetch` panics return well-formed
   `actor/handler_panic` HTTP errors, stop only that actor generation, and leave
   a peer actor and the runner usable. As with `net/http`, user-created
   goroutines remain responsible for recovering their own panics.
6. **State and actions:** successful actions perform the documented implicit
   complete-state save. The new restart test contains no explicit `Save`; its
   action-mutated value rehydrates after a real engine replacement. The
   unchanged M2 explicit-save restart test remains separate and green.
7. **Envelope hygiene:** ABI 3 remains sourced from Rust through the generated
   header and Go constant. `ActionCall.timeout_ms` is in the Rust-produced Go
   golden. Every M3 event and command kind has Rust-generated coverage, Go
   command re-encoding stays byte-identical, and the ABI-1 loader fixture is
   rejected. `scan.go` caps did not change; the schema still uses 256 header
   names and the existing 1 MiB blob ceiling.
8. **HTTP tunnel semantics:** a body over 2 MiB reaches `r.Body` intact. The
   boundary abort test cancels both `r.Context()` and a blocked body read.
   Method, path, query, Host, body digest, and Cookie length are preserved.
   Repeated request fields reduce to the last value at v2.3.10; response fields
   that cannot be represented safely fail structurally. Response length and
   body framing are coherent at the client.
9. **Resource lifecycle:** boundary deadline expiry removes the Rust
   correlation before emitting `HttpRequestAbort`. Go removes the request entry
   after the handler returns. Runner shutdown cancels an in-flight request,
   wakes its body reader, and leaves no Go correlation. The related pump tests
   use `goleak`. Core serializes actor stop behind an active request; runner
   shutdown is bounded by the existing drain deadline.
10. **Regression sweep:** the unchanged M1 disconnect/reconnect case and M2
    engine-restart persistence case remain in the full suite. The zero-actor
    real registration still uses an explicit empty manifest, and M2 actors
    with no actions still register and run.

## Decode surface notes

No fuzz command was invoked, no fuzz test or corpus entry was written, and no
deliberately malformed envelope was constructed. The typed action checks use
valid gateway JSON that core converts to valid CBOR arrays with the wrong arity
or value type. The new action-deadline golden is Rust-generated and consumed by
Go. `RawAction` now validates that a handler returned one valid CBOR value
without transcoding it; this review did not add an invalid-CBOR test. The
mandatory all-package command continues to execute the repository's ordinary
pre-existing test corpus without a `-fuzz` run.

## Final verification

The final worktree verification included:

- `go test -race -count=1 ./...` with the real pinned engine; conformance
  completed in 209.238 seconds.
- `cargo test --workspace` — 20 Rust tests plus doc tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `go vet ./...`, `cargo fmt --all --check`, and `git diff --check`.
- `scripts/build-ffi.sh` for all six targets, twice. The second complete pass
  changed no artifact, header, generated ABI constant, checksum, or source
  file. All six SHA-256 manifest entries validate.

No GitHub operation, push, upstream contact, dependency pin change, fuzz run,
or deliberate malformed-input test was performed. The Rivet pin remains
v2.3.10 at `957d4e48`.
