# Rivet runner performance results

Generated from two sequential repetitions per cell on 2026-08-04T14:46:03Z. Raw JSON, logs, process samples, and Go CPU profiles are committed under `bench/results-archive/2026-08-04-hibernation-fix`.

## Machine and pins

| Item | Value |
|---|---|
| Machine | Apple M3 Max; 16 logical CPUs; 64 GiB RAM |
| OS | macOS 26.5.2 (25F84) |
| Engine | `Rivet 2.3.10; Git SHA: 957d4e482f404913ca1955d8ecc357533f6fd081; Build Timestamp: 2026-08-03T14:25:23.388170000Z; Rustc Version: 1.97.0; Rustc Host: aarch64-apple-darwin; Cargo Target: aarch64-apple-darwin; Cargo Profile: release` |
| Go | `go version go1.26.5 darwin/arm64`; runner commit `387eba844f37f681e35c9ebf8e46cfa8222f5149` |
| Go native library | committed `internal/ffi/lib/darwin_arm64/librivetkit_go_ffi.dylib`; SHA-256 `4e593090638c0c7e98040a30b91e940ab513c4c8ca64132e62b1cd010b800826` |
| TypeScript | Node `v26.5.0`, npm `11.17.0`, `NODE_ENV=production`, no Node flags; `rivetkit@2.3.10` integrity `sha512-E+H0lBc3O8dK9Pj7W2XW3VwrCnfpwYYm5LlsZyHrmk5bCrJIBdnEFdZXn5nsYMz0waCfP1ieyP6d1tdvBG76Dg==` |
| Rust | `rustc 1.97.0 (2d8144b78 2026-07-07) (Homebrew)`; `cargo 1.97.0 (c980f4866 2026-06-30) (Homebrew)`; `rivetkit` v2.3.10 from `git+https://github.com/rivet-dev/rivet?tag=v2.3.10#957d4e482f404913ca1955d8ecc357533f6fd081`; `cargo build --release --locked` |
| Logging | error level for all runners |

## Scenario definitions

- **S1 hot actor actions:** concurrency 32, one counter actor, repeated `increment(1)` calls.
- **S2 spread actions:** concurrency 64, one worker for each of 64 counter actors.
- **S3 WebSocket echo:** 32 connections to one echo actor; each connection performs sequential 64-byte binary ping-pong round trips.
- **S4 cold start:** 50 fresh actors, sequentially measured from create request through the first persisted or volatile `increment(1)` result. S4 is count-bounded because pacing 50 samples to 60 seconds would fabricate throughput.
- S1-S3 use at least 10 seconds of excluded warmup and a 60-second measured window. S4 uses at least 10 seconds of excluded fresh-actor warmup and then exactly 50 measured actors. Latency uses an HDR histogram with three significant figures. All requests use the same Go HTTP/WebSocket gateway client.

## Post-hibernation-fix

The Go SDK previously registered every actor with WebSocket hibernation enabled, while the pinned TypeScript and Rust SDKs default it to false. An uncommitted latency investigation observed one engine hibernation acknowledgement per Go echo message and measured only about 36 us in Go's in-runner critical path. In interleaved Go-only investigation runs that changed only this flag, S3 client p50 moved from 8.243 ms to 6.459 ms, about 1.8 ms. Those flag-only A/B runs and the 36 us internal measurement are not part of the committed archive below. The earlier conclusion that Go was roughly 22% behind in S3 because of its callback-free FFI design was therefore a configuration mismatch, not a measured runner-performance cost.

The table below averages the two same-run persistent repetitions for S1 and S4 and the two non-persistence S3 repetitions. All three S3 echo actors use non-hibernating WebSockets.

| Scenario | SDK | Throughput ops/s | p50 ms | Runner CPU avg |
|---|---|---:|---:|---:|
| S1 | Go | 286.0 | 14.707 | 5.7% |
| S1 | TypeScript | 303.3 | 14.195 | 10.4% |
| S1 | Rust | 290.4 | 14.719 | 3.1% |
| S3 | Go | 3642.2 | 8.687 | 51.5% |
| S3 | TypeScript | 3660.1 | 8.643 | 37.4% |
| S3 | Rust | 4839.3 | 6.555 | 19.4% |
| S4 | Go | 16.2 | 60.623 | 1.8% |
| S4 | TypeScript | 17.5 | 55.647 | 3.8% |
| S4 | Rust | 16.4 | 59.199 | 1.4% |

In the corrected same-run S3 rows, Go and TypeScript are essentially even for this loopback workload: their averaged throughput differs by 0.5% and averaged p50 by 0.5%. Both differences are smaller than the respective run-to-run movements shown in the summary table, and the engine-limited caveat below still applies.

## Summary

The `r1/r2 (delta)` cells show both repetitions and the signed percentage change from run 1 to run 2. CPU is process `%CPU`, where 100% is one fully occupied logical core.

| Scenario | SDK | Persistence | Throughput ops/s r1/r2 (delta) | p50 ms r1/r2 (delta) | p95 ms r1/r2 (delta) | p99 ms r1/r2 (delta) | max ms r1/r2 | Loadgen errors r1/r2 | Engine CPU avg r1/r2 | Runner CPU avg r1/r2 | Valid |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| S1 | Go | persist | 284.7/287.2 (+0.9%) | 14.639/14.775 (+0.9%) | 339.199/341.503 (+0.7%) | 2689.023/2707.455 (+0.7%) | 15007.743/15007.743 | 0/0 | 127.9/131.4 | 5.6/5.7 | true/true |
| S1 | TypeScript | persist | 305.9/300.7 (-1.7%) | 14.135/14.255 (+0.8%) | 333.567/180.735 (-45.8%) | 2676.735/2668.543 (-0.3%) | 15007.743/15007.743 | 0/0 | 126.1/126.4 | 10.2/10.5 | true/true |
| S1 | Rust | persist | 291.8/289.1 (-0.9%) | 14.903/14.535 (-2.5%) | 339.455/334.335 (-1.5%) | 2703.359/2713.599 (+0.4%) | 15007.743/15007.743 | 0/0 | 129.9/129.8 | 3.1/3.1 | true/true |
| S1 | TypeScript | no-persist | 289.2/333.7 (+15.4%) | 7.019/5.415 (-22.9%) | 337.407/177.151 (-47.5%) | 2711.551/2664.447 (-1.7%) | 15056.895/15007.743 | 0/0 | 130.9/127.1 | 4.0/4.7 | true/true |
| S1 | Rust | no-persist | 302.8/337.9 (+11.6%) | 14.055/5.647 (-59.8%) | 265.983/181.503 (-31.8%) | 2680.831/2670.591 (-0.4%) | 15007.743/15007.743 | 0/0 | 136.7/136.4 | 2.5/2.7 | true/true |
| S2 | Go | persist | 270.0/260.6 (-3.5%) | 16.495/16.719 (+1.4%) | 690.175/694.271 (+0.6%) | 5451.775/5488.639 (+0.7%) | 15089.663/15007.743 | 0/0 | 134.7/135.0 | 5.7/5.6 | true/true |
| S2 | TypeScript | persist | 275.2/265.0 (-3.7%) | 16.479/16.623 (+0.9%) | 691.711/693.759 (+0.3%) | 5443.583/5476.351 (+0.6%) | 15097.855/15007.743 | 0/0 | 131.8/130.8 | 9.5/9.3 | true/true |
| S2 | Rust | persist | 274.6/265.6 (-3.3%) | 16.479/16.767 (+1.7%) | 688.127/695.295 (+1.0%) | 5472.255/5455.871 (-0.3%) | 15007.743/15089.663 | 0/0 | 133.8/135.5 | 3.0/3.0 | true/true |
| S2 | TypeScript | no-persist | 366.1/316.5 (-13.5%) | 14.639/15.015 (+2.6%) | 350.463/675.839 (+92.8%) | 4104.191/5373.951 (+30.9%) | 15007.743/15065.087 | 0/0 | 131.8/142.1 | 5.2/4.7 | true/true |
| S2 | Rust | no-persist | 366.9/313.9 (-14.4%) | 14.663/14.999 (+2.3%) | 658.943/674.815 (+2.4%) | 4114.431/5373.951 (+30.6%) | 15007.743/15089.663 | 0/0 | 131.3/139.6 | 3.1/2.7 | true/true |
| S3 | Go | not-applicable | 3609.3/3675.1 (+1.8%) | 8.743/8.631 (-1.3%) | 11.775/11.423 (-3.0%) | 13.783/12.743 (-7.5%) | 31.359/31.951 | 0/0 | 892.7/893.2 | 51.1/51.9 | true/true |
| S3 | TypeScript | not-applicable | 3604.6/3715.5 (+3.1%) | 8.751/8.535 (-2.5%) | 11.799/11.327 (-4.0%) | 13.727/12.647 (-7.9%) | 35.647/21.439 | 0/0 | 894.5/894.4 | 36.8/38.0 | true/true |
| S3 | Rust | not-applicable | 4805.2/4873.5 (+1.4%) | 6.583/6.527 (-0.9%) | 7.539/7.467 (-1.0%) | 7.887/7.779 (-1.4%) | 113.663/142.719 | 0/0 | 438.7/432.5 | 19.3/19.5 | true/true |
| S4 | Go | persist | 16.5/16.0 (-3.0%) | 60.511/60.735 (+0.4%) | 70.591/74.047 (+4.9%) | 75.327/74.495 (-1.1%) | 75.327/74.495 | 0/0 | 109.1/115.8 | 1.7/1.8 | true/true |
| S4 | TypeScript | persist | 17.8/17.2 (-3.2%) | 54.495/56.799 (+4.2%) | 65.343/66.303 (+1.5%) | 71.103/68.671 (-3.4%) | 71.103/68.671 | 0/0 | 107.0/113.1 | 3.6/4.0 | true/true |
| S4 | Rust | persist | 16.7/16.2 (-2.5%) | 58.143/60.255 (+3.6%) | 73.023/72.575 (-0.6%) | 78.271/73.663 (-5.9%) | 78.271/73.663 | 0/0 | 107.8/115.8 | 1.4/1.4 | true/true |
| S4 | TypeScript | no-persist | 19.6/17.7 (-9.7%) | 50.591/55.327 (+9.4%) | 53.279/67.007 (+25.8%) | 70.591/73.279 (+3.8%) | 70.591/73.279 | 0/0 | 116.8/123.5 | 3.6/3.3 | true/true |
| S4 | Rust | no-persist | 17.5/16.3 (-6.6%) | 56.447/60.191 (+6.6%) | 66.303/69.183 (+4.3%) | 70.463/74.303 (+5.4%) | 70.463/74.303 | 0/0 | 116.9/121.2 | 1.4/1.3 | true/true |

## S1 hot actor actions

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 17693 | 284.7 | 14.639 | 339.199 | 2689.023 | 15007.743 | 0 | true (21623/21623) | 127.9/142.0 | 5.6/6.8 | 71.4/72.4 |
| Go | persist | 2 | 17625 | 287.2 | 14.775 | 341.503 | 2707.455 | 15007.743 | 0 | true (20862/20862) | 131.4/148.4 | 5.7/6.7 | 75.6/76.8 |
| TypeScript | persist | 1 | 18811 | 305.9 | 14.135 | 333.567 | 2676.735 | 15007.743 | 0 | true (22821/22821) | 126.1/151.6 | 10.2/16.5 | 500.8/757.1 |
| TypeScript | persist | 2 | 18844 | 300.7 | 14.255 | 180.735 | 2668.543 | 15007.743 | 0 | true (22205/22205) | 126.4/237.2 | 10.5/36.7 | 979.2/1002.9 |
| Rust | persist | 1 | 17885 | 291.8 | 14.903 | 339.455 | 2703.359 | 15007.743 | 0 | true (21650/21650) | 129.9/141.9 | 3.1/3.8 | 24.9/26.3 |
| Rust | persist | 2 | 17734 | 289.1 | 14.535 | 334.335 | 2713.599 | 15007.743 | 0 | true (20877/20877) | 129.8/153.0 | 3.1/3.7 | 29.5/30.6 |
| TypeScript | no-persist | 1 | 17739 | 289.2 | 7.019 | 337.407 | 2711.551 | 15056.895 | 0 | true (18989/18989) | 130.9/162.3 | 4.0/5.7 | 384.8/652.3 |
| TypeScript | no-persist | 2 | 20701 | 333.7 | 5.415 | 177.151 | 2664.447 | 15007.743 | 0 | true (24712/24712) | 127.1/175.6 | 4.7/11.3 | 929.2/960.0 |
| Rust | no-persist | 1 | 18396 | 302.8 | 14.055 | 265.983 | 2680.831 | 15007.743 | 0 | true (19806/19806) | 136.7/168.4 | 2.5/3.2 | 22.6/24.3 |
| Rust | no-persist | 2 | 20742 | 337.9 | 5.647 | 181.503 | 2670.591 | 15007.743 | 0 | true (24629/24629) | 136.4/214.0 | 2.7/3.4 | 27.6/28.8 |

## S2 spread actions

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 16854 | 270.0 | 16.495 | 690.175 | 5451.775 | 15089.663 | 0 | true (19625/19625) | 134.7/174.4 | 5.7/7.0 | 85.6/90.3 |
| Go | persist | 2 | 16315 | 260.6 | 16.719 | 694.271 | 5488.639 | 15007.743 | 0 | true (19085/19085) | 135.0/168.0 | 5.6/7.0 | 96.6/97.0 |
| TypeScript | persist | 1 | 17055 | 275.2 | 16.479 | 691.711 | 5443.583 | 15097.855 | 0 | true (19957/19957) | 131.8/146.0 | 9.5/13.1 | 1042.7/1049.3 |
| TypeScript | persist | 2 | 16493 | 265.0 | 16.623 | 693.759 | 5476.351 | 15007.743 | 0 | true (19312/19312) | 130.8/150.5 | 9.3/14.6 | 1074.4/1077.2 |
| Rust | persist | 1 | 17028 | 274.6 | 16.479 | 688.127 | 5472.255 | 15007.743 | 0 | true (19827/19827) | 133.8/153.9 | 3.0/4.1 | 39.3/43.8 |
| Rust | persist | 2 | 16402 | 265.6 | 16.767 | 695.295 | 5455.871 | 15089.663 | 0 | true (19164/19164) | 135.5/158.9 | 3.0/3.9 | 50.0/50.4 |
| TypeScript | no-persist | 1 | 22792 | 366.1 | 14.639 | 350.463 | 4104.191 | 15007.743 | 0 | true (26542/26542) | 131.8/293.2 | 5.2/13.4 | 1093.4/1118.2 |
| TypeScript | no-persist | 2 | 19608 | 316.5 | 15.015 | 675.839 | 5373.951 | 15065.087 | 0 | true (23043/23043) | 142.1/228.0 | 4.7/6.9 | 1143.0/1144.9 |
| Rust | no-persist | 1 | 22634 | 366.9 | 14.663 | 658.943 | 4114.431 | 15007.743 | 0 | true (26261/26261) | 131.3/178.1 | 3.1/4.1 | 38.8/41.8 |
| Rust | no-persist | 2 | 19616 | 313.9 | 14.999 | 674.815 | 5373.951 | 15089.663 | 0 | true (23060/23060) | 139.6/207.7 | 2.7/3.7 | 45.2/45.7 |

## S3 WebSocket echo

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | not-applicable | 1 | 216578 | 3609.3 | 8.743 | 11.775 | 13.783 | 31.359 | 0 | true (216578/216578) | 892.7/908.1 | 51.1/53.0 | 97.5/97.5 |
| Go | not-applicable | 2 | 220531 | 3675.1 | 8.631 | 11.423 | 12.743 | 31.951 | 0 | true (220531/220531) | 893.2/902.7 | 51.9/53.1 | 97.6/97.6 |
| TypeScript | not-applicable | 1 | 216300 | 3604.6 | 8.751 | 11.799 | 13.727 | 35.647 | 0 | true (216300/216300) | 894.5/905.2 | 36.8/41.9 | 1086.1/1090.1 |
| TypeScript | not-applicable | 2 | 222950 | 3715.5 | 8.535 | 11.327 | 12.647 | 21.439 | 0 | true (222950/222950) | 894.4/909.0 | 38.0/39.3 | 1085.5/1085.5 |
| Rust | not-applicable | 1 | 288327 | 4805.2 | 6.583 | 7.539 | 7.887 | 113.663 | 0 | true (288327/288327) | 438.7/697.6 | 19.3/19.8 | 50.6/50.6 |
| Rust | not-applicable | 2 | 292428 | 4873.5 | 6.527 | 7.467 | 7.779 | 142.719 | 0 | true (292428/292428) | 432.5/440.5 | 19.5/19.9 | 50.8/50.8 |

## S4 cold start

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 50 | 16.5 | 60.511 | 70.591 | 75.327 | 75.327 | 0 | true (50/50) | 109.1/111.4 | 1.7/1.8 | 107.4/108.9 |
| Go | persist | 2 | 50 | 16.0 | 60.735 | 74.047 | 74.495 | 74.495 | 0 | true (50/50) | 115.8/117.3 | 1.8/2.2 | 122.1/124.8 |
| TypeScript | persist | 1 | 50 | 17.8 | 54.495 | 65.343 | 71.103 | 71.103 | 0 | true (50/50) | 107.0/108.6 | 3.6/3.9 | 1164.6/1172.3 |
| TypeScript | persist | 2 | 50 | 17.2 | 56.799 | 66.303 | 68.671 | 68.671 | 0 | true (50/50) | 113.1/115.0 | 4.0/4.1 | 1274.7/1283.2 |
| Rust | persist | 1 | 50 | 16.7 | 58.143 | 73.023 | 78.271 | 78.271 | 0 | true (50/50) | 107.8/111.7 | 1.4/1.6 | 58.6/60.2 |
| Rust | persist | 2 | 50 | 16.2 | 60.255 | 72.575 | 73.663 | 73.663 | 0 | true (50/50) | 115.8/117.8 | 1.4/1.4 | 71.8/73.3 |
| TypeScript | no-persist | 1 | 50 | 19.6 | 50.591 | 53.279 | 70.591 | 70.591 | 0 | true (50/50) | 116.8/118.5 | 3.6/3.9 | 1238.2/1247.8 |
| TypeScript | no-persist | 2 | 50 | 17.7 | 55.327 | 67.007 | 73.279 | 73.279 | 0 | true (50/50) | 123.5/124.3 | 3.3/3.7 | 1349.1/1357.3 |
| Rust | no-persist | 1 | 50 | 17.5 | 56.447 | 66.303 | 70.463 | 70.463 | 0 | true (50/50) | 116.9/118.0 | 1.4/1.4 | 57.5/58.6 |
| Rust | no-persist | 2 | 50 | 16.3 | 60.191 | 69.183 | 74.303 | 74.303 | 0 | true (50/50) | 121.2/123.0 | 1.3/1.4 | 71.4/72.9 |

## Caveats

- **The corrected S3 comparison uses matching WebSocket configuration.** Go, TypeScript, and Rust all run the echo actor with hibernation disabled. The earlier Go-only hibernation setting caused an engine acknowledgement on every message and explained the reported S3 latency gap. Uncommitted investigation runs produced the 8.243 ms versus 6.459 ms flag-only A/B and the roughly 36 us Go critical-path measurement; those observations are not in this archive. The committed post-fix S3 rows measure the SDK paths after removing that mismatch, so they do not support attributing the old gap to Go's callback-free FFI design.
- **Persistence is labeled, not assumed.** The strict `persist` rows await a state save before returning the increment result in every SDK. Go's public action adapter performs this save automatically after a successful handler, so an additional `ctx.Save` would double-save. TypeScript's state proxy normally requests deferred persistence; this actor also awaits `saveState({ immediate: true })`. Rust explicitly awaits `Ctx::save_state`. The `no-persist` rows use actor-generation-local values and exist only for TypeScript and Rust because Go exposes no no-persist successful action.
- **The native paths differ.** Go crosses a purego C ABI and MessagePack event-pump hop for each event before using the pinned Rust core. TypeScript crosses N-API between JavaScript and the same core and performs JavaScript/CBOR work. Rust calls the core natively. Those costs are the SDK implementations being measured, but this is not a language-only comparison.
- **Pinned Rust needs the database marker for state.** The standalone git dependency enables `sqlite-remote`, but its registry selects that backend only when `Actor::HAS_DATABASE` is true. Both Rust actors set the marker and issue no application SQL. Omitting it makes new actors fail with `SQLite is unavailable` at this pin.
- **The client path is neutral.** One Go load generator talks only to the engine gateway over loopback HTTP and WebSockets. It never imports a Go, TypeScript, or Rust actor client.
- **The gateway IP limiter is sharded, not removed.** Engine v2.3.10 hard-codes 10,000 requests/minute per client IP and trusts `X-Forwarded-For` as a reverse-proxy input. Each HTTP load worker uses one stable loopback identity, identically for every SDK, so that abuse-control ceiling does not cap the runner test. Every non-2xx response remains an error.
- **Correctness gates validity.** Measured and warmup errors must both be zero. Counter totals are reconciled after S1/S2, every S4 first result must be 1, and every S3 payload must match. Invalid cells are rejected by the report generator.
- **Internal diagnostics are reported separately.** The zero `Loadgen errors` cells count operation-visible HTTP/WebSocket errors, not every error-level record from the runner and engine processes. The archived logs contain pinned-engine background telemetry, SQLite-worker, signal-lag, and WebSocket-teardown diagnostics, plus pinned-Rust lifecycle diagnostics. They did not break any operation or reconciliation gate, but they may consume CPU and are part of the result rather than being silently discarded; exact counts appear below.
- **Freshness and ordering.** The engine is restarted with a new filesystem data directory before each SDK suite. Variants and repetitions within an SDK share that suite's engine process but use fresh uniquely keyed actors. All benchmark invocations are sequential.
- **S4 is deliberately count-bounded.** It reports exactly the requested 50 fresh actors. Its actual elapsed duration and throughput are reported; a forced 60-second pacing window would measure the pacer rather than cold start.
- **CPU attribution is sampled.** Engine and runner `%CPU`/RSS come from one-second `ps` samples during the measured interval. A process near 100% may be saturating one core even when the whole machine has idle cores. The report flags likely engine-limited rows below.
- **Single-machine loopback only.** These values include the engine and local transport on one macOS host. They do not predict networked or multi-host deployments, and the script cannot prove that unrelated host activity or thermal state was identical between suites.
- **Concurrency is part of the SDK behavior.** The same gateway concurrency is offered to every runner. Go dispatches one serialized actor worker; pinned Rust action futures are spawned onto Tokio, and TypeScript callbacks may overlap across awaited native work. The benchmark does not add user locks that would hide those SDK semantics.

### Likely engine-limited cells

These repetitions averaged at least 90% engine CPU and more engine CPU than runner CPU; treat runner-to-runner differences there as potentially engine-capped:

- S1 Go persist run 1: engine 127.9%, runner 5.6%
- S1 Go persist run 2: engine 131.4%, runner 5.7%
- S1 TypeScript persist run 1: engine 126.1%, runner 10.2%
- S1 TypeScript persist run 2: engine 126.4%, runner 10.5%
- S1 Rust persist run 1: engine 129.9%, runner 3.1%
- S1 Rust persist run 2: engine 129.8%, runner 3.1%
- S1 TypeScript no-persist run 1: engine 130.9%, runner 4.0%
- S1 TypeScript no-persist run 2: engine 127.1%, runner 4.7%
- S1 Rust no-persist run 1: engine 136.7%, runner 2.5%
- S1 Rust no-persist run 2: engine 136.4%, runner 2.7%
- S2 Go persist run 1: engine 134.7%, runner 5.7%
- S2 Go persist run 2: engine 135.0%, runner 5.6%
- S2 TypeScript persist run 1: engine 131.8%, runner 9.5%
- S2 TypeScript persist run 2: engine 130.8%, runner 9.3%
- S2 Rust persist run 1: engine 133.8%, runner 3.0%
- S2 Rust persist run 2: engine 135.5%, runner 3.0%
- S2 TypeScript no-persist run 1: engine 131.8%, runner 5.2%
- S2 TypeScript no-persist run 2: engine 142.1%, runner 4.7%
- S2 Rust no-persist run 1: engine 131.3%, runner 3.1%
- S2 Rust no-persist run 2: engine 139.6%, runner 2.7%
- S3 Go not-applicable run 1: engine 892.7%, runner 51.1%
- S3 Go not-applicable run 2: engine 893.2%, runner 51.9%
- S3 TypeScript not-applicable run 1: engine 894.5%, runner 36.8%
- S3 TypeScript not-applicable run 2: engine 894.4%, runner 38.0%
- S3 Rust not-applicable run 1: engine 438.7%, runner 19.3%
- S3 Rust not-applicable run 2: engine 432.5%, runner 19.5%
- S4 Go persist run 1: engine 109.1%, runner 1.7%
- S4 Go persist run 2: engine 115.8%, runner 1.8%
- S4 TypeScript persist run 1: engine 107.0%, runner 3.6%
- S4 TypeScript persist run 2: engine 113.1%, runner 4.0%
- S4 Rust persist run 1: engine 107.8%, runner 1.4%
- S4 Rust persist run 2: engine 115.8%, runner 1.4%
- S4 TypeScript no-persist run 1: engine 116.8%, runner 3.6%
- S4 TypeScript no-persist run 2: engine 123.5%, runner 3.3%
- S4 Rust no-persist run 1: engine 116.9%, runner 1.4%
- S4 Rust no-persist run 2: engine 121.2%, runner 1.3%

### Archived internal error logs

These counts are error-level log records, grouped by exact message patterns. They are not added to load-generator errors because they are background or teardown diagnostics rather than failed measured operations.

| Archived log | Error-level records | Exact-pattern breakdown |
|---|---:|---|
| `engine-go.log` | 1435 | `sqlite worker close channel dropped without clean close`: 1308; `websocket failed`: 96; `http req ingress metrics failed`: 26; `long signal recv time`: 5 |
| `engine-typescript.log` | 201 | `websocket failed`: 64; `http req ingress metrics failed`: 36; `http req egress metrics failed, likely corrupt now`: 6; `long signal recv time`: 95 |
| `engine-rust.log` | 2729 | `sqlite worker close channel dropped without clean close`: 2572; `websocket failed`: 64; `http req ingress metrics failed`: 41; `http req egress metrics failed, likely corrupt now`: 5; `long signal recv time`: 47 |
| `runner-go-persist.log` | 0 | none |
| `runner-typescript-persist.log` | 0 | none |
| `runner-typescript-no-persist.log` | 0 | none |
| `runner-rust-persist.log` | 726 | `shutdown serialize-state enqueue failed`: 655; `serializeState callback returned error`: 7; `failed to dispatch websocket close event`: 64 |
| `runner-rust-no-persist.log` | 624 | `shutdown serialize-state enqueue failed`: 610; `serializeState callback returned error`: 14 |

## Go CPU profiles

Profiling-only S1 and S3 runs are excluded from every table above. Their pprof data and text tops are in `bench/results-archive/2026-08-04-hibernation-fix/go-s1-cpu.pprof`, `bench/results-archive/2026-08-04-hibernation-fix/go-s3-cpu.pprof`, and the adjacent `*-pprof-top.txt` files.

## S5 per-actor SQLite transport candidates

Generated from two sequential repetitions per candidate; the last measured cell started at 2026-08-05T06:56:10Z. Raw JSON, process logs, environment data, and checksums are committed under `bench/results-archive/2026-08-05-sqlite`.

Each repetition uses 32 workers mapped one-to-one to 32 actors, 10 seconds of excluded warmup, and a 45-second measured window. The deterministic operation cycle is 50% point `SELECT`, 40% single-row `INSERT`, and 10% one transaction containing `INSERT`, `UPDATE`, and `SELECT`. Throughput counts the transaction as one composite operation. Final per-actor row counts must equal the post-warmup baseline plus successful measured inserts.

| Runner | Throughput ops/s r1/r2 (avg) | p50 ms r1/r2 | p95 ms r1/r2 | p99 ms r1/r2 | Runner CPU avg r1/r2 | Engine CPU avg r1/r2 | Row reconciliation r1/r2 | Valid |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Go-ffi | 317.6/304.8 (311.2) | 13.807/14.055 | 334.079/327.679 | 2668.543/2680.831 | 11.0%/10.2% | 127.0%/129.0% | 9515/9515; 8966/8966 | true/true |
| Go-socket | 326.0/313.3 (319.7) | 13.815/13.959 | 332.287/332.287 | 2670.591/2660.351 | 9.1%/8.9% | 127.7%/128.2% | 9412/9412; 8907/8907 | true/true |
| TypeScript `c.db` | 317.6/314.3 (315.9) | 13.903/14.439 | 331.263/337.407 | 2670.591/2639.871 | 13.6%/13.5% | 125.4%/129.7% | 9386/9386; 8930/8930 | true/true |

All three suites start a fresh engine data directory. Both Go rows use core's `LocalNative` SQLite worker and differ only in the Go-to-core transport. The TypeScript reference uses `rivetkit@2.3.10` `c.db` raw `execute` and callback `transaction` APIs with the same statements and no ORM. The TypeScript wrapper returns object rows and manages the transaction callback, while the Go API returns column/value matrices and exposes an explicit lease-backed `Tx`; those API-shape costs remain part of the measured SDK paths.

CPU is sampled process `%CPU`, where 100% is one fully occupied logical core. This section records the candidates without selecting a default.
The engine stayed near 127% CPU in every cell while runner CPU remained below
14%. This workload is therefore principally engine/Depot-bound: the measured
end-to-end tie is useful evidence that neither Go transport dominates here,
but it does not isolate transport overhead or predict a runner-bound workload.
The 2.7% difference between the Go averages is smaller than the roughly 4%
movement between repetitions of either Go candidate.
