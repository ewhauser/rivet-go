# Rivet runner performance results

Generated from two sequential repetitions per cell on 2026-08-04T07:32:26Z. Raw JSON, logs, process samples, and Go CPU profiles are committed under `bench/results-archive/2026-08-04-post-optimization`.

## Machine and pins

| Item | Value |
|---|---|
| Machine | Apple M3 Max; 16 logical CPUs; 64 GiB RAM |
| OS | macOS 26.5.2 (25F84) |
| Engine | `Rivet 2.3.10; Git SHA: 957d4e482f404913ca1955d8ecc357533f6fd081; Build Timestamp: 2026-08-03T14:25:23.388170000Z; Rustc Version: 1.97.0; Rustc Host: aarch64-apple-darwin; Cargo Target: aarch64-apple-darwin; Cargo Profile: release` |
| Go | `go version go1.26.5 darwin/arm64`; runner commit `71f731c8d7cddfdc397802352c84b73809942d62` |
| Go native library | committed `internal/ffi/lib/darwin_arm64/librivetkit_go_ffi.dylib`; SHA-256 `f65addc87c6a54d819330366a616dce267bdb74250bc8c5153b9a88be6131393` |
| TypeScript | Node `v26.5.0`, npm `11.17.0`, `NODE_ENV=production`, no Node flags; `rivetkit@2.3.10` integrity `sha512-E+H0lBc3O8dK9Pj7W2XW3VwrCnfpwYYm5LlsZyHrmk5bCrJIBdnEFdZXn5nsYMz0waCfP1ieyP6d1tdvBG76Dg==` |
| Rust | `rustc 1.97.0 (2d8144b78 2026-07-07) (Homebrew)`; `cargo 1.97.0 (c980f4866 2026-06-30) (Homebrew)`; `rivetkit` v2.3.10 from `git+https://github.com/rivet-dev/rivet?tag=v2.3.10#957d4e482f404913ca1955d8ecc357533f6fd081`; `cargo build --release --locked` |
| Logging | error level for all runners |

## Scenario definitions

- **S1 hot actor actions:** concurrency 32, one counter actor, repeated `increment(1)` calls.
- **S2 spread actions:** concurrency 64, one worker for each of 64 counter actors.
- **S3 WebSocket echo:** 32 connections to one echo actor; each connection performs sequential 64-byte binary ping-pong round trips.
- **S4 cold start:** 50 fresh actors, sequentially measured from create request through the first persisted or volatile `increment(1)` result. S4 is count-bounded because pacing 50 samples to 60 seconds would fabricate throughput.
- S1-S3 use at least 10 seconds of excluded warmup and a 60-second measured window. S4 uses at least 10 seconds of excluded fresh-actor warmup and then exactly 50 measured actors. Latency uses an HDR histogram with three significant figures. All requests use the same Go HTTP/WebSocket gateway client.

## Post-optimization comparison

The comparison below averages the two reportable persistent repetitions in
this archive and the committed first-run archive at
`bench/results-archive/2026-08-03`. S3 has no persistence variant.

| Scenario | Go throughput before/after | Change | Go p50 before/after | Change | Go runner CPU before/after | Change |
|---|---:|---:|---:|---:|---:|---:|
| S1 | 288.2 / 285.9 ops/s | -0.8% | 14.555 / 14.611 ms | +0.4% | 6.9% / 5.8% | -16.5% |
| S3 | 3,622.8 / 3,641.5 msg/s | +0.5% | 8.787 / 8.747 ms | -0.5% | 66.6% / 53.0% | -20.5% |
| S4 | 16.2 / 16.4 ops/s | +1.1% | 59.599 / 60.303 ms | +1.2% | 1.9% / 1.6% | -18.1% |

The kept changes reduce Go runner CPU consistently without materially moving
reportable throughput or latency. In S3, the final Go runner uses 20.5% less
CPU for 0.5% more messages per second, reducing its CPU multiple over
TypeScript from 2.05x to 1.62x. The remaining final S3 gap is 21.6% throughput
and 27.7% p50 latency versus TypeScript.

Final same-run persistent averages:

| Scenario | SDK | Throughput ops/s | p50 ms | Runner CPU avg |
|---|---|---:|---:|---:|
| S1 | Go | 285.9 | 14.611 | 5.8% |
| S1 | TypeScript | 304.8 | 14.407 | 10.4% |
| S1 | Rust | 286.4 | 14.899 | 3.6% |
| S3 | Go | 3,641.5 | 8.747 | 53.0% |
| S3 | TypeScript | 4,647.0 | 6.849 | 32.7% |
| S3 | Rust | 4,732.0 | 6.725 | 19.2% |
| S4 | Go | 16.4 | 60.303 | 1.6% |
| S4 | TypeScript | 17.6 | 55.423 | 4.7% |
| S4 | Rust | 16.3 | 59.807 | 1.7% |

Rust S3 rose from 3,775.7 to 4,732.0 msg/s between archives despite no Rust
runner change in the Go optimization commits. That cross-run movement is not
credited to this work; same-run rows are the fair final cross-SDK comparison.

## Summary

The `r1/r2 (delta)` cells show both repetitions and the signed percentage change from run 1 to run 2. CPU is process `%CPU`, where 100% is one fully occupied logical core.

| Scenario | SDK | Persistence | Throughput ops/s r1/r2 (delta) | p50 ms r1/r2 (delta) | p95 ms r1/r2 (delta) | p99 ms r1/r2 (delta) | max ms r1/r2 | Loadgen errors r1/r2 | Engine CPU avg r1/r2 | Runner CPU avg r1/r2 | Valid |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| S1 | Go | persist | 286.1/285.6 (-0.2%) | 14.583/14.639 (+0.4%) | 338.431/336.639 (-0.5%) | 2695.167/2689.023 (-0.2%) | 13590.527/15007.743 | 0/0 | 131.7/131.9 | 5.8/5.8 | true/true |
| S1 | TypeScript | persist | 303.5/306.1 (+0.9%) | 14.479/14.335 (-1.0%) | 335.871/179.199 (-46.6%) | 2684.927/2684.927 (+0.0%) | 14934.015/14917.631 | 0/0 | 125.4/128.6 | 10.2/10.5 | true/true |
| S1 | Rust | persist | 285.5/287.2 (+0.6%) | 14.783/15.015 (+1.6%) | 335.359/339.199 (+1.1%) | 2695.167/2693.119 (-0.1%) | 15007.743/15007.743 | 0/0 | 130.5/133.5 | 3.6/3.6 | true/true |
| S1 | TypeScript | no-persist | 284.9/333.0 (+16.9%) | 9.599/5.427 (-43.5%) | 341.759/326.143 (-4.6%) | 2711.551/2674.687 (-1.4%) | 15007.743/15007.743 | 0/0 | 140.8/136.4 | 4.9/5.6 | true/true |
| S1 | Rust | no-persist | 284.2/331.2 (+16.5%) | 13.743/7.059 (-48.6%) | 335.359/177.535 (-47.1%) | 2721.791/2678.783 (-1.6%) | 15007.743/15007.743 | 0/0 | 138.3/136.5 | 2.7/3.1 | true/true |
| S2 | Go | persist | 273.1/261.5 (-4.2%) | 16.215/16.527 (+1.9%) | 691.711/693.247 (+0.2%) | 5513.215/5451.775 (-1.1%) | 15007.743/15073.279 | 0/0 | 138.8/139.8 | 5.9/5.7 | true/true |
| S2 | TypeScript | persist | 270.6/262.7 (-2.9%) | 16.479/16.703 (+1.4%) | 693.247/692.223 (-0.1%) | 5439.487/5533.695 (+1.7%) | 15106.047/15065.087 | 0/0 | 132.3/133.3 | 10.5/10.3 | true/true |
| S2 | Rust | persist | 272.5/261.1 (-4.2%) | 16.319/16.911 (+3.6%) | 687.615/697.855 (+1.5%) | 5435.391/5472.255 (+0.7%) | 15007.743/15007.743 | 0/0 | 134.9/137.4 | 3.6/3.5 | true/true |
| S2 | TypeScript | no-persist | 359.8/312.2 (-13.2%) | 14.599/15.015 (+2.8%) | 657.919/675.839 (+2.7%) | 4100.095/5382.143 (+31.3%) | 15007.743/15007.743 | 0/0 | 135.7/144.1 | 6.2/5.2 | true/true |
| S2 | Rust | no-persist | 363.3/313.8 (-13.6%) | 14.655/15.071 (+2.8%) | 354.047/673.791 (+90.3%) | 4169.727/5431.295 (+30.3%) | 15007.743/15097.855 | 0/0 | 139.3/146.7 | 3.7/3.1 | true/true |
| S3 | Go | not-applicable | 3623.0/3659.9 (+1.0%) | 8.759/8.735 (-0.3%) | 9.759/9.639 (-1.2%) | 10.351/9.983 (-3.6%) | 106.495/118.975 | 0/0 | 465.2/462.2 | 52.8/53.2 | true/true |
| S3 | TypeScript | not-applicable | 4603.7/4690.4 (+1.9%) | 6.891/6.807 (-1.2%) | 7.831/7.707 (-1.6%) | 8.351/8.011 (-4.1%) | 94.463/118.143 | 0/0 | 438.5/436.2 | 32.5/32.9 | true/true |
| S3 | Rust | not-applicable | 4701.9/4762.2 (+1.3%) | 6.747/6.703 (-0.7%) | 7.695/7.627 (-0.9%) | 8.039/7.915 (-1.5%) | 111.807/74.111 | 0/0 | 437.0/433.3 | 19.1/19.3 | true/true |
| S4 | Go | persist | 16.8/16.0 (-4.3%) | 59.007/61.599 (+4.4%) | 70.463/69.055 (-2.0%) | 75.903/74.303 (-2.1%) | 75.903/74.303 | 0/0 | 109.7/117.3 | 1.6/1.6 | true/true |
| S4 | TypeScript | persist | 17.8/17.3 (-2.7%) | 54.335/56.511 (+4.0%) | 65.471/67.711 (+3.4%) | 72.191/70.463 (-2.4%) | 72.191/70.463 | 0/0 | 106.2/114.0 | 4.8/4.5 | true/true |
| S4 | Rust | persist | 16.7/15.9 (-5.0%) | 58.591/61.023 (+4.2%) | 71.487/75.391 (+5.5%) | 75.327/78.783 (+4.6%) | 75.327/78.783 | 0/0 | 111.6/117.0 | 1.7/1.7 | true/true |
| S4 | TypeScript | no-persist | 19.5/17.5 (-10.4%) | 50.943/56.255 (+10.4%) | 53.695/64.063 (+19.3%) | 62.495/67.647 (+8.2%) | 62.495/67.647 | 0/0 | 118.9/124.1 | 4.3/4.1 | true/true |
| S4 | Rust | no-persist | 17.6/16.4 (-6.8%) | 56.703/60.767 (+7.2%) | 59.391/64.735 (+9.0%) | 59.807/72.063 (+20.5%) | 59.807/72.063 | 0/0 | 120.7/126.8 | 1.7/1.6 | true/true |

## S1 hot actor actions

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 17737 | 286.1 | 14.583 | 338.431 | 2695.167 | 13590.527 | 0 | true (21657/21657) | 131.7/146.9 | 5.8/6.7 | 70.4/71.6 |
| Go | persist | 2 | 17533 | 285.6 | 14.639 | 336.639 | 2689.023 | 15007.743 | 0 | true (20734/20734) | 131.9/143.6 | 5.8/6.7 | 74.8/76.0 |
| TypeScript | persist | 1 | 18900 | 303.5 | 14.479 | 335.871 | 2684.927 | 14934.015 | 0 | true (22656/22656) | 125.4/144.2 | 10.2/14.3 | 504.4/751.5 |
| TypeScript | persist | 2 | 18796 | 306.1 | 14.335 | 179.199 | 2684.927 | 14917.631 | 0 | true (22275/22275) | 128.6/161.7 | 10.5/25.5 | 979.8/1005.6 |
| Rust | persist | 1 | 17737 | 285.5 | 14.783 | 335.359 | 2695.167 | 15007.743 | 0 | true (21505/21505) | 130.5/149.7 | 3.6/4.6 | 24.5/25.7 |
| Rust | persist | 2 | 17687 | 287.2 | 15.015 | 339.199 | 2693.119 | 15007.743 | 0 | true (20941/20941) | 133.5/172.5 | 3.6/4.2 | 28.9/30.0 |
| TypeScript | no-persist | 1 | 17448 | 284.9 | 9.599 | 341.759 | 2711.551 | 15007.743 | 0 | true (18676/18676) | 140.8/215.2 | 4.9/7.9 | 380.0/643.0 |
| TypeScript | no-persist | 2 | 20448 | 333.0 | 5.427 | 326.143 | 2674.687 | 15007.743 | 0 | true (24426/24426) | 136.4/179.2 | 5.6/14.4 | 920.2/952.9 |
| Rust | no-persist | 1 | 17623 | 284.2 | 13.743 | 335.359 | 2721.791 | 15007.743 | 0 | true (18888/18888) | 138.3/175.7 | 2.7/3.4 | 22.8/24.6 |
| Rust | no-persist | 2 | 20645 | 331.2 | 7.059 | 177.535 | 2678.783 | 15007.743 | 0 | true (24777/24777) | 136.5/181.2 | 3.1/4.1 | 28.0/29.3 |

## S2 spread actions

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 16919 | 273.1 | 16.215 | 691.711 | 5513.215 | 15007.743 | 0 | true (19696/19696) | 138.8/262.6 | 5.9/7.2 | 84.5/89.3 |
| Go | persist | 2 | 16308 | 261.5 | 16.527 | 693.247 | 5451.775 | 15073.279 | 0 | true (19032/19032) | 139.8/218.8 | 5.7/7.6 | 95.5/96.0 |
| TypeScript | persist | 1 | 16906 | 270.6 | 16.479 | 693.247 | 5439.487 | 15106.047 | 0 | true (19692/19692) | 132.3/160.3 | 10.5/14.0 | 1045.8/1052.3 |
| TypeScript | persist | 2 | 16312 | 262.7 | 16.703 | 692.223 | 5533.695 | 15065.087 | 0 | true (19097/19097) | 133.3/170.9 | 10.3/12.9 | 1079.2/1081.5 |
| Rust | persist | 1 | 16965 | 272.5 | 16.319 | 687.615 | 5435.391 | 15007.743 | 0 | true (19774/19774) | 134.9/156.6 | 3.6/4.5 | 38.5/43.2 |
| Rust | persist | 2 | 16184 | 261.1 | 16.911 | 697.855 | 5472.255 | 15007.743 | 0 | true (18926/18926) | 137.4/151.8 | 3.5/4.4 | 46.4/46.8 |
| TypeScript | no-persist | 1 | 22485 | 359.8 | 14.599 | 657.919 | 4100.095 | 15007.743 | 0 | true (26124/26124) | 135.7/182.9 | 6.2/18.2 | 1083.0/1107.5 |
| TypeScript | no-persist | 2 | 19371 | 312.2 | 15.015 | 675.839 | 5382.143 | 15007.743 | 0 | true (22896/22896) | 144.1/238.2 | 5.2/9.6 | 1139.6/1143.0 |
| Rust | no-persist | 1 | 22435 | 363.3 | 14.655 | 354.047 | 4169.727 | 15007.743 | 0 | true (26145/26145) | 139.3/302.0 | 3.7/4.4 | 39.1/42.4 |
| Rust | no-persist | 2 | 19533 | 313.8 | 15.071 | 673.791 | 5431.295 | 15097.855 | 0 | true (22870/22870) | 146.7/261.0 | 3.1/4.0 | 46.6/48.5 |

## S3 WebSocket echo

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | not-applicable | 1 | 217402 | 3623.0 | 8.759 | 9.759 | 10.351 | 106.495 | 0 | true (217402/217402) | 465.2/742.2 | 52.8/54.2 | 94.8/96.7 |
| Go | not-applicable | 2 | 219615 | 3659.9 | 8.735 | 9.639 | 9.983 | 118.975 | 0 | true (219615/219615) | 462.2/529.8 | 53.2/54.1 | 93.7/93.8 |
| TypeScript | not-applicable | 1 | 276241 | 4603.7 | 6.891 | 7.831 | 8.351 | 94.463 | 0 | true (276241/276241) | 438.5/675.7 | 32.5/40.3 | 1089.7/1094.5 |
| TypeScript | not-applicable | 2 | 281442 | 4690.4 | 6.807 | 7.707 | 8.011 | 118.143 | 0 | true (281442/281442) | 436.2/453.9 | 32.9/38.2 | 1088.6/1088.6 |
| Rust | not-applicable | 1 | 282132 | 4701.9 | 6.747 | 7.695 | 8.039 | 111.807 | 0 | true (282132/282132) | 437.0/646.9 | 19.1/19.7 | 47.0/47.0 |
| Rust | not-applicable | 2 | 285750 | 4762.2 | 6.703 | 7.627 | 7.915 | 74.111 | 0 | true (285750/285750) | 433.3/532.8 | 19.3/19.7 | 47.1/47.1 |

## S4 cold start

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 50 | 16.8 | 59.007 | 70.463 | 75.903 | 75.903 | 0 | true (50/50) | 109.7/111.9 | 1.6/1.8 | 100.7/101.7 |
| Go | persist | 2 | 50 | 16.0 | 61.599 | 69.055 | 74.303 | 74.303 | 0 | true (50/50) | 117.3/119.6 | 1.6/1.7 | 116.0/118.6 |
| TypeScript | persist | 1 | 50 | 17.8 | 54.335 | 65.471 | 72.191 | 72.191 | 0 | true (50/50) | 106.2/108.9 | 4.8/5.2 | 1167.8/1175.3 |
| TypeScript | persist | 2 | 50 | 17.3 | 56.511 | 67.711 | 70.463 | 70.463 | 0 | true (50/50) | 114.0/115.6 | 4.5/4.8 | 1276.8/1284.9 |
| Rust | persist | 1 | 50 | 16.7 | 58.591 | 71.487 | 75.327 | 75.327 | 0 | true (50/50) | 111.6/112.0 | 1.7/1.8 | 55.9/56.9 |
| Rust | persist | 2 | 50 | 15.9 | 61.023 | 75.391 | 78.783 | 78.783 | 0 | true (50/50) | 117.0/119.7 | 1.7/1.8 | 69.4/70.9 |
| TypeScript | no-persist | 1 | 50 | 19.5 | 50.943 | 53.695 | 62.495 | 62.495 | 0 | true (50/50) | 118.9/119.7 | 4.3/4.4 | 1222.4/1231.4 |
| TypeScript | no-persist | 2 | 50 | 17.5 | 56.255 | 64.063 | 67.647 | 67.647 | 0 | true (50/50) | 124.1/125.1 | 4.1/4.3 | 1328.6/1336.6 |
| Rust | no-persist | 1 | 50 | 17.6 | 56.703 | 59.391 | 59.807 | 59.807 | 0 | true (50/50) | 120.7/122.0 | 1.7/1.8 | 57.7/58.7 |
| Rust | no-persist | 2 | 50 | 16.4 | 60.767 | 64.735 | 72.063 | 72.063 | 0 | true (50/50) | 126.8/127.6 | 1.6/1.6 | 71.8/73.3 |

## Caveats

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

- S1 Go persist run 1: engine 131.7%, runner 5.8%
- S1 Go persist run 2: engine 131.9%, runner 5.8%
- S1 TypeScript persist run 1: engine 125.4%, runner 10.2%
- S1 TypeScript persist run 2: engine 128.6%, runner 10.5%
- S1 Rust persist run 1: engine 130.5%, runner 3.6%
- S1 Rust persist run 2: engine 133.5%, runner 3.6%
- S1 TypeScript no-persist run 1: engine 140.8%, runner 4.9%
- S1 TypeScript no-persist run 2: engine 136.4%, runner 5.6%
- S1 Rust no-persist run 1: engine 138.3%, runner 2.7%
- S1 Rust no-persist run 2: engine 136.5%, runner 3.1%
- S2 Go persist run 1: engine 138.8%, runner 5.9%
- S2 Go persist run 2: engine 139.8%, runner 5.7%
- S2 TypeScript persist run 1: engine 132.3%, runner 10.5%
- S2 TypeScript persist run 2: engine 133.3%, runner 10.3%
- S2 Rust persist run 1: engine 134.9%, runner 3.6%
- S2 Rust persist run 2: engine 137.4%, runner 3.5%
- S2 TypeScript no-persist run 1: engine 135.7%, runner 6.2%
- S2 TypeScript no-persist run 2: engine 144.1%, runner 5.2%
- S2 Rust no-persist run 1: engine 139.3%, runner 3.7%
- S2 Rust no-persist run 2: engine 146.7%, runner 3.1%
- S3 Go not-applicable run 1: engine 465.2%, runner 52.8%
- S3 Go not-applicable run 2: engine 462.2%, runner 53.2%
- S3 TypeScript not-applicable run 1: engine 438.5%, runner 32.5%
- S3 TypeScript not-applicable run 2: engine 436.2%, runner 32.9%
- S3 Rust not-applicable run 1: engine 437.0%, runner 19.1%
- S3 Rust not-applicable run 2: engine 433.3%, runner 19.3%
- S4 Go persist run 1: engine 109.7%, runner 1.6%
- S4 Go persist run 2: engine 117.3%, runner 1.6%
- S4 TypeScript persist run 1: engine 106.2%, runner 4.8%
- S4 TypeScript persist run 2: engine 114.0%, runner 4.5%
- S4 Rust persist run 1: engine 111.6%, runner 1.7%
- S4 Rust persist run 2: engine 117.0%, runner 1.7%
- S4 TypeScript no-persist run 1: engine 118.9%, runner 4.3%
- S4 TypeScript no-persist run 2: engine 124.1%, runner 4.1%
- S4 Rust no-persist run 1: engine 120.7%, runner 1.7%
- S4 Rust no-persist run 2: engine 126.8%, runner 1.6%

### Archived internal error logs

These counts are error-level log records, grouped by exact message patterns. They are not added to load-generator errors because they are background or teardown diagnostics rather than failed measured operations.

| Archived log | Error-level records | Exact-pattern breakdown |
|---|---:|---|
| `engine-go.log` | 1444 | `sqlite worker close channel dropped without clean close`: 1314; `websocket failed`: 96; `http req ingress metrics failed`: 24; `long signal recv time`: 10 |
| `engine-typescript.log` | 208 | `websocket failed`: 64; `http req ingress metrics failed`: 43; `http req egress metrics failed, likely corrupt now`: 4; `long signal recv time`: 97 |
| `engine-rust.log` | 2705 | `sqlite worker close channel dropped without clean close`: 2550; `websocket failed`: 64; `http req ingress metrics failed`: 35; `http req egress metrics failed, likely corrupt now`: 3; `long signal recv time`: 53 |
| `runner-go-persist.log` | 0 | none |
| `runner-typescript-persist.log` | 0 | none |
| `runner-typescript-no-persist.log` | 0 | none |
| `runner-rust-persist.log` | 720 | `shutdown serialize-state enqueue failed`: 628; `serializeState callback returned error`: 27; `serializeState timed out`: 1; `failed to dispatch websocket close event`: 64 |
| `runner-rust-no-persist.log` | 619 | `shutdown serialize-state enqueue failed`: 597; `serializeState callback returned error`: 19; `serializeState timed out`: 3 |

## Go CPU profiles

Profiling-only S1 and S3 runs are excluded from every table above. Their pprof data and text tops are in `bench/results-archive/2026-08-04-post-optimization/go-s1-cpu.pprof`, `bench/results-archive/2026-08-04-post-optimization/go-s3-cpu.pprof`, and the adjacent `*-pprof-top.txt` files.

The fixed 30-second S3 profile accumulated 15.44 seconds of runner samples,
down from 19.14 seconds in the archived first run. Absolute samples moved as
follows:

| Profile entry | First run | Post-optimization | Change |
|---|---:|---:|---:|
| Opaque native (`<unknown>`) | 11.58 s | 7.97 s | -31.2% |
| `runtime.cgocall` | 2.48 s | 2.42 s | -2.4% |
| `Runner.Poll` cumulative | 1.34 s | 1.41 s | +5.2% |
| `Runner.Submit` cumulative | 1.16 s | 1.02 s | -12.1% |
| `runtime.pthread_cond_wait` | 2.62 s | 2.49 s | -5.0% |
| `runtime.pthread_cond_signal` | 1.59 s | 1.93 s | +21.4% |

The total sample reduction is 19.3%, led by 3.61 fewer seconds in opaque
native work. FFI call overhead itself is essentially unchanged, consistent
with retaining one poll and two submits per echo. Scheduler signaling is now a
larger share and even rises in absolute time, while serialization remains below
one percent; neither serialization work nor the inactive 64-event cap is a
supported next target.
