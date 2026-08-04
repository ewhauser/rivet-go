# Rivet runner performance results

Generated from two sequential repetitions per cell on 2026-08-04T05:15:48Z. Raw JSON, logs, process samples, and Go CPU profiles are committed under `bench/results-archive/2026-08-03`.

## Machine and pins

| Item | Value |
|---|---|
| Machine | Apple M3 Max; 16 logical CPUs; 64 GiB RAM |
| OS | macOS 26.5.2 (25F84) |
| Engine | `Rivet 2.3.10; Git SHA: 957d4e482f404913ca1955d8ecc357533f6fd081; Build Timestamp: 2026-08-03T14:25:23.388170000Z; Rustc Version: 1.97.0; Rustc Host: aarch64-apple-darwin; Cargo Target: aarch64-apple-darwin; Cargo Profile: release` |
| Go | `go version go1.26.5 darwin/arm64`; runner commit `23cdbb9c12396128089c88f19e5c3411bfd39635` |
| Go native library | committed `internal/ffi/lib/darwin_arm64/librivetkit_go_ffi.dylib`; SHA-256 `513cf94b1459eca96038228ff72bab12fd5a65adc37001112f58169ca3cd9bbb` |
| TypeScript | Node `v26.5.0`, npm `11.17.0`, `NODE_ENV=production`, no Node flags; `rivetkit@2.3.10` integrity `sha512-E+H0lBc3O8dK9Pj7W2XW3VwrCnfpwYYm5LlsZyHrmk5bCrJIBdnEFdZXn5nsYMz0waCfP1ieyP6d1tdvBG76Dg==` |
| Rust | `rustc 1.97.0 (2d8144b78 2026-07-07) (Homebrew)`; `cargo 1.97.0 (c980f4866 2026-06-30) (Homebrew)`; `rivetkit` v2.3.10 from `git+https://github.com/rivet-dev/rivet?tag=v2.3.10#957d4e482f404913ca1955d8ecc357533f6fd081`; `cargo build --release --locked` |
| Logging | error level for all runners |

## Scenario definitions

- **S1 hot actor actions:** concurrency 32, one counter actor, repeated `increment(1)` calls.
- **S2 spread actions:** concurrency 64, one worker for each of 64 counter actors.
- **S3 WebSocket echo:** 32 connections to one echo actor; each connection performs sequential 64-byte binary ping-pong round trips.
- **S4 cold start:** 50 fresh actors, sequentially measured from create request through the first persisted or volatile `increment(1)` result. S4 is count-bounded because pacing 50 samples to 60 seconds would fabricate throughput.
- S1-S3 use at least 10 seconds of excluded warmup and a 60-second measured window. S4 uses at least 10 seconds of excluded fresh-actor warmup and then exactly 50 measured actors. Latency uses an HDR histogram with three significant figures. All requests use the same Go HTTP/WebSocket gateway client.

## Summary

The `r1/r2 (delta)` cells show both repetitions and the signed percentage change from run 1 to run 2. CPU is process `%CPU`, where 100% is one fully occupied logical core.

| Scenario | SDK | Persistence | Throughput ops/s r1/r2 (delta) | p50 ms r1/r2 (delta) | p95 ms r1/r2 (delta) | p99 ms r1/r2 (delta) | max ms r1/r2 | Loadgen errors r1/r2 | Engine CPU avg r1/r2 | Runner CPU avg r1/r2 | Valid |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| S1 | Go | persist | 289.5/287.0 (-0.8%) | 14.703/14.407 (-2.0%) | 338.687/339.711 (+0.3%) | 2715.647/2715.647 (+0.0%) | 13475.839/15048.703 | 0/0 | 131.1/132.0 | 6.9/6.9 | true/true |
| S1 | TypeScript | persist | 304.3/308.6 (+1.4%) | 14.343/14.639 (+2.1%) | 336.895/337.407 (+0.2%) | 2689.023/2672.639 (-0.6%) | 14581.759/15007.743 | 0/0 | 128.2/128.7 | 10.3/10.3 | true/true |
| S1 | Rust | persist | 288.1/286.4 (-0.6%) | 14.719/14.951 (+1.6%) | 337.663/340.479 (+0.8%) | 2697.215/2709.503 (+0.5%) | 15007.743/15007.743 | 0/0 | 130.9/133.4 | 3.4/3.5 | true/true |
| S1 | TypeScript | no-persist | 289.3/335.5 (+16.0%) | 7.291/5.751 (-21.1%) | 337.151/179.327 (-46.8%) | 2723.839/2654.207 (-2.6%) | 15007.743/15007.743 | 0/0 | 134.8/136.4 | 4.8/5.5 | true/true |
| S1 | Rust | no-persist | 311.6/336.2 (+7.9%) | 13.479/8.067 (-40.2%) | 179.583/177.407 (-1.2%) | 2693.119/2670.591 (-0.8%) | 14950.399/15007.743 | 0/0 | 139.2/141.7 | 2.9/3.1 | true/true |
| S2 | Go | persist | 271.8/262.7 (-3.4%) | 16.463/16.687 (+1.4%) | 691.199/695.807 (+0.7%) | 5427.199/6619.135 (+22.0%) | 15007.743/15007.743 | 0/0 | 136.7/135.4 | 7.1/6.8 | true/true |
| S2 | TypeScript | persist | 269.7/261.6 (-3.0%) | 16.719/16.735 (+0.1%) | 690.687/694.783 (+0.6%) | 5480.447/5459.967 (-0.4%) | 15007.743/15089.663 | 0/0 | 132.9/137.8 | 10.5/10.2 | true/true |
| S2 | Rust | persist | 273.9/261.2 (-4.6%) | 16.463/16.623 (+1.0%) | 694.271/696.831 (+0.4%) | 5451.775/5554.175 (+1.9%) | 15007.743/15007.743 | 0/0 | 135.6/136.8 | 3.5/3.4 | true/true |
| S2 | TypeScript | no-persist | 359.9/311.7 (-13.4%) | 14.767/15.047 (+1.9%) | 662.015/679.935 (+2.7%) | 4104.191/5390.335 (+31.3%) | 15089.663/15056.895 | 0/0 | 129.0/142.1 | 5.8/5.3 | true/true |
| S2 | Rust | no-persist | 370.8/328.0 (-11.5%) | 14.687/14.991 (+2.1%) | 472.063/674.303 (+42.8%) | 4102.143/5345.279 (+30.3%) | 15007.743/15097.855 | 0/0 | 135.0/138.5 | 3.5/3.2 | true/true |
| S3 | Go | not-applicable | 3615.8/3629.7 (+0.4%) | 8.767/8.807 (+0.5%) | 9.751/9.711 (-0.4%) | 10.319/10.095 (-2.2%) | 112.447/93.119 | 0/0 | 461.0/455.9 | 66.6/66.7 | true/true |
| S3 | TypeScript | not-applicable | 4659.9/4633.0 (-0.6%) | 6.803/6.891 (+1.3%) | 7.735/7.795 (+0.8%) | 8.139/8.155 (+0.2%) | 91.967/107.583 | 0/0 | 440.1/437.1 | 32.4/32.6 | true/true |
| S3 | Rust | not-applicable | 3734.7/3816.7 (+2.2%) | 8.455/8.319 (-1.6%) | 11.391/11.031 (-3.2%) | 13.335/12.287 (-7.9%) | 34.047/23.967 | 0/0 | 896.0/895.1 | 20.5/20.7 | true/true |
| S4 | Go | persist | 16.6/15.8 (-4.7%) | 57.951/61.247 (+5.7%) | 73.215/73.919 (+1.0%) | 78.079/75.199 (-3.7%) | 78.079/75.199 | 0/0 | 106.5/113.2 | 1.9/2.0 | true/true |
| S4 | TypeScript | persist | 17.6/16.9 (-3.9%) | 54.623/56.991 (+4.3%) | 66.943/69.759 (+4.2%) | 69.759/70.911 (+1.7%) | 69.759/70.911 | 0/0 | 101.5/113.5 | 4.4/4.5 | true/true |
| S4 | Rust | persist | 16.4/15.8 (-3.3%) | 60.639/61.151 (+0.8%) | 70.975/71.871 (+1.3%) | 76.031/74.367 (-2.2%) | 76.031/74.367 | 0/0 | 107.8/113.0 | 1.5/1.5 | true/true |
| S4 | TypeScript | no-persist | 19.2/17.8 (-7.3%) | 51.871/55.903 (+7.8%) | 54.271/58.687 (+8.1%) | 60.607/68.159 (+12.5%) | 60.607/68.159 | 0/0 | 116.9/125.0 | 4.0/4.3 | true/true |
| S4 | Rust | no-persist | 17.7/16.6 (-6.4%) | 56.479/60.255 (+6.7%) | 61.503/63.391 (+3.1%) | 67.711/71.039 (+4.9%) | 67.711/71.039 | 0/0 | 119.2/125.8 | 1.6/1.5 | true/true |

## S1 hot actor actions

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 17755 | 289.5 | 14.703 | 338.687 | 2715.647 | 13475.839 | 0 | true (21721/21721) | 131.1/143.7 | 6.9/8.2 | 70.5/71.6 |
| Go | persist | 2 | 17606 | 287.0 | 14.407 | 339.711 | 2715.647 | 15048.703 | 0 | true (20700/20700) | 132.0/228.7 | 6.9/8.1 | 74.9/76.0 |
| TypeScript | persist | 1 | 18868 | 304.3 | 14.343 | 336.895 | 2689.023 | 14581.759 | 0 | true (22843/22843) | 128.2/189.9 | 10.3/16.3 | 505.7/756.8 |
| TypeScript | persist | 2 | 18894 | 308.6 | 14.639 | 337.407 | 2672.639 | 15007.743 | 0 | true (22358/22358) | 128.7/137.5 | 10.3/16.8 | 983.4/1008.3 |
| Rust | persist | 1 | 17719 | 288.1 | 14.719 | 337.663 | 2697.215 | 15007.743 | 0 | true (21527/21527) | 130.9/157.6 | 3.4/4.3 | 24.3/25.4 |
| Rust | persist | 2 | 17699 | 286.4 | 14.951 | 340.479 | 2709.503 | 15007.743 | 0 | true (20869/20869) | 133.4/160.7 | 3.5/4.3 | 28.6/29.8 |
| TypeScript | no-persist | 1 | 17724 | 289.3 | 7.291 | 337.151 | 2723.839 | 15007.743 | 0 | true (19027/19027) | 134.8/166.2 | 4.8/13.7 | 386.7/661.3 |
| TypeScript | no-persist | 2 | 20587 | 335.5 | 5.751 | 179.327 | 2654.207 | 15007.743 | 0 | true (24607/24607) | 136.4/281.9 | 5.5/25.7 | 930.0/964.4 |
| Rust | no-persist | 1 | 19133 | 311.6 | 13.479 | 179.583 | 2693.119 | 14950.399 | 0 | true (21956/21956) | 139.2/207.5 | 2.9/3.8 | 22.8/24.1 |
| Rust | no-persist | 2 | 20631 | 336.2 | 8.067 | 177.407 | 2670.591 | 15007.743 | 0 | true (24700/24700) | 141.7/208.9 | 3.1/3.8 | 27.7/28.6 |

## S2 spread actions

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 16822 | 271.8 | 16.463 | 691.199 | 5427.199 | 15007.743 | 0 | true (19596/19596) | 136.7/155.8 | 7.1/8.2 | 84.8/89.5 |
| Go | persist | 2 | 16357 | 262.7 | 16.687 | 695.807 | 6619.135 | 15007.743 | 0 | true (19076/19076) | 135.4/158.6 | 6.8/8.5 | 95.9/96.4 |
| TypeScript | persist | 1 | 16839 | 269.7 | 16.719 | 690.687 | 5480.447 | 15007.743 | 0 | true (19635/19635) | 132.9/149.2 | 10.5/14.0 | 1046.6/1052.0 |
| TypeScript | persist | 2 | 16397 | 261.6 | 16.735 | 694.783 | 5459.967 | 15089.663 | 0 | true (19207/19207) | 137.8/262.3 | 10.2/12.8 | 1076.9/1079.8 |
| Rust | persist | 1 | 16987 | 273.9 | 16.463 | 694.271 | 5451.775 | 15007.743 | 0 | true (19792/19792) | 135.6/202.0 | 3.5/5.0 | 38.4/43.0 |
| Rust | persist | 2 | 16347 | 261.2 | 16.623 | 696.831 | 5554.175 | 15007.743 | 0 | true (19076/19076) | 136.8/203.8 | 3.4/4.5 | 49.2/49.6 |
| TypeScript | no-persist | 1 | 22419 | 359.9 | 14.767 | 662.015 | 4104.191 | 15089.663 | 0 | true (26057/26057) | 129.0/160.9 | 5.8/17.2 | 1093.3/1118.9 |
| TypeScript | no-persist | 2 | 19429 | 311.7 | 15.047 | 679.935 | 5390.335 | 15056.895 | 0 | true (23018/23018) | 142.1/208.8 | 5.3/12.3 | 1148.9/1152.2 |
| Rust | no-persist | 1 | 22783 | 370.8 | 14.687 | 472.063 | 4102.143 | 15007.743 | 0 | true (26423/26423) | 135.0/163.7 | 3.5/4.0 | 39.4/41.7 |
| Rust | no-persist | 2 | 20396 | 328.0 | 14.991 | 674.303 | 5345.279 | 15097.855 | 0 | true (23380/23380) | 138.5/193.6 | 3.2/4.0 | 48.2/48.7 |

## S3 WebSocket echo

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | not-applicable | 1 | 216970 | 3615.8 | 8.767 | 9.751 | 10.319 | 112.447 | 0 | true (216970/216970) | 461.0/713.9 | 66.6/68.3 | 95.3/97.2 |
| Go | not-applicable | 2 | 217807 | 3629.7 | 8.807 | 9.711 | 10.095 | 93.119 | 0 | true (217807/217807) | 455.9/482.5 | 66.7/67.6 | 94.2/94.2 |
| TypeScript | not-applicable | 1 | 279615 | 4659.9 | 6.803 | 7.735 | 8.139 | 91.967 | 0 | true (279615/279615) | 440.1/697.8 | 32.4/39.3 | 1089.6/1094.4 |
| TypeScript | not-applicable | 2 | 278001 | 4633.0 | 6.891 | 7.795 | 8.155 | 107.583 | 0 | true (278001/278001) | 437.1/538.9 | 32.6/37.9 | 1089.0/1089.2 |
| Rust | not-applicable | 1 | 224099 | 3734.7 | 8.455 | 11.391 | 13.335 | 34.047 | 0 | true (224099/224099) | 896.0/904.9 | 20.5/21.4 | 49.9/49.9 |
| Rust | not-applicable | 2 | 229020 | 3816.7 | 8.319 | 11.031 | 12.287 | 23.967 | 0 | true (229020/229020) | 895.1/908.4 | 20.7/21.4 | 50.0/50.0 |

## S4 cold start

| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Loadgen errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|
| Go | persist | 1 | 50 | 16.6 | 57.951 | 73.215 | 78.079 | 78.079 | 0 | true (50/50) | 106.5/111.1 | 1.9/2.1 | 102.7/104.2 |
| Go | persist | 2 | 50 | 15.8 | 61.247 | 73.919 | 75.199 | 75.199 | 0 | true (50/50) | 113.2/114.8 | 2.0/2.2 | 118.2/120.3 |
| TypeScript | persist | 1 | 50 | 17.6 | 54.623 | 66.943 | 69.759 | 69.759 | 0 | true (50/50) | 101.5/104.7 | 4.4/4.5 | 1165.3/1173.1 |
| TypeScript | persist | 2 | 50 | 16.9 | 56.991 | 69.759 | 70.911 | 70.911 | 0 | true (50/50) | 113.5/115.6 | 4.5/4.6 | 1271.5/1279.3 |
| Rust | persist | 1 | 50 | 16.4 | 60.639 | 70.975 | 76.031 | 76.031 | 0 | true (50/50) | 107.8/109.4 | 1.5/1.7 | 55.7/57.2 |
| Rust | persist | 2 | 50 | 15.8 | 61.151 | 71.871 | 74.367 | 74.367 | 0 | true (50/50) | 113.0/115.9 | 1.5/1.6 | 68.9/70.3 |
| TypeScript | no-persist | 1 | 50 | 19.2 | 51.871 | 54.271 | 60.607 | 60.607 | 0 | true (50/50) | 116.9/118.3 | 4.0/4.3 | 1238.4/1247.4 |
| TypeScript | no-persist | 2 | 50 | 17.8 | 55.903 | 58.687 | 68.159 | 68.159 | 0 | true (50/50) | 125.0/126.6 | 4.3/4.4 | 1348.5/1357.2 |
| Rust | no-persist | 1 | 50 | 17.7 | 56.479 | 61.503 | 67.711 | 67.711 | 0 | true (50/50) | 119.2/120.4 | 1.6/1.6 | 60.2/61.2 |
| Rust | no-persist | 2 | 50 | 16.6 | 60.255 | 63.391 | 71.039 | 71.039 | 0 | true (50/50) | 125.8/127.6 | 1.5/1.6 | 74.1/75.5 |

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

- S1 Go persist run 1: engine 131.1%, runner 6.9%
- S1 Go persist run 2: engine 132.0%, runner 6.9%
- S1 TypeScript persist run 1: engine 128.2%, runner 10.3%
- S1 TypeScript persist run 2: engine 128.7%, runner 10.3%
- S1 Rust persist run 1: engine 130.9%, runner 3.4%
- S1 Rust persist run 2: engine 133.4%, runner 3.5%
- S1 TypeScript no-persist run 1: engine 134.8%, runner 4.8%
- S1 TypeScript no-persist run 2: engine 136.4%, runner 5.5%
- S1 Rust no-persist run 1: engine 139.2%, runner 2.9%
- S1 Rust no-persist run 2: engine 141.7%, runner 3.1%
- S2 Go persist run 1: engine 136.7%, runner 7.1%
- S2 Go persist run 2: engine 135.4%, runner 6.8%
- S2 TypeScript persist run 1: engine 132.9%, runner 10.5%
- S2 TypeScript persist run 2: engine 137.8%, runner 10.2%
- S2 Rust persist run 1: engine 135.6%, runner 3.5%
- S2 Rust persist run 2: engine 136.8%, runner 3.4%
- S2 TypeScript no-persist run 1: engine 129.0%, runner 5.8%
- S2 TypeScript no-persist run 2: engine 142.1%, runner 5.3%
- S2 Rust no-persist run 1: engine 135.0%, runner 3.5%
- S2 Rust no-persist run 2: engine 138.5%, runner 3.2%
- S3 Go not-applicable run 1: engine 461.0%, runner 66.6%
- S3 Go not-applicable run 2: engine 455.9%, runner 66.7%
- S3 TypeScript not-applicable run 1: engine 440.1%, runner 32.4%
- S3 TypeScript not-applicable run 2: engine 437.1%, runner 32.6%
- S3 Rust not-applicable run 1: engine 896.0%, runner 20.5%
- S3 Rust not-applicable run 2: engine 895.1%, runner 20.7%
- S4 Go persist run 1: engine 106.5%, runner 1.9%
- S4 Go persist run 2: engine 113.2%, runner 2.0%
- S4 TypeScript persist run 1: engine 101.5%, runner 4.4%
- S4 TypeScript persist run 2: engine 113.5%, runner 4.5%
- S4 Rust persist run 1: engine 107.8%, runner 1.5%
- S4 Rust persist run 2: engine 113.0%, runner 1.5%
- S4 TypeScript no-persist run 1: engine 116.9%, runner 4.0%
- S4 TypeScript no-persist run 2: engine 125.0%, runner 4.3%
- S4 Rust no-persist run 1: engine 119.2%, runner 1.6%
- S4 Rust no-persist run 2: engine 125.8%, runner 1.5%

### Archived internal error logs

These counts are error-level log records, grouped by exact message patterns. They are not added to load-generator errors because they are background or teardown diagnostics rather than failed measured operations.

| Archived log | Error-level records | Exact-pattern breakdown |
|---|---:|---|
| `engine-go.log` | 1445 | `sqlite worker close channel dropped without clean close`: 1318; `websocket failed`: 96; `http req ingress metrics failed`: 24; `http req egress metrics failed, likely corrupt now`: 2; `long signal recv time`: 5 |
| `engine-typescript.log` | 226 | `websocket failed`: 64; `http req ingress metrics failed`: 42; `http req egress metrics failed, likely corrupt now`: 5; `long signal recv time`: 115 |
| `engine-rust.log` | 2718 | `sqlite worker close channel dropped without clean close`: 2552; `websocket failed`: 64; `http req ingress metrics failed`: 36; `http req egress metrics failed, likely corrupt now`: 4; `long signal recv time`: 62 |
| `runner-go-persist.log` | 0 | none |
| `runner-typescript-persist.log` | 0 | none |
| `runner-typescript-no-persist.log` | 0 | none |
| `runner-rust-persist.log` | 715 | `shutdown serialize-state enqueue failed`: 633; `serializeState callback returned error`: 16; `serializeState timed out`: 2; `failed to dispatch websocket close event`: 64 |
| `runner-rust-no-persist.log` | 625 | `shutdown serialize-state enqueue failed`: 598; `serializeState callback returned error`: 25; `serializeState timed out`: 2 |

## Go CPU profiles

Profiling-only S1 and S3 runs are excluded from every table above. Their pprof data and text tops are in `bench/results-archive/2026-08-03/go-s1-cpu.pprof`, `bench/results-archive/2026-08-03/go-s3-cpu.pprof`, and the adjacent `*-pprof-top.txt` files.
