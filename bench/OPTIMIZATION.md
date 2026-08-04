# Go runner optimization log

All throughput and CPU figures use the committed cross-SDK harness and retain
its correctness gates. S3 candidate measurements use two sequential runs with
a 10-second warmup and 60-second measured window. The quick S1 sanity run uses
a 10-second warmup and 15-second measured window.

## Baselines

### Archived first run (`bench/results-archive/2026-08-03`)

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,615.8 | 8.767 | 66.6% | yes |
| S3 | 2 | 3,629.7 | 8.807 | 66.7% | yes |
| S1 | 1 | 289.5 | 14.703 | 6.9% | yes |
| S1 | 2 | 287.0 | 14.407 | 6.9% | yes |

### Fresh adjacent baseline (`9acf70c`)

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 4,046.6 | 7.907 | 72.0% | yes |
| S3 | 2 | 3,598.0 | 8.887 | 66.3% | yes |
| S1 quick | 1 | 279.7 | 14.239 | 6.4% | yes |

The fresh S3 pair averages 3,822.3 msg/s, 8.397 ms p50, and 69.2% runner
CPU. Its 12.5% repetition spread is retained rather than hidden; keep/revert
decisions therefore require an effect larger and more consistent than that
ambient variation, or another adjacent confirmation run.

## Attempts

Attempts are appended here before moving to the next lever.

### 1. Fold handler WebSocket sends into the message acknowledgement batch — reverted

The M1 submitter was instrumented with general pump counters. A 10-second
warmup plus 15-second S3 diagnostic measured 91,470 events, 182,838 commands,
and 182,838 native submit batches: exactly **1.000 command per batch**. The
serialized actor worker waits for `Send` admission before it can enqueue
`WsMessageAck`, so the opportunistic queue drain cannot coalesce an echo.

The attempt staged non-cancelable handler sends until the handler returned and
then submitted `WsSend` followed by its exact per-index `WsMessageAck` in one
batch. Cancelable `SendContext` calls retained synchronous admission. The same
diagnostic then measured 94,542 events, 188,982 commands, and 94,508 submit
batches: **2.000 commands per batch**, halving submit crossings as intended.

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,730.4 | 8.543 | 65.0% | yes |
| S3 | 2 | 3,609.3 | 8.863 | 63.8% | yes |
| S1 quick | 1 | 278.4 | 14.519 | 6.5% | yes |

Against the fresh adjacent baseline averages, S3 throughput changed from
3,822.3 to 3,669.9 msg/s (**-4.0%**), p50 from 8.397 to 8.703 ms
(**+3.6%**), and runner CPU from 69.2% to 64.4% (**-6.9%**). Quick S1 was
effectively flat at 279.7 versus 278.4 ops/s. Correctness remained green, but
the primary throughput and latency target regressed, so the batching behavior
was reverted. The batch-density counters and benchmark-only reporting hook
remain as measurement infrastructure.

### 2. Yield once and redrain the native event queue — reverted

Poll instrumentation measured 98,914 S3 events in 98,891 event-bearing poll
batches: **1.000 events per batch**. The existing Rust poll implementation
already drains every event that is ready, so the 64-event cap is not active and
was not raised. The attempt called `thread::yield_now()` once after the first
drain and then redrained the queue, giving adjacent producers one scheduler
opportunity without a timer or fixed latency floor. A short diagnostic still
measured **1.000 events per batch**.

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,838.3 | 8.295 | 70.7% | yes |
| S3 | 2 | 3,644.7 | 8.767 | 68.1% | yes |
| S1 quick | 1 | 278.3 | 14.695 | 6.4% | yes |

Against the fresh adjacent baseline averages, S3 throughput changed from
3,822.3 to 3,741.5 msg/s (**-2.1%**), p50 from 8.397 to 8.531 ms
(**+1.6%**), and runner CPU from 69.2% to 69.4% (**+0.4%**). Quick S1 was
flat at 279.7 versus 278.3 ops/s. Correctness remained green, but the yield did
not coalesce events and regressed the primary metrics, so it was reverted.

### 3. Let the sole poll goroutine migrate between OS threads — reverted

The attempt removed `runtime.LockOSThread`/`UnlockOSThread` around the single
poll loop. The one-poller invariant remained enforced in Go and Rust, and
purego still bracketed each blocking foreign call for the Go scheduler.

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,450.5 | 9.239 | 63.9% | yes |
| S3 | 2 | 3,183.3 | 10.023 | 60.5% | yes |
| S1 quick | 1 | 277.6 | 14.263 | 6.5% | yes |

Against the fresh adjacent baseline averages, S3 throughput changed from
3,822.3 to 3,316.9 msg/s (**-13.2%**), p50 from 8.397 to 9.631 ms
(**+14.7%**), and runner CPU from 69.2% to 62.2% (**-10.1%**). Lower CPU
reflected less useful work, not improved efficiency. Quick S1 remained flat.
The threading change was reverted, so the documented pinned-thread contract
requires no deviation note.

### 4. Flat-combine submits on the calling goroutine — reverted

The attempt removed the dedicated Go submit goroutine. The first caller became
the submit leader and performed the FFI call itself, while concurrent followers
accumulated behind a mutex into the next bounded `CommandBatch`. This removed
the channel wake and result wake in the serial S3 path while preserving burst
coalescing, backpressure, ordering, and shutdown rejection. Twenty consecutive
pump package test runs passed before measurement.

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,845.1 | 8.279 | 63.3% | yes |
| S3 | 2 | 3,626.9 | 8.815 | 60.6% | yes |
| S1 quick | 1 | 273.3 | 14.095 | 5.9% | yes |

Against the fresh adjacent baseline averages, S3 throughput changed from
3,822.3 to 3,736.0 msg/s (**-2.3%**), p50 from 8.397 to 8.547 ms
(**+1.8%**), and runner CPU from 69.2% to 62.0% (**-10.4%**). Quick S1
throughput changed from 279.7 to 273.3 ops/s (**-2.3%**). The scheduler work
fell, but useful throughput and primary latency both regressed, so the flat
combiner was reverted.

### 5. Queue WebSocket acknowledgements without waiting for submit completion — reverted

The attempt enqueued each exact `WsMessageAck` after its serialized handler and
let the actor worker continue without waiting for the submit result. Native
admission failures still became fatal pump errors; shutdown waited for every
asynchronous acknowledgement; channel FIFO kept acknowledgements before later
commands; and Rust's hibernatable-message operation remained blocked until the
matching acknowledgement was actually handled. Five consecutive pump test
runs passed. S3 submit density nevertheless remained **1.000 command per
batch** because the next event was not ready soon enough to coalesce.

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,916.2 | 8.135 | 69.3% | yes |
| S3 | 2 | 3,579.4 | 8.927 | 64.8% | yes |
| S1 quick | 1 | 287.7 | 14.159 | 6.8% | yes |

Against the fresh adjacent baseline averages, S3 throughput changed from
3,822.3 to 3,747.8 msg/s (**-1.9%**), p50 from 8.397 to 8.531 ms
(**+1.6%**), and runner CPU from 69.2% to 67.0% (**-3.0%**). Quick S1 rose
from 279.7 to 287.7 ops/s, but S1 is engine-bound and the S3 primary metrics
regressed. The asynchronous acknowledgement path was reverted.

### 6. Dispatch native commands on a blocking worker — reverted

The attempt retained the bounded 1,024-batch FIFO and its nonblocking submit
admission, but replaced the Tokio `mpsc` receiver task with a dedicated
`spawn_blocking` command dispatcher backed by a bounded crossbeam channel. This
preserved batch order and backpressure while removing one Tokio task wake per
submitted batch.

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,504.9 | 9.111 | 66.3% | yes |
| S3 | 2 | 3,233.9 | 9.879 | 62.6% | yes |
| S1 quick | 1 | 270.2 | 14.543 | 6.8% | yes |

Against the fresh adjacent baseline averages, S3 throughput changed from
3,822.3 to 3,369.4 msg/s (**-11.8%**), p50 from 8.397 to 9.495 ms
(**+13.1%**), and runner CPU from 69.2% to 64.5% (**-6.8%**). Lower CPU
again reflected less useful work. Quick S1 throughput changed from 279.7 to
270.2 ops/s (**-3.4%**). Correctness remained green, but all primary metrics
regressed, so the dispatcher change was reverted.

### 7. Use one permanent Tokio worker — kept

The native runtime used two permanent Tokio workers even though every
hibernatable WebSocket callback enters `block_in_place` while awaiting its
exact Go acknowledgement. Tokio supplies a replacement worker during that
blocking interval. The attempt reduced the permanent worker count to one,
leaving callback blocking, acknowledgement order, command-queue backpressure,
and the public FFI contract unchanged.

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,847.2 | 8.279 | 58.1% | yes |
| S3 | 2 | 3,690.6 | 8.655 | 56.5% | yes |
| S1 quick | 1 | 279.8 | 13.935 | 5.6% | yes |

The S3 pair averaged 3,768.9 msg/s, 8.467 ms p50, and 57.3% runner CPU.
Against the original fresh baseline, that is **-1.4%** throughput, **+0.8%**
p50, and **-17.2%** runner CPU; throughput and latency are within the baseline's
12.5% repetition spread, while the CPU reduction is large and consistent.

An immediately following two-worker control measured 3,730.5 and 3,585.7
msg/s, 8.551 and 8.919 ms p50, and 67.7% and 66.1% runner CPU; its quick S1
run measured 265.6 ops/s, 14.271 ms p50, and 6.3% runner CPU. Against that
adjacent control, one worker improved average S3 throughput from 3,658.1 to
3,768.9 msg/s (**+3.0%**), p50 from 8.735 to 8.467 ms (**-3.1%**), and CPU
from 66.9% to 57.3% (**-14.4%**). Quick S1 improved from 265.6 to 279.8
ops/s (**+5.3%**), with lower p50 and CPU. The adjacent A/B result confirms
that the reduced scheduler contention improves useful work as well as CPU
efficiency, so the one-worker configuration was kept.

### 8. Replace per-message acknowledgement channels with direct completions — kept

Native sampling identified crossbeam channel receive as the hottest resolved
Rust symbol. Each hibernatable WebSocket message allocated a bounded crossbeam
channel solely to park one callback until its exact `WsMessageAck`. The attempt
replaced that channel with an actor-local completion containing only a mutex,
condition variable, and three states: pending, completed, or cancelled. The
pending FIFO still carries the exact `u16` message index. A matching ack wakes
only its callback; WebSocket close explicitly cancels every pending completion;
and the existing 60-second timeout still closes with the same 1011 reason.

| Scenario | Run | Throughput ops/s | p50 ms | Runner CPU avg | Correct |
|---|---:|---:|---:|---:|---|
| S3 | 1 | 3,942.7 | 8.091 | 56.0% | yes |
| S3 | 2 | 3,752.5 | 8.495 | 54.1% | yes |
| S1 quick | 1 | 280.2 | 14.391 | 5.4% | yes |

The S3 pair averaged 3,847.6 msg/s, 8.293 ms p50, and 55.1% runner CPU.
Against the kept one-worker pair, throughput improved **2.1%**, p50 improved
**2.1%**, and runner CPU fell **3.9%**. Quick S1 was flat at 279.8 versus
280.2 ops/s.

An immediately following crossbeam-channel control measured 3,829.7 and
3,623.4 msg/s, 8.311 and 8.823 ms p50, and 58.2% and 56.0% runner CPU; its
quick S1 measured 287.5 ops/s, 14.327 ms p50, and 5.5% runner CPU. Against
that adjacent control, direct completions improved average S3 throughput from
3,726.6 to 3,847.6 msg/s (**+3.2%**), p50 from 8.567 to 8.293 ms
(**-3.2%**), and CPU from 57.1% to 55.1% (**-3.5%**). Quick S1 moved
**-2.5%**, inside its engine-bound variation. The consistent S3 throughput,
latency, and CPU gains justify keeping the direct completion.
