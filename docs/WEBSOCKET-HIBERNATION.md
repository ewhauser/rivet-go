# WebSocket hibernation

Raw gateway WebSocket hibernation is opt-in per actor:

```go
err := rivet.Register(registry, "chat", rivet.Actor[chatState]{
    HibernateWebSockets: true,
    OnMessage: func(ctx *rivet.Context[chatState], conn *rivet.Connection, message rivet.Message) {
        // Handle the message.
    },
})
```

The default is false, matching rivetkit TypeScript, rivetkit Rust, and
`rivetkit-core` at the pinned v2.3.10 release. When a default actor sleeps, its
raw WebSockets close with code 1001 and reason `actor sleeping`, and
`OnDisconnect` runs. Clients reconnect through the normal gateway path.

When `HibernateWebSockets` is true, core and the engine retain the transport
while the Go actor generation stops. A later message wakes a new generation
on the same client connection. Hibernation itself does not call
`OnDisconnect`; a real client or server close still does.

Hibernation adds work to every message. Core persists the hibernatable
connection's message index and the engine sends a matching acknowledgement.
The Go-to-Rust boundary also retains a FIFO acknowledgement until the handler
returns. Default sockets skip both that boundary bookkeeping and the engine
acknowledgement. In the uncommitted loopback latency investigation that
motivated the opt-in default, changing only this flag moved Go S3 client p50
from 8.243 ms to 6.459 ms. The observed cost was therefore about 1.8 ms p50 per
echo on that machine and workload; it is not a universal network-latency
estimate, and those investigation-only A/B runs are not in the benchmark
archive.

Enable hibernation for long-lived clients that must remain connected across
actor sleep. Leave it disabled for latency-sensitive sockets that can
reconnect, or for actors that never need to sleep with a socket open. Runner
shutdown always closes raw WebSockets with code 1001 and reason
`runner shutting down`, regardless of this option, because another process
cannot resume the terminating runner's Go connection state.
