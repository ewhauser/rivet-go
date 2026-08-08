# Cross-actor actions

This example ports RivetKit's `cross-actor-actions` demo to Go. Inventory
actors own stock and reservations. Checkout actors use `Context.Client` and
typed `rivet.Call[T]` action calls to reserve or release stock without sharing
state directly.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

Then start the example runner:

```sh
go run ./examples/cross-actor-actions
```

The runner accepts `-endpoint`, `-runner-name`, and `-token` flags. The local
development token defaults to `dev`.

Create one keyed `inventory` actor with JSON creation input, then create a
keyed `checkout` actor and call `addItem`. The checkout resolves inventory by
key, reads its stock, and reserves the requested quantity through two
actor-to-actor action calls. The real-engine conformance suite covers the full
flow, insufficient stock, and cancellation returning reserved items.

Each individual actor remains serial, but a multi-actor workflow is not a
database transaction. Production workflows should account for partial failure
between the reservation call and saving checkout state.
