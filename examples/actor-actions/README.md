# Actor actions

This example ports RivetKit's `actor-actions` demo to Go. It defines company
and employee actors with typed state and actions. A company action also uses
its actor-scoped Go client to create an employee actor.

Start Rivet Engine from the repository root:

```sh
go run ./cmd/rivet-go-dev
```

Then start the example runner:

```sh
go run ./examples/actor-actions
```

The runner accepts `-endpoint`, `-runner-name`, and `-token` flags. The local
development token defaults to `dev`; set it explicitly outside the development
launcher.

Creation input is JSON stored in `rivet.CreateOptions.Input`. Action arguments
and results use the typed `rivet.Call[T]` helper. See the real-engine example
conformance test for a complete client flow that creates a company, calls its
actions, resolves its employee by key, and updates the employee profile.

The source actor never calls itself: Rivet actors serialize action processing,
and the actor-scoped client returns `rivet.ErrSelfCall` before a self-call can
deadlock.
