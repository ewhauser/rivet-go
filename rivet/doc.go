// Package rivet hosts typed Go actors on Rivet Engine.
//
// Register actor definitions on a Registry, then call Serve. Each actor
// generation has one serialized Context containing typed state. Successful
// actions persist the full state automatically; WebSocket and HTTP handlers
// call Context.Save when they mutate state outside an action.
//
// Context exposes actor identity, an actor-scoped Client, live raw WebSocket
// connection snapshots, and the low-level actor KV store. Actor.Database opts
// one actor type into durable SQLite through Context.DB. Config's
// SQLiteTransport selects the shared transport and defaults to FFI; setting it
// to disabled overrides every actor declaration.
//
// NewClient creates an independent concurrency-safe client for resolving,
// creating, and calling actors. Actor handles support typed results through
// Call and raw JSON action calls through ActorHandle.CallRaw. Context.Client
// inherits Engine authentication and transport fields from Config and rejects
// a direct call to its own actor generation.
//
// Serve handles SIGINT and SIGTERM. Registry.Serve accepts a caller-owned
// context for services that already manage signals. Both paths request native
// drain, finish admitted handler work, close raw gateway WebSockets, wait for
// RunnerStopped, and release the native handle.
//
// Config.Logger enables structured slog records. Config.Hooks receives stable
// dependency-free counters, gauges, and poll durations; nil discards both.
package rivet
