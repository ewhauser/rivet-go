// Package rivet hosts typed Go actors on Rivet Engine.
//
// Register actor definitions on a Registry, then call Serve. Each actor
// generation has one serialized Context containing typed state. Successful
// actions persist the full state automatically; WebSocket and HTTP handlers
// call Context.Save when they mutate state outside an action.
//
// Actor.Database opts one actor type into durable SQLite through Context.DB.
// Database-less actors keep the default remote state/KV backend. Config's
// SQLiteTransport selects the shared transport and defaults to FFI; setting it
// to disabled overrides every actor declaration.
//
// Serve handles SIGINT and SIGTERM. Registry.Serve accepts a caller-owned
// context for services that already manage signals. Both paths request native
// drain, finish admitted handler work, close raw gateway WebSockets, wait for
// RunnerStopped, and release the native handle.
//
// Config.Logger enables structured slog records. Config.Hooks receives stable
// dependency-free counters, gauges, and poll durations; nil discards both.
package rivet
