package rivet

import "time"

// Hooks receives dependency-free runtime measurements on a serialized
// dispatcher outside pump locks and runtime-critical goroutines. Hooks may
// call back into the SDK and should return quickly so shutdown can drain the
// observation queue. A hook panic is recovered without stopping the runner.
type Hooks interface {
	Counter(name string, delta int64)
	Gauge(name string, value int64)
	ObserveDuration(name string, value time.Duration)
}

// Stable metric names emitted through Hooks.
const (
	MetricEventsPolled      = "events_polled_total"
	MetricEventBatches      = "event_batches_polled_total"
	MetricCommandsSubmitted = "commands_submitted_total"
	MetricSubmitBatches     = "submit_batches_total"
	MetricBackpressureHits  = "backpressure_hits_total"
	MetricActorStarts       = "actor_starts_total"
	MetricActorStops        = "actor_stops_total"
	MetricActorPanics       = "actor_panics_total"
	MetricLiveActors        = "live_actors"
	MetricLiveConnections   = "live_connections"
	MetricPollLatency       = "poll_latency"
)
