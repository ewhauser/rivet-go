package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
	"github.com/fxamacker/cbor/v2"
)

type counterState struct {
	Count int64 `json:"count"`
}

type echoState struct{}

type pumpStats struct {
	mu       sync.Mutex
	counters map[string]int64
}

func newPumpStats() *pumpStats {
	return &pumpStats{counters: make(map[string]int64)}
}

func (s *pumpStats) Counter(name string, delta int64) {
	s.mu.Lock()
	s.counters[name] += delta
	s.mu.Unlock()
}

func (*pumpStats) Gauge(string, int64) {}

func (*pumpStats) ObserveDuration(string, time.Duration) {}

func (s *pumpStats) report() {
	s.mu.Lock()
	commands := s.counters[rivet.MetricCommandsSubmitted]
	batches := s.counters[rivet.MetricSubmitBatches]
	events := s.counters[rivet.MetricEventsPolled]
	eventBatches := s.counters[rivet.MetricEventBatches]
	s.mu.Unlock()
	commandAverage := 0.0
	if batches != 0 {
		commandAverage = float64(commands) / float64(batches)
	}
	eventAverage := 0.0
	if eventBatches != 0 {
		eventAverage = float64(events) / float64(eventBatches)
	}
	fmt.Fprintf(
		os.Stderr,
		"pump stats: events=%d event_batches=%d events_per_batch=%.3f commands=%d submit_batches=%d commands_per_batch=%.3f\n",
		events,
		eventBatches,
		eventAverage,
		commands,
		batches,
		commandAverage,
	)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "runner-go:", err)
		os.Exit(1)
	}
}

func run() error {
	endpoint := envOr("RIVET_ENDPOINT", "http://127.0.0.1:6420")
	runnerName := envOr("BENCH_RUNNER_NAME", "bench-go-persist")
	if mode := envOr("BENCH_PERSIST_MODE", "persist"); mode != "persist" {
		return fmt.Errorf("unsupported BENCH_PERSIST_MODE %q: Go actions always persist on successful completion", mode)
	}

	if address := os.Getenv("BENCH_PPROF_ADDR"); address != "" {
		server := &http.Server{
			Addr:              address,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintln(os.Stderr, "runner-go pprof:", err)
			}
		}()
	}

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "counter", rivet.Actor[counterState]{
		Actions: rivet.Actions[counterState]{
			"increment": rivet.Action(func(ctx *rivet.Context[counterState], amount int64) (int64, error) {
				ctx.State().Count += amount
				// The public action adapter performs one awaited Save after this
				// handler returns. Calling Save here would persist twice.
				return ctx.State().Count, nil
			}),
			"get": rivet.RawAction(func(ctx *rivet.Context[counterState], encoded []byte) ([]byte, error) {
				var args []cbor.RawMessage
				if err := cbor.Unmarshal(encoded, &args); err != nil {
					return nil, err
				}
				if len(args) != 0 {
					return nil, fmt.Errorf("get expects no arguments, received %d", len(args))
				}
				return cbor.Marshal(ctx.State().Count)
			}),
		},
	}); err != nil {
		return err
	}
	if err := rivet.Register(registry, "echo", rivet.Actor[echoState]{
		OnMessage: func(_ *rivet.Context[echoState], connection *rivet.Connection, message rivet.Message) {
			if message.Binary {
				_ = connection.SendBinary(message.Data)
				return
			}
			_ = connection.SendText(string(message.Data))
		},
	}); err != nil {
		return err
	}

	var hooks rivet.Hooks
	var stats *pumpStats
	if os.Getenv("BENCH_PUMP_STATS") != "" {
		stats = newPumpStats()
		hooks = stats
		defer stats.report()
	}

	return registry.Serve(context.Background(), rivet.Config{
		Endpoint:        endpoint,
		Namespace:       "default",
		RunnerName:      runnerName,
		Version:         1,
		TotalSlots:      100_000,
		LogLevel:        "error",
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		ShutdownTimeout: 10 * time.Second,
		Hooks:           hooks,
	})
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
