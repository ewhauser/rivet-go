package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

type counterState struct {
	Count int `json:"count"`
}

type incrementArgs struct {
	Amount int `json:"amount"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "counter:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName, metricsAddress string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "counter-example", "engine-visible runner name")
	flag.StringVar(&metricsAddress, "metrics-address", "", "optional expvar listen address, for example 127.0.0.1:6060")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	var hooks rivet.Hooks
	var metricsServer *http.Server
	if metricsAddress != "" {
		hooks = newExpvarHooks("rivet_go")
		metricsServer = &http.Server{Addr: metricsAddress, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("metrics server stopped", slog.Any("error", err))
			}
		}()
		logger.Info("expvar metrics enabled", slog.String("address", metricsAddress))
	}

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "counter", rivet.Actor[counterState]{
		Actions: rivet.Actions[counterState]{
			"increment": rivet.Action(func(ctx *rivet.Context[counterState], args incrementArgs) (int, error) {
				ctx.State().Count += args.Amount
				if err := ctx.Broadcast("countChanged", ctx.State().Count); err != nil {
					return 0, err
				}
				return ctx.State().Count, nil
			}),
			"get": rivet.Action(func(ctx *rivet.Context[counterState], _ struct{}) (int, error) {
				return ctx.State().Count, nil
			}),
		},
		OnFetch: func(ctx *rivet.Context[counterState], writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = fmt.Fprintf(writer, "%d\n", ctx.State().Count)
		},
	}); err != nil {
		return err
	}

	err := rivet.Serve(registry, rivet.Config{
		Endpoint:   endpoint,
		RunnerName: runnerName,
		TotalSlots: 16,
		LogLevel:   "info",
		Logger:     logger,
		Hooks:      hooks,
	})
	if metricsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if shutdownErr := metricsServer.Shutdown(shutdownCtx); err == nil {
			err = shutdownErr
		}
	}
	return err
}

type expvarHooks struct {
	mu       sync.Mutex
	counters *expvar.Map
	gauges   *expvar.Map
	timings  *expvar.Map
}

func newExpvarHooks(name string) *expvarHooks {
	root := expvar.NewMap(name)
	counters := new(expvar.Map)
	counters.Init()
	gauges := new(expvar.Map)
	gauges.Init()
	timings := new(expvar.Map)
	timings.Init()
	root.Set("counters", counters)
	root.Set("gauges", gauges)
	root.Set("duration_nanoseconds", timings)
	return &expvarHooks{counters: counters, gauges: gauges, timings: timings}
}

func (h *expvarHooks) Counter(name string, delta int64) {
	h.counters.Add(name, delta)
}

func (h *expvarHooks) Gauge(name string, value int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	metric := h.gauges.Get(name)
	if metric == nil {
		integer := new(expvar.Int)
		h.gauges.Set(name, integer)
		metric = integer
	}
	metric.(*expvar.Int).Set(value)
}

func (h *expvarHooks) ObserveDuration(name string, value time.Duration) {
	h.timings.Add(name+"_total", value.Nanoseconds())
	h.timings.Add(name+"_count", 1)
}
