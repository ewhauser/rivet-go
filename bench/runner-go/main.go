package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
	"github.com/fxamacker/cbor/v2"
)

type counterState struct {
	Count int64 `json:"count"`
}

type echoState struct{}

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

	return registry.Serve(context.Background(), rivet.Config{
		Endpoint:        endpoint,
		Namespace:       "default",
		RunnerName:      runnerName,
		Version:         1,
		TotalSlots:      100_000,
		LogLevel:        "error",
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		ShutdownTimeout: 10 * time.Second,
	})
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
