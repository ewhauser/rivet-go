package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

type chatState struct {
	Sequence         uint64 `json:"sequence"`
	CompletedActions uint64 `json:"completedActions"`
}

type chatMessage struct {
	Sequence uint64 `json:"sequence"`
	Text     string `json:"text"`
}

type holdArgs struct {
	Milliseconds int `json:"milliseconds"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chat:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "chat-example", "engine-visible runner name")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "chat", rivet.Actor[chatState]{
		Actions: rivet.Actions[chatState]{
			"stats": rivet.Action(func(ctx *rivet.Context[chatState], _ struct{}) (chatState, error) {
				return *ctx.State(), nil
			}),
			"hold": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[chatState], args holdArgs) (uint64, error) {
				if args.Milliseconds < 1 || args.Milliseconds > 5_000 {
					return 0, errors.New("milliseconds must be between 1 and 5000")
				}
				logger.Info("drain probe action started", slog.String("actor_id", ctx.ActorID()))
				timer := time.NewTimer(time.Duration(args.Milliseconds) * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-actionCtx.Done():
					return 0, actionCtx.Err()
				}
				ctx.State().CompletedActions++
				return ctx.State().CompletedActions, nil
			}),
		},
		OnConnect: func(_ *rivet.Context[chatState], connection *rivet.Connection) error {
			return connection.SendText("connected")
		},
		OnMessage: func(ctx *rivet.Context[chatState], connection *rivet.Connection, message rivet.Message) {
			if message.Binary {
				_ = connection.Close(1003, "text messages only")
				return
			}
			ctx.State().Sequence++
			if err := ctx.Save(context.Background()); err != nil {
				logger.Error("save chat state", slog.Any("error", err))
				_ = connection.Close(1011, "state save failed")
				return
			}
			if err := ctx.Broadcast("message", chatMessage{
				Sequence: ctx.State().Sequence,
				Text:     string(message.Data),
			}); err != nil {
				logger.Error("broadcast chat message", slog.Any("error", err))
			}
		},
		OnDisconnect: func(ctx *rivet.Context[chatState], connection *rivet.Connection) {
			code, reason := connection.CloseInfo()
			logger.Info("chat client disconnected",
				slog.String("actor_id", ctx.ActorID()),
				slog.Any("code", code),
				slog.String("reason", reason),
			)
		},
	}); err != nil {
		return err
	}
	return rivet.Serve(registry, rivet.Config{
		Endpoint:        endpoint,
		RunnerName:      runnerName,
		TotalSlots:      16,
		LogLevel:        "info",
		Logger:          logger,
		ShutdownTimeout: 10 * time.Second,
	})
}
