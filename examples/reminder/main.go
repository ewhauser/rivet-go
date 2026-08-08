package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

const maxReminderDelay = 7 * 24 * time.Hour

type reminderState struct {
	Message       string `json:"message"`
	Pending       bool   `json:"pending"`
	DueAtMS       int64  `json:"dueAtMs"`
	TriggeredAtMS int64  `json:"triggeredAtMs"`
}

type scheduleArgs struct {
	Message           string `json:"message"`
	DelayMilliseconds int64  `json:"delayMilliseconds"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "reminder:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "reminder-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerReminder(registry, logger); err != nil {
		return err
	}
	return rivet.Serve(registry, rivet.Config{
		Endpoint:   endpoint,
		RunnerName: runnerName,
		TotalSlots: 16,
		LogLevel:   "info",
		Logger:     logger,
	})
}

func registerReminder(registry *rivet.Registry, logger *slog.Logger) error {
	return rivet.Register(registry, "reminder", rivet.Actor[reminderState]{
		OnAlarm: func(ctx *rivet.Context[reminderState]) error {
			if !ctx.State().Pending {
				return nil
			}
			ctx.State().Pending = false
			ctx.State().TriggeredAtMS = time.Now().UnixMilli()
			logger.Info("reminder triggered",
				slog.String("actor_id", ctx.ActorID()),
				slog.String("message", ctx.State().Message),
			)
			return nil
		},
		Actions: rivet.Actions[reminderState]{
			"schedule": rivet.Action(func(ctx *rivet.Context[reminderState], args scheduleArgs) (reminderState, error) {
				message, dueAt, err := reminderSchedule(args, time.Now())
				if err != nil {
					return reminderState{}, err
				}
				if err := ctx.Schedule(dueAt); err != nil {
					return reminderState{}, err
				}
				ctx.State().Message = message
				ctx.State().Pending = true
				ctx.State().DueAtMS = dueAt.UnixMilli()
				ctx.State().TriggeredAtMS = 0
				return *ctx.State(), nil
			}),
			"cancel": rivet.Action(func(ctx *rivet.Context[reminderState], _ struct{}) (reminderState, error) {
				if err := ctx.ClearSchedule(); err != nil {
					return reminderState{}, err
				}
				ctx.State().Pending = false
				ctx.State().DueAtMS = 0
				return *ctx.State(), nil
			}),
			"status": rivet.Action(func(ctx *rivet.Context[reminderState], _ struct{}) (reminderState, error) {
				return *ctx.State(), nil
			}),
			"sleep": rivet.Action(func(ctx *rivet.Context[reminderState], _ struct{}) (bool, error) {
				return true, ctx.Sleep()
			}),
		},
	})
}

func reminderSchedule(args scheduleArgs, now time.Time) (string, time.Time, error) {
	message := strings.TrimSpace(args.Message)
	if message == "" {
		return "", time.Time{}, rivet.ActionError{Code: "message_required", Message: "message must not be empty"}
	}
	if args.DelayMilliseconds <= 0 {
		return "", time.Time{}, rivet.ActionError{Code: "invalid_delay", Message: "delayMilliseconds must be positive"}
	}
	if args.DelayMilliseconds > maxReminderDelay.Milliseconds() {
		return "", time.Time{}, rivet.ActionError{Code: "invalid_delay", Message: "delayMilliseconds must not exceed seven days"}
	}
	delay := time.Duration(args.DelayMilliseconds) * time.Millisecond
	return message, now.Add(delay), nil
}
