package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

const maxScheduleDelay = 7 * 24 * time.Hour

type reminder struct {
	ID          string `json:"id"`
	ScheduleID  string `json:"scheduleId"`
	Message     string `json:"message"`
	ScheduledAt int64  `json:"scheduledAt"`
	CompletedAt int64  `json:"completedAt,omitempty"`
}

type reminderState struct {
	Reminders      []reminder `json:"reminders"`
	CompletedCount int        `json:"completedCount"`
}

type scheduleRequest struct {
	Message           string `json:"message"`
	DelayMilliseconds int64  `json:"delayMilliseconds"`
}

type scheduleAtRequest struct {
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

type reminderID struct {
	ID string `json:"id"`
}

type cancelResult struct {
	Success bool `json:"success"`
}

type scheduleInfo struct {
	ID     string `json:"id"`
	Action string `json:"action"`
	RunAt  int64  `json:"runAt"`
}

type reminderStats struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Pending   int `json:"pending"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "scheduling:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "scheduling-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerScheduling(registry, logger); err != nil {
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

func registerScheduling(registry *rivet.Registry, logger *slog.Logger) error {
	return rivet.Register(registry, "scheduled-reminders", rivet.Actor[reminderState]{
		Actions: rivet.Actions[reminderState]{
			"scheduleReminder": rivet.ActionWithContext(func(
				ctx context.Context,
				actor *rivet.Context[reminderState],
				request scheduleRequest,
			) (reminder, error) {
				message, delay, err := validateScheduleRequest(request)
				if err != nil {
					return reminder{}, err
				}
				id, err := newReminderID()
				if err != nil {
					return reminder{}, err
				}
				runAt := time.Now().Add(delay)
				scheduleID, err := actor.Schedules().After(ctx, delay, "triggerReminder", reminderID{ID: id})
				if err != nil {
					return reminder{}, err
				}
				created := reminder{
					ID: id, ScheduleID: scheduleID, Message: message,
					ScheduledAt: runAt.UnixMilli(),
				}
				actor.State().Reminders = append(actor.State().Reminders, created)
				return created, nil
			}),
			"scheduleReminderAt": rivet.ActionWithContext(func(
				ctx context.Context,
				actor *rivet.Context[reminderState],
				request scheduleAtRequest,
			) (reminder, error) {
				message := strings.TrimSpace(request.Message)
				if message == "" {
					return reminder{}, rivet.ActionError{Code: "message_required", Message: "message must not be empty"}
				}
				id, err := newReminderID()
				if err != nil {
					return reminder{}, err
				}
				scheduleID, err := actor.Schedules().At(
					ctx, time.UnixMilli(request.Timestamp), "triggerReminder", reminderID{ID: id},
				)
				if err != nil {
					return reminder{}, err
				}
				created := reminder{
					ID: id, ScheduleID: scheduleID, Message: message, ScheduledAt: request.Timestamp,
				}
				actor.State().Reminders = append(actor.State().Reminders, created)
				return created, nil
			}),
			"triggerReminder": rivet.Action(func(
				actor *rivet.Context[reminderState],
				input reminderID,
			) (bool, error) {
				for index := range actor.State().Reminders {
					item := &actor.State().Reminders[index]
					if item.ID != input.ID {
						continue
					}
					item.CompletedAt = time.Now().UnixMilli()
					actor.State().CompletedCount++
					if err := actor.Broadcast("reminderTriggered", *item); err != nil {
						return false, err
					}
					logger.Info("reminder triggered",
						slog.String("actor_id", actor.ActorID()),
						slog.String("message", item.Message),
					)
					return true, nil
				}
				return false, rivet.ActionError{Code: "reminder_not_found", Message: "reminder was not found"}
			}),
			"cancelReminder": rivet.ActionWithContext(func(
				ctx context.Context,
				actor *rivet.Context[reminderState],
				input reminderID,
			) (cancelResult, error) {
				for index, item := range actor.State().Reminders {
					if item.ID != input.ID || item.CompletedAt != 0 {
						continue
					}
					cancelled, err := actor.Schedules().Cancel(ctx, item.ScheduleID)
					if err != nil || !cancelled {
						return cancelResult{Success: cancelled}, err
					}
					actor.State().Reminders = append(
						actor.State().Reminders[:index], actor.State().Reminders[index+1:]...,
					)
					return cancelResult{Success: true}, nil
				}
				return cancelResult{}, nil
			}),
			"getReminders": rivet.Action(func(actor *rivet.Context[reminderState], _ struct{}) ([]reminder, error) {
				return append([]reminder(nil), actor.State().Reminders...), nil
			}),
			"getPendingSchedules": rivet.ActionWithContext(func(
				ctx context.Context,
				actor *rivet.Context[reminderState],
				_ struct{},
			) ([]scheduleInfo, error) {
				schedules, err := actor.Schedules().List(ctx)
				if err != nil {
					return nil, err
				}
				result := make([]scheduleInfo, len(schedules))
				for index, schedule := range schedules {
					result[index] = scheduleInfo{
						ID: schedule.ID, Action: schedule.Action, RunAt: schedule.RunAt.UnixMilli(),
					}
				}
				return result, nil
			}),
			"getStats": rivet.Action(func(actor *rivet.Context[reminderState], _ struct{}) (reminderStats, error) {
				total := len(actor.State().Reminders)
				return reminderStats{
					Total: total, Completed: actor.State().CompletedCount,
					Pending: total - actor.State().CompletedCount,
				}, nil
			}),
			"sleep": rivet.Action(func(actor *rivet.Context[reminderState], _ struct{}) (bool, error) {
				return true, actor.Sleep()
			}),
		},
	})
}

func validateScheduleRequest(request scheduleRequest) (string, time.Duration, error) {
	message := strings.TrimSpace(request.Message)
	if message == "" {
		return "", 0, rivet.ActionError{Code: "message_required", Message: "message must not be empty"}
	}
	if request.DelayMilliseconds <= 0 || request.DelayMilliseconds > maxScheduleDelay.Milliseconds() {
		return "", 0, rivet.ActionError{Code: "invalid_delay", Message: "delayMilliseconds must be between 1 ms and seven days"}
	}
	return message, time.Duration(request.DelayMilliseconds) * time.Millisecond, nil
}

func newReminderID() (string, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate reminder ID: %w", err)
	}
	return "reminder-" + hex.EncodeToString(value[:]), nil
}
