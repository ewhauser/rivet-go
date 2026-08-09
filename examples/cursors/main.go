package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

type cursorPosition struct {
	UserID    string  `json:"userId"`
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
	Timestamp int64   `json:"timestamp"`
}

type textLabel struct {
	ID        string  `json:"id"`
	UserID    string  `json:"userId"`
	Text      string  `json:"text"`
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
	Timestamp int64   `json:"timestamp"`
}

type cursorRoomState struct {
	TextLabels []textLabel `json:"textLabels"`
}

type cursorConnectionState struct {
	Cursor *cursorPosition `json:"cursor"`
}

type updateCursorArgs struct {
	UserID string  `json:"userId"`
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
}

type updateTextArgs struct {
	ID     string  `json:"id"`
	UserID string  `json:"userId"`
	Text   string  `json:"text"`
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
}

type removeTextArgs struct {
	ID string `json:"id"`
}

type roomState struct {
	Cursors    map[string]cursorPosition `json:"cursors"`
	TextLabels []textLabel               `json:"textLabels"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "cursors:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "cursors-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerCursorRoom(registry, logger); err != nil {
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

func registerCursorRoom(registry *rivet.Registry, logger *slog.Logger) error {
	return rivet.Register(registry, "cursor-room", rivet.Actor[cursorRoomState]{
		HibernateWebSockets: true,
		ConnectionState: rivet.NewConnectionState(func(
			*rivet.Context[cursorRoomState], *rivet.Connection,
		) (cursorConnectionState, error) {
			return cursorConnectionState{}, nil
		}),
		Actions: rivet.Actions[cursorRoomState]{
			"updateCursor": rivet.Action(func(ctx *rivet.Context[cursorRoomState], args updateCursorArgs) (cursorPosition, error) {
				state, err := currentCursorState(ctx)
				if err != nil {
					return cursorPosition{}, err
				}
				cursor := cursorPosition{
					UserID: args.UserID, X: args.X, Y: args.Y, Timestamp: time.Now().UnixMilli(),
				}
				state.Cursor = &cursor
				if err := ctx.Broadcast("cursorMoved", cursor); err != nil {
					return cursorPosition{}, err
				}
				return cursor, nil
			}),
			"updateText": rivet.Action(func(ctx *rivet.Context[cursorRoomState], args updateTextArgs) (textLabel, error) {
				label := textLabel{
					ID: args.ID, UserID: args.UserID, Text: args.Text,
					X: args.X, Y: args.Y, Timestamp: time.Now().UnixMilli(),
				}
				labels := &ctx.State().TextLabels
				updated := false
				for index := range *labels {
					if (*labels)[index].ID == label.ID {
						(*labels)[index] = label
						updated = true
						break
					}
				}
				if !updated {
					*labels = append(*labels, label)
				}
				if err := ctx.Broadcast("textUpdated", label); err != nil {
					return textLabel{}, err
				}
				return label, nil
			}),
			"removeText": rivet.Action(func(ctx *rivet.Context[cursorRoomState], args removeTextArgs) (bool, error) {
				labels := ctx.State().TextLabels
				filtered := labels[:0]
				removed := false
				for _, label := range labels {
					if label.ID == args.ID {
						removed = true
						continue
					}
					filtered = append(filtered, label)
				}
				ctx.State().TextLabels = filtered
				if err := ctx.Broadcast("textRemoved", args.ID); err != nil {
					return false, err
				}
				return removed, nil
			}),
			"getRoomState": rivet.Action(func(ctx *rivet.Context[cursorRoomState], _ struct{}) (roomState, error) {
				cursors := make(map[string]cursorPosition)
				for _, connection := range ctx.Connections() {
					state, err := rivet.GetConnectionState[cursorConnectionState](connection)
					if err != nil {
						return roomState{}, err
					}
					if state.Cursor != nil {
						cursors[state.Cursor.UserID] = *state.Cursor
					}
				}
				return roomState{
					Cursors: cursors, TextLabels: append([]textLabel(nil), ctx.State().TextLabels...),
				}, nil
			}),
		},
		OnActorDisconnect: func(ctx *rivet.Context[cursorRoomState], connection *rivet.Connection) {
			state, err := rivet.GetConnectionState[cursorConnectionState](connection)
			if err != nil {
				logger.Error("read disconnected cursor state", slog.Any("error", err))
				return
			}
			if state.Cursor != nil {
				if err := ctx.Broadcast("cursorRemoved", *state.Cursor); err != nil {
					logger.Error("broadcast removed cursor", slog.Any("error", err))
				}
			}
		},
	})
}

func currentCursorState(ctx *rivet.Context[cursorRoomState]) (*cursorConnectionState, error) {
	connection := ctx.CurrentConnection()
	if connection == nil {
		return nil, rivet.ActionError{
			Code: "connection_required", Message: "updateCursor must be called through ActorConnect",
		}
	}
	return rivet.GetConnectionState[cursorConnectionState](connection)
}
