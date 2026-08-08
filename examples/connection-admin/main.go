package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/ewhauser/rivet-go/rivet"
)

type connectionAdminState struct{}

type connectionSummary struct {
	ID           string `json:"id"`
	Label        string `json:"label"`
	Path         string `json:"path"`
	CanHibernate bool   `json:"canHibernate"`
}

type sendArgs struct {
	ConnectionID string `json:"connectionId"`
	Message      string `json:"message"`
}

type disconnectArgs struct {
	ConnectionID string `json:"connectionId"`
	Code         int    `json:"code"`
	Reason       string `json:"reason"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "connection-admin:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "connection-admin-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerConnectionAdmin(registry, logger); err != nil {
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

func registerConnectionAdmin(registry *rivet.Registry, logger *slog.Logger) error {
	return rivet.Register(registry, "connection-admin", rivet.Actor[connectionAdminState]{
		HibernateWebSockets: true,
		Actions: rivet.Actions[connectionAdminState]{
			"listConnections": rivet.Action(func(ctx *rivet.Context[connectionAdminState], _ struct{}) ([]connectionSummary, error) {
				connections := ctx.Connections()
				result := make([]connectionSummary, len(connections))
				for index, connection := range connections {
					result[index] = summarizeConnection(connection)
				}
				return result, nil
			}),
			"send": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[connectionAdminState], args sendArgs) (bool, error) {
				connection, err := findConnection(ctx.Connections(), args.ConnectionID)
				if err != nil {
					return false, err
				}
				if err := connection.SendContext(actionCtx, []byte(args.Message), false); err != nil {
					return false, err
				}
				return true, nil
			}),
			"disconnect": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[connectionAdminState], args disconnectArgs) (bool, error) {
				connection, err := findConnection(ctx.Connections(), args.ConnectionID)
				if err != nil {
					return false, err
				}
				code, reason, err := normalizeDisconnect(args.Code, args.Reason)
				if err != nil {
					return false, err
				}
				if err := connection.CloseContext(actionCtx, code, reason); err != nil {
					return false, err
				}
				return true, nil
			}),
		},
		OnConnect: func(_ *rivet.Context[connectionAdminState], connection *rivet.Connection) error {
			label := connectionLabel(connection.Headers(), connection.Path())
			if label == "" {
				return rivet.ActionError{Code: "label_required", Message: "set x-client-label or the client query parameter"}
			}
			logger.Info("raw WebSocket connected",
				slog.String("connection_id", connection.ID()),
				slog.String("label", label),
			)
			return connection.SendText("connected")
		},
		OnMessage: func(_ *rivet.Context[connectionAdminState], connection *rivet.Connection, message rivet.Message) {
			if message.Binary {
				_ = connection.Close(1003, "text messages only")
				return
			}
			if err := connection.SendText("echo:" + string(message.Data)); err != nil {
				logger.Error("echo raw WebSocket message", slog.Any("error", err))
			}
		},
		OnDisconnect: func(_ *rivet.Context[connectionAdminState], connection *rivet.Connection) {
			code, reason := connection.CloseInfo()
			logger.Info("raw WebSocket disconnected",
				slog.String("connection_id", connection.ID()),
				slog.Any("code", code),
				slog.String("reason", reason),
			)
		},
	})
}

func summarizeConnection(connection *rivet.Connection) connectionSummary {
	return connectionSummary{
		ID:           connection.ID(),
		Label:        connectionLabel(connection.Headers(), connection.Path()),
		Path:         connection.Path(),
		CanHibernate: connection.CanHibernate(),
	}
}

func findConnection(connections []*rivet.Connection, id string) (*rivet.Connection, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, rivet.ActionError{Code: "connection_id_required", Message: "connectionId must not be empty"}
	}
	for _, connection := range connections {
		if connection.ID() == id {
			return connection, nil
		}
	}
	return nil, rivet.ActionError{
		Code:    "connection_not_found",
		Message: fmt.Sprintf("connection %q is not live", id),
	}
}

func connectionLabel(headers map[string]string, path string) string {
	for name, value := range headers {
		if strings.EqualFold(name, "x-client-label") {
			if label := strings.TrimSpace(value); label != "" {
				return label
			}
		}
	}
	parsed, err := url.ParseRequestURI(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(parsed.Query().Get("client"))
}

func normalizeDisconnect(code int, reason string) (uint16, string, error) {
	if code == 0 {
		code = 4000
	}
	if code < 3000 || code > 4999 {
		return 0, "", rivet.ActionError{
			Code:    "invalid_close_code",
			Message: "code must be between 3000 and 4999",
		}
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "disconnected by actor"
	}
	if len([]byte(reason)) > 123 {
		return 0, "", rivet.ActionError{
			Code:    "invalid_close_reason",
			Message: "reason must not exceed 123 bytes",
		}
	}
	return uint16(code), reason, nil
}
