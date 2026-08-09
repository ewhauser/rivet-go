package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

const promptQueue = "prompts"

type message struct {
	ID        string `json:"id"`
	RequestID string `json:"requestId"`
	Role      string `json:"role"`
	Content   string `json:"content"`
	CreatedAt int64  `json:"createdAt"`
}

type agentState struct {
	Messages []message               `json:"messages"`
	Replies  map[string]promptResult `json:"replies"`
}

type promptRequest struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

type promptResult struct {
	RequestID string `json:"requestId"`
	Content   string `json:"content"`
}

type promptFailure struct {
	Error string `json:"error"`
}

// Provider deliberately contains no Rivet types. Production applications can
// adapt any model SDK here and unit test the actor with a deterministic fake.
type Provider interface {
	Complete(context.Context, []message) (string, error)
}

type echoProvider struct{}

func (echoProvider) Complete(_ context.Context, history []message) (string, error) {
	for index := len(history) - 1; index >= 0; index-- {
		if history[index].Role == "user" {
			return "Echo: " + history[index].Content, nil
		}
	}
	return "", errors.New("conversation has no user message")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ai-agent:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "ai-agent-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerAIAgent(registry, echoProvider{}, logger); err != nil {
		return err
	}
	return rivet.Serve(registry, rivet.Config{
		Endpoint: endpoint, RunnerName: runnerName, TotalSlots: 16,
		LogLevel: "info", Logger: logger,
	})
}

func registerAIAgent(registry *rivet.Registry, provider Provider, logger *slog.Logger) error {
	if provider == nil {
		return errors.New("AI provider is nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return rivet.Register(registry, "ai-agent", rivet.Actor[agentState]{
		OnStart: func(actor *rivet.Context[agentState]) error {
			if actor.State().Replies == nil {
				actor.State().Replies = make(map[string]promptResult)
			}
			return nil
		},
		Run: func(ctx context.Context, actor *rivet.RunContext[agentState]) error {
			return runPrompts(ctx, actor, provider, logger)
		},
		Actions: rivet.Actions[agentState]{
			"sendMessage": rivet.ActionWithContext(func(
				ctx context.Context,
				actor *rivet.Context[agentState],
				request promptRequest,
			) (promptRequest, error) {
				request.Content = strings.TrimSpace(request.Content)
				if request.Content == "" {
					return promptRequest{}, rivet.ActionError{Code: "content_required", Message: "content is required"}
				}
				if request.ID == "" {
					var err error
					request.ID, err = newPromptID()
					if err != nil {
						return promptRequest{}, err
					}
				}
				if _, err := actor.Queue().Send(ctx, promptQueue, request); err != nil {
					return promptRequest{}, err
				}
				return request, nil
			}),
			"getMessages": rivet.Action(func(actor *rivet.Context[agentState], _ struct{}) ([]message, error) {
				return append([]message(nil), actor.State().Messages...), nil
			}),
			"sleep": rivet.Action(func(actor *rivet.Context[agentState], _ struct{}) (bool, error) {
				return true, actor.Sleep()
			}),
		},
	})
}

func runPrompts(
	ctx context.Context,
	actor *rivet.RunContext[agentState],
	provider Provider,
	logger *slog.Logger,
) error {
	for {
		queued, err := actor.Queue().Next(ctx, rivet.QueueNextOptions{
			Names: []string{promptQueue}, Completable: true,
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, rivet.ErrActorAborted) {
				return nil
			}
			return err
		}
		var request promptRequest
		if err := queued.DecodeBody(&request); err != nil {
			logger.Warn("discarding malformed prompt", slog.Any("error", err))
			if completeErr := queued.Complete(ctx, promptFailure{Error: "invalid prompt request"}); completeErr != nil {
				return completeErr
			}
			continue
		}
		if reply, exists := actor.State().Replies[request.ID]; exists {
			if err := queued.Complete(ctx, reply); err != nil {
				return err
			}
			continue
		}
		if !hasUserMessage(actor.State().Messages, request.ID) {
			actor.State().Messages = append(actor.State().Messages, message{
				ID: request.ID + "-user", RequestID: request.ID, Role: "user",
				Content: request.Content, CreatedAt: time.Now().UnixMilli(),
			})
			if err := actor.Save(ctx); err != nil {
				return err
			}
		}

		history := append([]message(nil), actor.State().Messages...)
		var content string
		err = actor.KeepAwake(ctx, func(workCtx context.Context) error {
			var providerErr error
			content, providerErr = provider.Complete(workCtx, history)
			return providerErr
		})
		if err != nil {
			logger.Error("provider completion failed", slog.String("request_id", request.ID), slog.Any("error", err))
			if retryErr := queued.Retry(ctx); retryErr != nil {
				return retryErr
			}
			if waitErr := actor.KeepAwake(ctx, func(workCtx context.Context) error {
				timer := time.NewTimer(time.Second)
				defer timer.Stop()
				select {
				case <-timer.C:
					return nil
				case <-workCtx.Done():
					return workCtx.Err()
				}
			}); waitErr != nil && !errors.Is(waitErr, context.Canceled) {
				return waitErr
			}
			continue
		}

		reply := promptResult{RequestID: request.ID, Content: content}
		actor.State().Messages = append(actor.State().Messages, message{
			ID: request.ID + "-assistant", RequestID: request.ID, Role: "assistant",
			Content: content, CreatedAt: time.Now().UnixMilli(),
		})
		actor.State().Replies[request.ID] = reply
		if err := actor.Save(ctx); err != nil {
			return err
		}
		if err := actor.Broadcast("messageAdded", actor.State().Messages[len(actor.State().Messages)-1]); err != nil {
			logger.Warn("broadcast assistant message", slog.String("request_id", request.ID), slog.Any("error", err))
		}
		if err := queued.Complete(ctx, reply); err != nil {
			return err
		}
	}
}

func hasUserMessage(messages []message, requestID string) bool {
	for _, item := range messages {
		if item.RequestID == requestID && item.Role == "user" {
			return true
		}
	}
	return false
}

func newPromptID() (string, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate prompt ID: %w", err)
	}
	return "prompt-" + hex.EncodeToString(random[:]), nil
}
