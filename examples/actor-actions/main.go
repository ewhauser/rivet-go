package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

type employeeInput struct {
	EmployeeID string `json:"employeeId"`
	Name       string `json:"name"`
	Email      string `json:"email"`
	Position   string `json:"position"`
	CompanyID  string `json:"companyId"`
}

type employeeProfile struct {
	EmployeeID string `json:"employeeId"`
	Name       string `json:"name"`
	Email      string `json:"email"`
	Position   string `json:"position"`
	CompanyID  string `json:"companyId"`
	HiredAt    int64  `json:"hiredAt"`
}

type employeeState struct {
	Profile employeeProfile `json:"profile"`
}

type employeeUpdate struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Position string `json:"position"`
}

type companyInput struct {
	Name     string `json:"name"`
	Industry string `json:"industry"`
}

type companyProfile struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Industry  string `json:"industry"`
	FoundedAt int64  `json:"foundedAt"`
}

type companyState struct {
	Profile        companyProfile `json:"profile"`
	EmployeeEmails []string       `json:"employeeEmails"`
}

type createEmployeeArgs struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Position string `json:"position"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "actor-actions:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName, token string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "actor-actions-example", "engine-visible runner name")
	flag.StringVar(&token, "token", "dev", "Rivet Engine bearer token for actor-to-actor calls")
	flag.Parse()

	registry := rivet.NewRegistry()
	if err := registerActorActions(registry); err != nil {
		return err
	}
	return rivet.Serve(registry, rivet.Config{
		Endpoint:   endpoint,
		RunnerName: runnerName,
		Token:      token,
		TotalSlots: 16,
		LogLevel:   "info",
		Logger:     slog.New(slog.NewJSONHandler(os.Stderr, nil)),
	})
}

func registerActorActions(registry *rivet.Registry) error {
	if err := rivet.Register(registry, "employee", rivet.Actor[employeeState]{
		OnStart: func(ctx *rivet.Context[employeeState]) error {
			if ctx.State().Profile.EmployeeID != "" {
				return nil
			}
			var input employeeInput
			if err := decodeCreationInput(ctx.Input(), &input); err != nil {
				return fmt.Errorf("decode employee input: %w", err)
			}
			if input.EmployeeID == "" || input.Email == "" {
				return errors.New("employeeId and email are required")
			}
			ctx.State().Profile = employeeProfile{
				EmployeeID: input.EmployeeID,
				Name:       input.Name,
				Email:      input.Email,
				Position:   input.Position,
				CompanyID:  input.CompanyID,
				HiredAt:    time.Now().UnixMilli(),
			}
			return nil
		},
		Actions: rivet.Actions[employeeState]{
			"getProfile": rivet.Action(func(ctx *rivet.Context[employeeState], _ struct{}) (employeeProfile, error) {
				return ctx.State().Profile, nil
			}),
			"updateProfile": rivet.Action(func(ctx *rivet.Context[employeeState], update employeeUpdate) (employeeProfile, error) {
				if update.Name != "" {
					ctx.State().Profile.Name = update.Name
				}
				if update.Email != "" {
					ctx.State().Profile.Email = update.Email
				}
				if update.Position != "" {
					ctx.State().Profile.Position = update.Position
				}
				return ctx.State().Profile, nil
			}),
		},
	}); err != nil {
		return err
	}

	return rivet.Register(registry, "company", rivet.Actor[companyState]{
		OnStart: func(ctx *rivet.Context[companyState]) error {
			if ctx.State().Profile.ID != "" {
				return nil
			}
			var input companyInput
			if err := decodeCreationInput(ctx.Input(), &input); err != nil {
				return fmt.Errorf("decode company input: %w", err)
			}
			if strings.TrimSpace(input.Name) == "" {
				return errors.New("company name is required")
			}
			id, err := randomID()
			if err != nil {
				return err
			}
			ctx.State().Profile = companyProfile{
				ID:        id,
				Name:      input.Name,
				Industry:  input.Industry,
				FoundedAt: time.Now().UnixMilli(),
			}
			ctx.State().EmployeeEmails = []string{}
			return nil
		},
		Actions: rivet.Actions[companyState]{
			"getProfile": rivet.Action(func(ctx *rivet.Context[companyState], _ struct{}) (companyProfile, error) {
				return ctx.State().Profile, nil
			}),
			"updateProfile": rivet.Action(func(ctx *rivet.Context[companyState], update companyInput) (companyProfile, error) {
				if update.Name != "" {
					ctx.State().Profile.Name = update.Name
				}
				if update.Industry != "" {
					ctx.State().Profile.Industry = update.Industry
				}
				return ctx.State().Profile, nil
			}),
			"createEmployee": rivet.ActionWithContext(func(actionContext context.Context, ctx *rivet.Context[companyState], args createEmployeeArgs) (employeeProfile, error) {
				if strings.TrimSpace(args.Email) == "" {
					return employeeProfile{}, rivet.ActionError{Code: "email_required", Message: "employee email is required"}
				}
				employeeID, err := randomID()
				if err != nil {
					return employeeProfile{}, err
				}
				input := employeeInput{
					EmployeeID: employeeID,
					Name:       args.Name,
					Email:      args.Email,
					Position:   args.Position,
					CompanyID:  ctx.State().Profile.ID,
				}
				encoded, err := json.Marshal(input)
				if err != nil {
					return employeeProfile{}, err
				}
				if _, err := ctx.Client().Create(actionContext, "employee", rivet.CreateOptions{
					Key:   []string{args.Email},
					Input: encoded,
				}); err != nil {
					return employeeProfile{}, fmt.Errorf("create employee actor: %w", err)
				}
				ctx.State().EmployeeEmails = append(ctx.State().EmployeeEmails, args.Email)
				return employeeProfile{
					EmployeeID: employeeID,
					Name:       args.Name,
					Email:      args.Email,
					Position:   args.Position,
					CompanyID:  ctx.State().Profile.ID,
					HiredAt:    time.Now().UnixMilli(),
				}, nil
			}),
			"getEmployees": rivet.Action(func(ctx *rivet.Context[companyState], _ struct{}) ([]string, error) {
				return append([]string(nil), ctx.State().EmployeeEmails...), nil
			}),
		},
	})
}

func decodeCreationInput(input []byte, output any) error {
	if len(input) == 0 {
		return errors.New("creation input is required")
	}
	if err := json.Unmarshal(input, output); err != nil {
		return err
	}
	return nil
}

func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
