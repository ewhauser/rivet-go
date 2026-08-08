package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

type employee struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Role      string `json:"role"`
	CreatedAt int64  `json:"createdAt"`
}

type projectStatus string

const (
	projectPlanning projectStatus = "planning"
	projectActive   projectStatus = "active"
	projectDone     projectStatus = "done"
)

type project struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Status    projectStatus `json:"status"`
	CreatedAt int64         `json:"createdAt"`
}

type companyDatabaseState struct {
	CompanyName string     `json:"companyName"`
	Employees   []employee `json:"employees"`
	Projects    []project  `json:"projects"`
	CreatedAt   int64      `json:"createdAt"`
	UpdatedAt   int64      `json:"updatedAt"`
}

type companyStats struct {
	EmployeeCount int   `json:"employeeCount"`
	ProjectCount  int   `json:"projectCount"`
	CreatedAt     int64 `json:"createdAt"`
	UpdatedAt     int64 `json:"updatedAt"`
}

type companyInfo struct {
	ActorID     string `json:"actorId"`
	ActorName   string `json:"actorName"`
	ActorKey    string `json:"actorKey"`
	CompanyName string `json:"companyName"`
}

type addEmployeeArgs struct {
	Name string `json:"name"`
	Role string `json:"role"`
}

type addProjectArgs struct {
	Name   string        `json:"name"`
	Status projectStatus `json:"status"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "per-tenant-database:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "per-tenant-database-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerCompanyDatabase(registry); err != nil {
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

func registerCompanyDatabase(registry *rivet.Registry) error {
	return rivet.Register(registry, "company-database", rivet.Actor[companyDatabaseState]{
		OnStart: func(ctx *rivet.Context[companyDatabaseState]) error {
			if !initializeCompanyState(ctx.State(), ctx.Key(), time.Now()) {
				return nil
			}
			saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return ctx.Save(saveCtx)
		},
		Actions: rivet.Actions[companyDatabaseState]{
			"getCompany": rivet.Action(func(ctx *rivet.Context[companyDatabaseState], _ struct{}) (companyInfo, error) {
				return companyInfo{
					ActorID:     ctx.ActorID(),
					ActorName:   ctx.Name(),
					ActorKey:    ctx.Key(),
					CompanyName: ctx.State().CompanyName,
				}, nil
			}),
			"addEmployee": rivet.Action(func(ctx *rivet.Context[companyDatabaseState], args addEmployeeArgs) (employee, error) {
				id, err := newEntityID("emp")
				if err != nil {
					return employee{}, err
				}
				name := strings.TrimSpace(args.Name)
				if name == "" {
					name = "New Employee"
				}
				role := strings.TrimSpace(args.Role)
				if role == "" {
					role = "Generalist"
				}
				now := time.Now().UnixMilli()
				result := employee{ID: id, Name: name, Role: role, CreatedAt: now}
				ctx.State().Employees = append(ctx.State().Employees, result)
				ctx.State().UpdatedAt = now
				if err := ctx.Broadcast("employeeAdded", result); err != nil {
					return employee{}, err
				}
				return result, nil
			}),
			"listEmployees": rivet.Action(func(ctx *rivet.Context[companyDatabaseState], _ struct{}) ([]employee, error) {
				return append([]employee(nil), ctx.State().Employees...), nil
			}),
			"addProject": rivet.Action(func(ctx *rivet.Context[companyDatabaseState], args addProjectArgs) (project, error) {
				if !validProjectStatus(args.Status) {
					return project{}, rivet.ActionError{
						Code:    "invalid_project_status",
						Message: "status must be planning, active, or done",
					}
				}
				id, err := newEntityID("proj")
				if err != nil {
					return project{}, err
				}
				name := strings.TrimSpace(args.Name)
				if name == "" {
					name = "New Project"
				}
				now := time.Now().UnixMilli()
				result := project{ID: id, Name: name, Status: args.Status, CreatedAt: now}
				ctx.State().Projects = append(ctx.State().Projects, result)
				ctx.State().UpdatedAt = now
				if err := ctx.Broadcast("projectAdded", result); err != nil {
					return project{}, err
				}
				return result, nil
			}),
			"listProjects": rivet.Action(func(ctx *rivet.Context[companyDatabaseState], _ struct{}) ([]project, error) {
				return append([]project(nil), ctx.State().Projects...), nil
			}),
			"getStats": rivet.Action(func(ctx *rivet.Context[companyDatabaseState], _ struct{}) (companyStats, error) {
				return companyStats{
					EmployeeCount: len(ctx.State().Employees),
					ProjectCount:  len(ctx.State().Projects),
					CreatedAt:     ctx.State().CreatedAt,
					UpdatedAt:     ctx.State().UpdatedAt,
				}, nil
			}),
		},
	})
}

func initializeCompanyState(state *companyDatabaseState, actorKey string, now time.Time) bool {
	if state == nil || state.CreatedAt != 0 {
		return false
	}
	timestamp := now.UnixMilli()
	*state = companyDatabaseState{
		CompanyName: companyName(actorKey),
		Employees:   make([]employee, 0),
		Projects:    make([]project, 0),
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
	}
	return true
}

func companyName(actorKey string) string {
	if name := strings.TrimSpace(actorKey); name != "" {
		return name
	}
	return "Unknown Company"
}

func validProjectStatus(status projectStatus) bool {
	return status == projectPlanning || status == projectActive || status == projectDone
}

func newEntityID(prefix string) (string, error) {
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate %s ID: %w", prefix, err)
	}
	return fmt.Sprintf("%s_%x", prefix, suffix), nil
}
