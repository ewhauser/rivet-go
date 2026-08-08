package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

const todoSchema = `CREATE TABLE IF NOT EXISTS todos (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	title TEXT NOT NULL,
	completed INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL
)`

type todoState struct{}

type todo struct {
	ID        int64  `json:"id"`
	Title     string `json:"title"`
	Completed bool   `json:"completed"`
	CreatedAt int64  `json:"createdAt"`
}

type addTodoArgs struct {
	Title string `json:"title"`
}

type todoIDArgs struct {
	ID int64 `json:"id"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "todo-sqlite:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "todo-sqlite-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerTodoList(registry); err != nil {
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

func registerTodoList(registry *rivet.Registry) error {
	return rivet.Register(registry, "todo-list", rivet.Actor[todoState]{
		Database: true,
		OnStart: func(ctx *rivet.Context[todoState]) error {
			migrationCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := ctx.DB().Exec(migrationCtx, todoSchema)
			return err
		},
		Actions: rivet.Actions[todoState]{
			"add": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[todoState], args addTodoArgs) (todo, error) {
				title := strings.TrimSpace(args.Title)
				if title == "" {
					return todo{}, rivet.ActionError{Code: "title_required", Message: "title must not be empty"}
				}
				createdAt := time.Now().UnixMilli()
				result, err := ctx.DB().Exec(
					actionCtx,
					"INSERT INTO todos (title, completed, created_at) VALUES (?, ?, ?)",
					title,
					int64(0),
					createdAt,
				)
				if err != nil {
					return todo{}, err
				}
				return todo{ID: result.LastInsertID, Title: title, CreatedAt: createdAt}, nil
			}),
			"list": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[todoState], _ struct{}) ([]todo, error) {
				rows, err := ctx.DB().Query(
					actionCtx,
					"SELECT id, title, completed, created_at FROM todos ORDER BY id",
				)
				if err != nil {
					return nil, err
				}
				return decodeTodos(rows)
			}),
			"toggle": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[todoState], args todoIDArgs) (todo, error) {
				tx, err := ctx.DB().Begin(actionCtx)
				if err != nil {
					return todo{}, err
				}
				committed := false
				defer func() {
					if !committed {
						rollbackTodoTransaction(tx)
					}
				}()

				result, err := tx.Exec(
					actionCtx,
					"UPDATE todos SET completed = NOT completed WHERE id = ?",
					args.ID,
				)
				if err != nil {
					return todo{}, err
				}
				if result.RowsAffected == 0 {
					return todo{}, todoNotFound(args.ID)
				}
				updated, err := queryTodo(actionCtx, tx, args.ID)
				if err != nil {
					return todo{}, err
				}
				if err := tx.Commit(actionCtx); err != nil {
					return todo{}, err
				}
				committed = true
				return updated, nil
			}),
			"delete": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[todoState], args todoIDArgs) (bool, error) {
				result, err := ctx.DB().Exec(actionCtx, "DELETE FROM todos WHERE id = ?", args.ID)
				if err != nil {
					return false, err
				}
				if result.RowsAffected == 0 {
					return false, todoNotFound(args.ID)
				}
				return true, nil
			}),
		},
	})
}

type todoQuerier interface {
	Query(context.Context, string, ...any) (rivet.Rows, error)
}

func queryTodo(ctx context.Context, database todoQuerier, id int64) (todo, error) {
	rows, err := database.Query(
		ctx,
		"SELECT id, title, completed, created_at FROM todos WHERE id = ?",
		id,
	)
	if err != nil {
		return todo{}, err
	}
	todos, err := decodeTodos(rows)
	if err != nil {
		return todo{}, err
	}
	if len(todos) == 0 {
		return todo{}, todoNotFound(id)
	}
	return todos[0], nil
}

func decodeTodos(rows rivet.Rows) ([]todo, error) {
	decoded := make([]todo, 0, len(rows.Values))
	for index, values := range rows.Values {
		if len(values) != 4 {
			return nil, fmt.Errorf("todo row %d has %d columns, want 4", index, len(values))
		}
		id, idOK := values[0].(int64)
		title, titleOK := values[1].(string)
		completed, completedOK := values[2].(int64)
		createdAt, createdAtOK := values[3].(int64)
		if !idOK || !titleOK || !completedOK || !createdAtOK {
			return nil, fmt.Errorf("todo row %d has unexpected SQLite value types", index)
		}
		decoded = append(decoded, todo{
			ID:        id,
			Title:     title,
			Completed: completed != 0,
			CreatedAt: createdAt,
		})
	}
	return decoded, nil
}

func rollbackTodoTransaction(tx rivet.Tx) {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}

func todoNotFound(id int64) error {
	return rivet.ActionError{
		Code:    "todo_not_found",
		Message: fmt.Sprintf("todo %d was not found", id),
	}
}
