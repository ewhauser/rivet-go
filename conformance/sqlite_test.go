package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

type sqliteConformanceState struct {
	Saves int `json:"saves"`
}

type sqliteStartObservation struct {
	actorID    string
	generation uint64
	saves      int
}

type sqliteHTTPResult struct {
	Count              int64    `json:"count,omitempty"`
	RowsAffected       int64    `json:"rows_affected,omitempty"`
	LastInsertID       int64    `json:"last_insert_id,omitempty"`
	ValuesOK           bool     `json:"values_ok,omitempty"`
	CommitVisible      bool     `json:"commit_visible,omitempty"`
	RollbackInvisible  bool     `json:"rollback_invisible,omitempty"`
	LeaseErrorCode     string   `json:"lease_error_code,omitempty"`
	SyntaxErrorCode    string   `json:"syntax_error_code,omitempty"`
	ConstraintCode     string   `json:"constraint_code,omitempty"`
	SyntaxSQLiteCode   int32    `json:"syntax_sqlite_code,omitempty"`
	ConstraintSQLCode  int32    `json:"constraint_sqlite_code,omitempty"`
	ConcurrentFailures []string `json:"concurrent_failures,omitempty"`
	StateSaves         int      `json:"state_saves,omitempty"`
	LargeRows          int      `json:"large_rows,omitempty"`
}

func TestPerActorSQLiteConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine SQLite conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}

	for _, transport := range []rivet.SQLiteTransport{rivet.SQLiteTransportFFI, rivet.SQLiteTransportSocket} {
		t.Run(string(transport), func(t *testing.T) {
			engine := startEngine(t, engineBinary)
			started := make(chan sqliteStartObservation, 16)
			stopped := make(chan sqliteStartObservation, 16)
			oldLease := make(chan rivet.Tx, 1)

			registry := rivet.NewRegistry()
			definition := sqliteConformanceActor(started, stopped, oldLease)
			if err := rivet.Register(registry, "m7-sqlite", definition); err != nil {
				t.Fatalf("register SQLite actor: %v", err)
			}
			runnerName := fmt.Sprintf("rivet-go-m7-%s-%d", transport, time.Now().UnixNano())
			served := startRegistryWithConfig(t, engine, runnerName, registry, rivet.Config{SQLiteTransport: transport})

			first := createActor(t, engine.endpoint, "m7-sqlite", runnerName, "restart", nil, nil)
			firstStart := waitSQLiteStart(t, started, first.ActorID, 30*time.Second)
			second := createActor(t, engine.endpoint, "m7-sqlite", runnerName, "restart", nil, nil)
			_ = waitSQLiteStart(t, started, second.ActorID, 30*time.Second)

			crud := sqliteGateway(t, engine.endpoint, first.ActorID, "/crud", 30*time.Second)
			if !crud.ValuesOK || crud.RowsAffected != 1 || crud.LastInsertID <= 0 {
				t.Fatalf("CRUD/value round trip = %#v", crud)
			}

			transactions := sqliteGateway(t, engine.endpoint, first.ActorID, "/transactions", 30*time.Second)
			if !transactions.CommitVisible || !transactions.RollbackInvisible || transactions.LeaseErrorCode != "transaction_expired" {
				t.Fatalf("transaction conformance = %#v", transactions)
			}

			errorsResult := sqliteGateway(t, engine.endpoint, first.ActorID, "/errors", 30*time.Second)
			if errorsResult.SyntaxErrorCode != "sqlite_error" || errorsResult.ConstraintCode != "sqlite_error" || errorsResult.SyntaxSQLiteCode == 0 || errorsResult.ConstraintSQLCode == 0 {
				t.Fatalf("structured SQLite errors = %#v", errorsResult)
			}

			concurrent := sqliteGateway(t, engine.endpoint, first.ActorID, "/concurrent", 30*time.Second)
			if concurrent.Count != 16 || len(concurrent.ConcurrentFailures) != 0 {
				t.Fatalf("concurrent SQLite access = %#v", concurrent)
			}

			stateAndSQL := gatewayAction(t, engine.endpoint, first.ActorID, "state_and_sql", []any{struct{}{}}, 30*time.Second)
			assertActionOutput(t, stateAndSQL, http.StatusOK, 1)
			snapshot := sqliteGateway(t, engine.endpoint, first.ActorID, "/snapshot", 30*time.Second)
			if snapshot.StateSaves != 1 {
				t.Fatalf("SQL alongside actor state = %#v", snapshot)
			}

			_ = sqliteGateway(t, engine.endpoint, first.ActorID, "/isolation?label=first-only", 30*time.Second)
			_ = sqliteGateway(t, engine.endpoint, second.ActorID, "/isolation?label=second-only", 30*time.Second)
			firstIsolation := sqliteGateway(t, engine.endpoint, first.ActorID, "/isolation-count", 30*time.Second)
			secondIsolation := sqliteGateway(t, engine.endpoint, second.ActorID, "/isolation-count", 30*time.Second)
			if firstIsolation.Count != 1 || secondIsolation.Count != 1 {
				t.Fatalf("per-actor SQLite isolation: first=%#v second=%#v", firstIsolation, secondIsolation)
			}
			deleteActor(t, engine.endpoint, second.ActorID)

			if transport == rivet.SQLiteTransportFFI {
				large := sqliteGateway(t, engine.endpoint, first.ActorID, "/large", 45*time.Second)
				if large.LargeRows != 2_200 {
					t.Fatalf("chunked FFI result rows = %d, want 2200", large.LargeRows)
				}
			}

			beforeSleep := sqliteGateway(t, engine.endpoint, first.ActorID, "/count", 30*time.Second).Count
			_ = sqliteGateway(t, engine.endpoint, first.ActorID, "/sleep", 30*time.Second)
			waitForActor(t, engine.endpoint, first.ActorID, false, func(actor actorRecord) bool {
				return actor.SleepTS != nil && actor.ConnectableTS == nil
			})
			postSleep := sqliteGateway(t, engine.endpoint, first.ActorID, "/count", rehydrateWindow)
			if postSleep.Count != beforeSleep {
				t.Fatalf("SQLite count after sleep/wake = %d, want %d", postSleep.Count, beforeSleep)
			}
			secondStart := waitSQLiteStart(t, started, first.ActorID, rehydrateWindow)
			currentStart := secondStart
			if secondStart.generation <= firstStart.generation {
				t.Fatalf("wake generation = %d, want greater than %d", secondStart.generation, firstStart.generation)
			}

			if transport == rivet.SQLiteTransportSocket {
				_ = sqliteGateway(t, engine.endpoint, first.ActorID, "/lease-sleep", 30*time.Second)
				var lease rivet.Tx
				select {
				case lease = <-oldLease:
				case <-time.After(5 * time.Second):
					t.Fatal("socket lease was not published")
				}
				waitForActor(t, engine.endpoint, first.ActorID, false, func(actor actorRecord) bool {
					return actor.SleepTS != nil && actor.ConnectableTS == nil
				})
				_, leaseErr := lease.Exec(context.Background(), "INSERT INTO todos(label) VALUES ('dead-lease')")
				var structured *rivet.SQLiteError
				if !errors.As(leaseErr, &structured) || structured.Code == "" {
					t.Fatalf("old socket lease error = %T %v, want structured", leaseErr, leaseErr)
				}
				afterLeaseSleep := sqliteGateway(t, engine.endpoint, first.ActorID, "/label-count?label=dead-lease", rehydrateWindow)
				if afterLeaseSleep.Count != 0 {
					t.Fatalf("dead socket lease committed %d rows", afterLeaseSleep.Count)
				}
				currentStart = waitSQLiteStart(t, started, first.ActorID, rehydrateWindow)
			}

			beforeRestart := sqliteGateway(t, engine.endpoint, first.ActorID, "/count", 30*time.Second)
			_ = sqliteGateway(t, engine.endpoint, first.ActorID, "/sleep", 30*time.Second)
			waitForActor(t, engine.endpoint, first.ActorID, false, func(actor actorRecord) bool {
				return actor.SleepTS != nil && actor.ConnectableTS == nil
			})
			waitSQLiteStop(t, stopped, served, first.ActorID, currentStart.generation, 30*time.Second)
			served.stop(t)
			engine.kill(t)
			engine.start(t)
			restartedRegistry := rivet.NewRegistry()
			if err := rivet.Register(restartedRegistry, "m7-sqlite", definition); err != nil {
				t.Fatalf("register SQLite actor after engine restart: %v", err)
			}
			startRegistryWithConfig(t, engine, runnerName, restartedRegistry, rivet.Config{SQLiteTransport: transport})
			waitForActor(t, engine.endpoint, first.ActorID, false, func(actor actorRecord) bool {
				return actor.SleepTS != nil && actor.ConnectableTS == nil
			})
			postRestart := sqliteGateway(t, engine.endpoint, first.ActorID, "/snapshot", rehydrateWindow)
			restarted := waitSQLiteStart(t, started, first.ActorID, rehydrateWindow)
			if postRestart.Count != beforeRestart.Count || postRestart.StateSaves != 1 {
				t.Fatalf("SQLite/state after engine restart = %#v, before=%#v", postRestart, beforeRestart)
			}
			if restarted.generation <= currentStart.generation {
				t.Fatalf("post-engine-restart generation = %d did not advance", restarted.generation)
			}

			deleteActor(t, engine.endpoint, first.ActorID)
		})
	}
}

func sqliteConformanceActor(
	started chan<- sqliteStartObservation,
	stopped chan<- sqliteStartObservation,
	oldLease chan<- rivet.Tx,
) rivet.Actor[sqliteConformanceState] {
	return rivet.Actor[sqliteConformanceState]{
		OnStart: func(ctx *rivet.Context[sqliteConformanceState]) error {
			opCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for _, statement := range []string{`
				CREATE TABLE IF NOT EXISTS todos (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					label TEXT NOT NULL UNIQUE,
					i INTEGER,
					r REAL,
					t TEXT,
					b BLOB,
					n BLOB
				)`,
				`CREATE TABLE IF NOT EXISTS concurrent_rows (id INTEGER PRIMARY KEY)`,
				`CREATE TABLE IF NOT EXISTS isolation_rows (label TEXT NOT NULL)`,
			} {
				if _, err := ctx.DB().Exec(opCtx, statement); err != nil {
					return err
				}
			}
			started <- sqliteStartObservation{actorID: ctx.ActorID(), generation: ctx.Generation(), saves: ctx.State().Saves}
			return nil
		},
		OnStop: func(ctx *rivet.Context[sqliteConformanceState]) error {
			stopped <- sqliteStartObservation{actorID: ctx.ActorID(), generation: ctx.Generation(), saves: ctx.State().Saves}
			return nil
		},
		Actions: rivet.Actions[sqliteConformanceState]{
			"state_and_sql": rivet.Action(func(ctx *rivet.Context[sqliteConformanceState], _ struct{}) (int, error) {
				opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := ctx.DB().Exec(opCtx, "INSERT INTO todos(label) VALUES (?)", fmt.Sprintf("state-%d", ctx.State().Saves+1)); err != nil {
					return 0, err
				}
				ctx.State().Saves++
				return ctx.State().Saves, nil
			}),
		},
		OnFetch: func(ctx *rivet.Context[sqliteConformanceState], writer http.ResponseWriter, request *http.Request) {
			result, err := runSQLiteHTTPCase(ctx, request, oldLease)
			writer.Header().Set("Content-Type", "application/json")
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
				return
			}
			_ = json.NewEncoder(writer).Encode(result)
		},
	}
}

func runSQLiteHTTPCase(
	actor *rivet.Context[sqliteConformanceState],
	request *http.Request,
	oldLease chan<- rivet.Tx,
) (sqliteHTTPResult, error) {
	ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
	defer cancel()
	db := actor.DB()
	switch request.URL.Path {
	case "/crud":
		insert, err := db.Exec(ctx, "INSERT INTO todos(label, i, r, t, b, n) VALUES (?, ?, ?, ?, ?, ?)", "typed", int64(42), float64(1.25), "text", []byte{}, nil)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		rows, err := db.Query(ctx, "SELECT i, r, t, b, n FROM todos WHERE id = ?", insert.LastInsertID)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		valuesOK := len(rows.Columns) == 5 && len(rows.Values) == 1 && len(rows.Values[0]) == 5
		if valuesOK {
			blob, blobOK := rows.Values[0][3].([]byte)
			valuesOK = rows.Values[0][0] == int64(42) && rows.Values[0][1] == float64(1.25) && rows.Values[0][2] == "text" && blobOK && len(blob) == 0 && rows.Values[0][4] == nil
		}
		if !valuesOK {
			return sqliteHTTPResult{}, fmt.Errorf("typed SQLite row mismatch: columns=%#v values=%#v", rows.Columns, rows.Values)
		}
		update, err := db.Exec(ctx, "UPDATE todos SET t = ? WHERE id = ?", "updated", insert.LastInsertID)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		if _, err := db.Exec(ctx, "DELETE FROM todos WHERE label = ?", "not-present"); err != nil {
			return sqliteHTTPResult{}, err
		}
		return sqliteHTTPResult{ValuesOK: valuesOK, RowsAffected: update.RowsAffected, LastInsertID: insert.LastInsertID}, nil

	case "/transactions":
		commitTx, err := db.Begin(ctx)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		if _, err := commitTx.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "committed"); err != nil {
			return sqliteHTTPResult{}, err
		}
		if err := commitTx.Commit(ctx); err != nil {
			return sqliteHTTPResult{}, err
		}
		rollbackTx, err := db.Begin(ctx)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		if _, err := rollbackTx.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "rolled-back"); err != nil {
			return sqliteHTTPResult{}, err
		}
		if err := rollbackTx.Rollback(ctx); err != nil {
			return sqliteHTTPResult{}, err
		}
		committed, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos WHERE label = ?", "committed")
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		rolledBack, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos WHERE label = ?", "rolled-back")
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		leaseCtx, leaseCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		leaseTx, err := db.Begin(leaseCtx)
		if err != nil {
			leaseCancel()
			return sqliteHTTPResult{}, err
		}
		time.Sleep(400 * time.Millisecond)
		_, leaseErr := leaseTx.Exec(context.Background(), "SELECT 1")
		leaseCancel()
		var structured *rivet.SQLiteError
		if !errors.As(leaseErr, &structured) {
			return sqliteHTTPResult{}, fmt.Errorf("lease expiry returned %T: %v", leaseErr, leaseErr)
		}
		return sqliteHTTPResult{CommitVisible: committed == 1, RollbackInvisible: rolledBack == 0, LeaseErrorCode: structured.Code}, nil

	case "/errors":
		_, syntaxErr := db.Exec(ctx, "INSER broken SQL")
		_, constraintErr := db.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "typed")
		var syntax, constraint *rivet.SQLiteError
		if !errors.As(syntaxErr, &syntax) || !errors.As(constraintErr, &constraint) {
			return sqliteHTTPResult{}, fmt.Errorf("unstructured SQL errors: syntax=%T constraint=%T", syntaxErr, constraintErr)
		}
		if _, err := db.Query(ctx, "SELECT 1"); err != nil {
			return sqliteHTTPResult{}, fmt.Errorf("actor unhealthy after SQL errors: %w", err)
		}
		return sqliteHTTPResult{SyntaxErrorCode: syntax.Code, ConstraintCode: constraint.Code, SyntaxSQLiteCode: syntax.SQLiteCode, ConstraintSQLCode: constraint.SQLiteCode}, nil

	case "/concurrent":
		var wait sync.WaitGroup
		var failuresMu sync.Mutex
		var failures []string
		for index := range 16 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				if _, err := db.Exec(ctx, "INSERT INTO concurrent_rows(id) VALUES (?)", int64(index+1)); err != nil {
					failuresMu.Lock()
					failures = append(failures, err.Error())
					failuresMu.Unlock()
				}
			}()
		}
		wait.Wait()
		count, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM concurrent_rows")
		return sqliteHTTPResult{Count: count, ConcurrentFailures: failures}, err

	case "/isolation":
		_, err := db.Exec(ctx, "INSERT INTO isolation_rows(label) VALUES (?)", request.URL.Query().Get("label"))
		return sqliteHTTPResult{}, err
	case "/isolation-count":
		count, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM isolation_rows")
		return sqliteHTTPResult{Count: count}, err
	case "/label-count":
		count, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos WHERE label = ?", request.URL.Query().Get("label"))
		return sqliteHTTPResult{Count: count}, err
	case "/count":
		count, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos")
		return sqliteHTTPResult{Count: count}, err
	case "/snapshot", "/m2-rehydrate":
		count, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos")
		return sqliteHTTPResult{Count: count, StateSaves: actor.State().Saves}, err
	case "/large":
		rows, err := db.Query(ctx, `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x < 2200) SELECT printf('%0600d', x) FROM n`)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		return sqliteHTTPResult{LargeRows: len(rows.Values)}, nil
	case "/sleep":
		return sqliteHTTPResult{}, actor.Sleep()
	case "/lease-sleep":
		tx, err := db.Begin(ctx)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "dead-lease"); err != nil {
			return sqliteHTTPResult{}, err
		}
		oldLease <- tx
		return sqliteHTTPResult{}, actor.Sleep()
	default:
		return sqliteHTTPResult{}, fmt.Errorf("unknown SQLite conformance path %q", request.URL.Path)
	}
}

func sqliteCount(ctx context.Context, db *rivet.DB, query string, args ...any) (int64, error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	if len(rows.Values) != 1 || len(rows.Values[0]) != 1 {
		return 0, fmt.Errorf("count query returned %#v", rows)
	}
	count, ok := rows.Values[0][0].(int64)
	if !ok {
		return 0, fmt.Errorf("count value has type %T", rows.Values[0][0])
	}
	return count, nil
}

func sqliteGateway(t *testing.T, endpoint, actorID, path string, timeout time.Duration) sqliteHTTPResult {
	t.Helper()
	response, body, err := gatewayRequest(endpoint, actorID, "/request"+path, nil, timeout)
	if err != nil {
		t.Fatalf("SQLite gateway %s: %v", path, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("SQLite gateway %s returned %s: %s", path, response.Status, strings.TrimSpace(string(body)))
	}
	var result sqliteHTTPResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode SQLite gateway %s: %v; body=%s", path, err, body)
	}
	return result
}

func waitSQLiteStart(t *testing.T, started <-chan sqliteStartObservation, actorID string, timeout time.Duration) sqliteStartObservation {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case observation := <-started:
			if observation.actorID == actorID {
				return observation
			}
		case <-deadline.C:
			t.Fatalf("actor %s did not report SQLite OnStart within %s", actorID, timeout)
		}
	}
}

func waitSQLiteStop(
	t *testing.T,
	stopped <-chan sqliteStartObservation,
	served *servedRegistry,
	actorID string,
	generation uint64,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case observation := <-stopped:
			if observation.actorID == actorID && observation.generation == generation {
				return
			}
		case err := <-served.result:
			served.stopOnce.Do(func() {
				served.cancel()
				served.stopErr = err
			})
			t.Fatalf("runner exited while engine was stopped: %v", err)
		case <-deadline.C:
			t.Fatalf("actor %s generation %d did not stop within %s", actorID, generation, timeout)
		}
	}
}
