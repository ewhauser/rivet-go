package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	LeaseCommitCode    string   `json:"lease_commit_code,omitempty"`
	LeaseRollbackCode  string   `json:"lease_rollback_code,omitempty"`
	CancelledTxUsable  bool     `json:"cancelled_tx_usable,omitempty"`
	InterleavingCode   string   `json:"interleaving_code,omitempty"`
	SecondBeginCode    string   `json:"second_begin_code,omitempty"`
	SyntaxErrorCode    string   `json:"syntax_error_code,omitempty"`
	ConstraintCode     string   `json:"constraint_code,omitempty"`
	MultiExecCode      string   `json:"multi_exec_code,omitempty"`
	SyntaxSQLiteCode   int32    `json:"syntax_sqlite_code,omitempty"`
	ConstraintSQLCode  int32    `json:"constraint_sqlite_code,omitempty"`
	ConcurrentFailures []string `json:"concurrent_failures,omitempty"`
	StateSaves         int      `json:"state_saves,omitempty"`
	LargeRows          int      `json:"large_rows,omitempty"`
	ResultLimitCode    string   `json:"result_limit_code,omitempty"`
	ConnectionUsable   bool     `json:"connection_usable,omitempty"`
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
			fidelity := sqliteGateway(t, engine.endpoint, first.ActorID, "/value-fidelity", 45*time.Second)
			if !fidelity.ValuesOK {
				t.Fatalf("value-fidelity round trip = %#v", fidelity)
			}

			transactions := sqliteGateway(t, engine.endpoint, first.ActorID, "/transactions", 30*time.Second)
			if !transactions.CommitVisible || !transactions.RollbackInvisible || !transactions.CancelledTxUsable ||
				transactions.LeaseErrorCode != "transaction_expired" || transactions.LeaseCommitCode != "transaction_expired" ||
				transactions.LeaseRollbackCode != "transaction_expired" || transactions.InterleavingCode != "context deadline exceeded" ||
				transactions.SecondBeginCode != "transaction_already_open" {
				t.Fatalf("transaction conformance = %#v", transactions)
			}

			errorsResult := sqliteGateway(t, engine.endpoint, first.ActorID, "/errors", 30*time.Second)
			if errorsResult.SyntaxErrorCode != "sqlite_error" || errorsResult.ConstraintCode != "sqlite_error" || errorsResult.MultiExecCode != "sqlite_error" || errorsResult.SyntaxSQLiteCode == 0 || errorsResult.ConstraintSQLCode == 0 {
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
			limit := sqliteGateway(t, engine.endpoint, first.ActorID, "/result-limit", 90*time.Second)
			if limit.ResultLimitCode != "sqlite_result_too_large" || !limit.ConnectionUsable {
				t.Fatalf("result-limit recovery = %#v", limit)
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

			{
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
				if !errors.As(leaseErr, &structured) || structured.Code != "sqlite_endpoint_closed" {
					t.Fatalf("old %s lease error = %T %v, want sqlite_endpoint_closed", transport, leaseErr, leaseErr)
				}
				afterLeaseSleep := sqliteGateway(t, engine.endpoint, first.ActorID, "/label-count?label=dead-lease", rehydrateWindow)
				if afterLeaseSleep.Count != 0 {
					t.Fatalf("dead %s lease committed %d rows", transport, afterLeaseSleep.Count)
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

func TestDatabaseActorLiveGenerationDoesNotRehydrateAcrossEngineCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine SQLite crash recovery conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)
	type observation struct {
		actorID    string
		generation uint64
		count      int
	}
	started := make(chan observation, 4)
	stopped := make(chan observation, 2)
	definition := rivet.Actor[persistentCounterState]{
		Database: true,
		OnStart: func(ctx *rivet.Context[persistentCounterState]) error {
			if ctx.State().Count == 0 {
				ctx.State().Count = 41
				saveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := ctx.Save(saveCtx); err != nil {
					return err
				}
			}
			started <- observation{actorID: ctx.ActorID(), generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
		OnStop: func(ctx *rivet.Context[persistentCounterState]) error {
			stopped <- observation{actorID: ctx.ActorID(), generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
	}

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m7-live-crash", definition); err != nil {
		t.Fatalf("register live-crash SQLite actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m7-live-crash-%d", time.Now().UnixNano())
	served := startRegistry(t, engine, runnerName, registry)
	key := "m7-live-database-crash"
	actor := createActor(t, engine.endpoint, "m7-live-crash", runnerName, "restart", &key, nil)
	var first observation
	select {
	case first = <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("database actor did not start before engine crash")
	}
	if first.actorID != actor.ActorID || first.count != 41 {
		t.Fatalf("first database actor observation = %#v", first)
	}
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})

	engine.kill(t)
	select {
	case stoppedActor := <-stopped:
		if stoppedActor.actorID != actor.ActorID || stoppedActor.generation != first.generation || stoppedActor.count != 41 {
			t.Fatalf("pre-restart database actor stop = %#v", stoppedActor)
		}
	case err := <-served.result:
		served.stopOnce.Do(func() {
			served.cancel()
			served.stopErr = err
		})
		t.Fatalf("runner exited while the engine was stopped: %v", err)
	case <-time.After(disconnectLivenessWindow + 10*time.Second):
		t.Fatal("database actor worker did not stop after engine loss")
	}

	engine.start(t)
	eventually(t, 30*time.Second, func() (bool, error) {
		envoys, err := listEnvoys(engine.endpoint, runnerName)
		if err != nil {
			return false, err
		}
		for _, envoy := range envoys {
			if envoy.PoolName == runnerName && envoy.StopTS == nil {
				return true, nil
			}
		}
		return false, nil
	})
	// v2.3.10 retains the old live generation through its 22-second envoy
	// liveness window. Even after that window, LocalNative database actors are
	// not converted into a wakeable new generation by standalone replacement.
	liveness := time.NewTimer(disconnectLivenessWindow)
	defer liveness.Stop()
	<-liveness.C
	stale, err := getActor(engine.endpoint, actor.ActorID, true)
	if err != nil {
		t.Fatal(err)
	}
	if stale.ConnectableTS == nil || stale.SleepTS != nil || stale.DestroyTS != nil || len(stale.Error) != 0 {
		t.Fatalf("post-crash live database actor metadata = %#v, want the old generation to remain connectable", stale)
	}

	response, body, wakeErr := gatewayRequest(
		engine.endpoint,
		actor.ActorID,
		"/m2-rehydrate",
		nil,
		10*time.Second,
	)
	if wakeErr != nil {
		t.Fatalf("post-crash database request: %v", wakeErr)
	}
	if response == nil || response.StatusCode != http.StatusServiceUnavailable || strings.TrimSpace(string(body)) != "Actor not found" {
		t.Fatalf("post-crash database request = %s body=%q, want 503 Actor not found", responseStatus(response), body)
	}
	select {
	case observation := <-started:
		t.Fatalf("post-crash database wake reached Go ActorStart: %#v", observation)
	default:
	}
	stillStale, err := getActor(engine.endpoint, actor.ActorID, true)
	if err != nil {
		t.Fatal(err)
	}
	if stillStale.ConnectableTS == nil || stillStale.SleepTS != nil || stillStale.DestroyTS != nil || len(stillStale.Error) != 0 {
		t.Fatalf("post-request live database actor metadata = %#v, want the old generation to remain assigned", stillStale)
	}
}

func sqliteConformanceActor(
	started chan<- sqliteStartObservation,
	stopped chan<- sqliteStartObservation,
	oldLease chan<- rivet.Tx,
) rivet.Actor[sqliteConformanceState] {
	return rivet.Actor[sqliteConformanceState]{
		Database: true,
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
				if err := ctx.Save(opCtx); err != nil {
					return 0, err
				}
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

	case "/value-fidelity":
		text := "before\x00after"
		rows, err := db.Query(
			ctx,
			"SELECT ?, ?, ?, ?, ?, ?, ?, ?",
			int64(math.MinInt64),
			int64(math.MaxInt64),
			math.NaN(),
			math.Inf(1),
			math.Inf(-1),
			text,
			[]byte{},
			nil,
		)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		valuesOK := len(rows.Values) == 1 && len(rows.Values[0]) == 8
		if valuesOK {
			values := rows.Values[0]
			emptyBlob, blobOK := values[6].([]byte)
			positive, positiveOK := values[3].(float64)
			negative, negativeOK := values[4].(float64)
			valuesOK = values[0] == int64(math.MinInt64) &&
				values[1] == int64(math.MaxInt64) &&
				values[2] == nil &&
				positiveOK && math.IsInf(positive, 1) &&
				negativeOK && math.IsInf(negative, -1) &&
				values[5] == text && blobOK && len(emptyBlob) == 0 && values[7] == nil
		}
		if !valuesOK {
			return sqliteHTTPResult{}, fmt.Errorf("boundary SQLite row mismatch: columns=%#v values=%#v", rows.Columns, rows.Values)
		}
		blob := bytes.Repeat([]byte{0xa5}, 1<<20)
		blobRows, err := db.Query(ctx, "SELECT ?", blob)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		if len(blobRows.Values) != 1 || len(blobRows.Values[0]) != 1 {
			return sqliteHTTPResult{}, fmt.Errorf("blob boundary row shape = %#v", blobRows.Values)
		}
		gotBlob, ok := blobRows.Values[0][0].([]byte)
		if !ok || !bytes.Equal(gotBlob, blob) {
			return sqliteHTTPResult{}, fmt.Errorf("blob boundary result differs: type=%T length=%d", blobRows.Values[0][0], len(gotBlob))
		}
		_, tooLargeErr := db.Query(ctx, "SELECT ?", append(blob, 0))
		var tooLarge *rivet.SQLiteError
		if !errors.As(tooLargeErr, &tooLarge) || tooLarge.Code != "sqlite_argument_too_large" {
			return sqliteHTTPResult{}, fmt.Errorf("oversized blob error = %T %v", tooLargeErr, tooLargeErr)
		}
		return sqliteHTTPResult{ValuesOK: true}, nil

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

		cancelTx, err := db.Begin(ctx)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		cancelled, cancelNow := context.WithCancel(context.Background())
		cancelNow()
		if _, err := cancelTx.Query(cancelled, "SELECT 1"); !errors.Is(err, context.Canceled) {
			return sqliteHTTPResult{}, fmt.Errorf("cancelled transaction query = %T %v", err, err)
		}
		_, cancelRecoveryErr := cancelTx.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "cancel-recovery")
		if rollbackErr := cancelTx.Rollback(ctx); rollbackErr != nil {
			return sqliteHTTPResult{}, rollbackErr
		}

		interleaveTx, err := db.Begin(ctx)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		if _, err := interleaveTx.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "interleave-rollback"); err != nil {
			return sqliteHTTPResult{}, err
		}
		interleaveCtx, interleaveCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		_, interleaveErr := db.Query(interleaveCtx, "SELECT COUNT(*) FROM todos")
		interleaveCancel()
		if !errors.Is(interleaveErr, context.DeadlineExceeded) {
			return sqliteHTTPResult{}, fmt.Errorf("non-transaction query interleaved with lease: %T %v", interleaveErr, interleaveErr)
		}
		if err := interleaveTx.Rollback(ctx); err != nil {
			return sqliteHTTPResult{}, err
		}
		interleavedWrites, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos WHERE label = ?", "interleave-rollback")
		if err != nil || interleavedWrites != 0 {
			return sqliteHTTPResult{}, fmt.Errorf("interleaved rollback count = %d: %w", interleavedWrites, err)
		}

		openTx, err := db.Begin(ctx)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		_, secondErr := db.Begin(ctx)
		var secondStructured *rivet.SQLiteError
		if !errors.As(secondErr, &secondStructured) || secondStructured.Code != "transaction_already_open" {
			return sqliteHTTPResult{}, fmt.Errorf("second Begin while lease open = %T %v", secondErr, secondErr)
		}
		if err := openTx.Rollback(ctx); err != nil {
			return sqliteHTTPResult{}, err
		}
		recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, recoveryErr := db.Query(recoveryCtx, "SELECT 1")
		recoveryCancel()
		if recoveryErr != nil {
			return sqliteHTTPResult{}, fmt.Errorf("database remained gated after rejected second Begin: %w", recoveryErr)
		}

		leaseCtx, leaseCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		leaseTx, err := db.Begin(leaseCtx)
		if err != nil {
			leaseCancel()
			return sqliteHTTPResult{}, err
		}
		if _, err := leaseTx.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "expired-commit"); err != nil {
			leaseCancel()
			return sqliteHTTPResult{}, err
		}
		time.Sleep(400 * time.Millisecond)
		_, leaseErr := leaseTx.Exec(context.Background(), "SELECT 1")
		commitErr := leaseTx.Commit(context.Background())
		leaseCancel()
		var leaseStructured, commitStructured *rivet.SQLiteError
		if !errors.As(leaseErr, &leaseStructured) || !errors.As(commitErr, &commitStructured) {
			return sqliteHTTPResult{}, fmt.Errorf("lease expiry errors: operation=%T %v commit=%T %v", leaseErr, leaseErr, commitErr, commitErr)
		}
		expiredCommitCount, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos WHERE label = ?", "expired-commit")
		if err != nil {
			return sqliteHTTPResult{}, err
		}

		rollbackLeaseCtx, rollbackLeaseCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		rollbackLease, err := db.Begin(rollbackLeaseCtx)
		if err != nil {
			rollbackLeaseCancel()
			return sqliteHTTPResult{}, err
		}
		if _, err := rollbackLease.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "expired-rollback"); err != nil {
			rollbackLeaseCancel()
			return sqliteHTTPResult{}, err
		}
		time.Sleep(400 * time.Millisecond)
		rollbackLeaseErr := rollbackLease.Rollback(context.Background())
		rollbackLeaseCancel()
		var rollbackStructured *rivet.SQLiteError
		if !errors.As(rollbackLeaseErr, &rollbackStructured) {
			return sqliteHTTPResult{}, fmt.Errorf("expired rollback error = %T %v", rollbackLeaseErr, rollbackLeaseErr)
		}
		expiredRollbackCount, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos WHERE label = ?", "expired-rollback")
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		return sqliteHTTPResult{
			CommitVisible:     committed == 1,
			RollbackInvisible: rolledBack == 0 && expiredCommitCount == 0 && expiredRollbackCount == 0,
			LeaseErrorCode:    leaseStructured.Code,
			LeaseCommitCode:   commitStructured.Code,
			LeaseRollbackCode: rollbackStructured.Code,
			CancelledTxUsable: cancelRecoveryErr == nil,
			InterleavingCode:  interleaveErr.Error(),
			SecondBeginCode:   secondStructured.Code,
		}, nil

	case "/errors":
		_, syntaxErr := db.Exec(ctx, "INSER broken SQL")
		_, constraintErr := db.Exec(ctx, "INSERT INTO todos(label) VALUES (?)", "typed")
		_, multiErr := db.Exec(ctx, "INSERT INTO todos(label) VALUES ('multi-first'); INSERT INTO todos(label) VALUES ('multi-second')")
		var syntax, constraint, multi *rivet.SQLiteError
		if !errors.As(syntaxErr, &syntax) || !errors.As(constraintErr, &constraint) || !errors.As(multiErr, &multi) {
			return sqliteHTTPResult{}, fmt.Errorf("unstructured SQL errors: syntax=%T constraint=%T multi=%T", syntaxErr, constraintErr, multiErr)
		}
		if multi.SQLiteCode != -1 || multi.StatementIndex != 0 {
			return sqliteHTTPResult{}, fmt.Errorf("multi-statement error metadata = %#v", multi)
		}
		multiCount, err := sqliteCount(ctx, db, "SELECT COUNT(*) FROM todos WHERE label LIKE 'multi-%'")
		if err != nil || multiCount != 0 {
			return sqliteHTTPResult{}, fmt.Errorf("multi-statement Exec changed %d rows: %w", multiCount, err)
		}
		if _, err := db.Query(ctx, "SELECT 1"); err != nil {
			return sqliteHTTPResult{}, fmt.Errorf("actor unhealthy after SQL errors: %w", err)
		}
		return sqliteHTTPResult{SyntaxErrorCode: syntax.Code, ConstraintCode: constraint.Code, MultiExecCode: multi.Code, SyntaxSQLiteCode: syntax.SQLiteCode, ConstraintSQLCode: constraint.SQLiteCode}, nil

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
		empty, err := db.Query(ctx, "SELECT 1 WHERE 0")
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		if len(empty.Columns) != 1 || len(empty.Values) != 0 {
			return sqliteHTTPResult{}, fmt.Errorf("empty SQLite result = %#v", empty)
		}
		rows, err := db.Query(ctx, `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x < 2200) SELECT printf('%0600d', x) FROM n`)
		if err != nil {
			return sqliteHTTPResult{}, err
		}
		return sqliteHTTPResult{LargeRows: len(rows.Values)}, nil
	case "/result-limit":
		_, limitErr := db.Query(ctx, `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x < 1024) SELECT zeroblob(32768) FROM n`)
		var structured *rivet.SQLiteError
		if !errors.As(limitErr, &structured) {
			return sqliteHTTPResult{}, fmt.Errorf("oversized result error = %T %v", limitErr, limitErr)
		}
		rows, recoveryErr := db.Query(ctx, "SELECT 1")
		usable := recoveryErr == nil && len(rows.Values) == 1 && len(rows.Values[0]) == 1 && rows.Values[0][0] == int64(1)
		return sqliteHTTPResult{ResultLimitCode: structured.Code, ConnectionUsable: usable}, recoveryErr
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
