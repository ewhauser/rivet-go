package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
)

func TestRunnableExamplesAndSIGTERMDrain(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine example conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	counterBinary := buildExample(t, "counter")
	counterRunner := fmt.Sprintf("counter-example-%d", time.Now().UnixNano())
	counter := startExample(t, counterBinary,
		"-endpoint", engine.endpoint,
		"-runner-name", counterRunner,
	)
	waitForRunner(t, engine.endpoint, counterRunner, true)
	counterActor := createActor(t, engine.endpoint, "counter", counterRunner, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, counterActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	assertActionOutput(t, gatewayAction(t, engine.endpoint, counterActor.ActorID, "increment", []any{
		map[string]int{"amount": 3},
	}, 10*time.Second), http.StatusOK, 3)
	response, body, err := gatewayHTTPRequest(
		engine.endpoint,
		counterActor.ActorID,
		"/request/current",
		http.MethodGet,
		nil,
		nil,
		10*time.Second,
	)
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "3\n" {
		t.Fatalf("counter HTTP example: status=%s body=%q err=%v", responseStatus(response), body, err)
	}
	counter.signal(t, syscall.SIGTERM)
	if err := counter.wait(15 * time.Second); err != nil {
		t.Fatalf("counter example SIGTERM exit: %v\n%s", err, counter.logTail())
	}
	waitForRunner(t, engine.endpoint, counterRunner, false)

	chatBinary := buildExample(t, "chat")
	chatRunner := fmt.Sprintf("chat-example-%d", time.Now().UnixNano())
	chat := startExample(t, chatBinary,
		"-endpoint", engine.endpoint,
		"-runner-name", chatRunner,
	)
	waitForRunner(t, engine.endpoint, chatRunner, true)
	chatActor := createActor(t, engine.endpoint, "chat", chatRunner, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, chatActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	client := openGatewayWebSocket(t, engine.endpoint, chatActor.ActorID, "example-client", true)
	waitTextFrame(t, client, "connected")
	client.write(t, websocket.TextMessage, []byte("hello from conformance"))
	waitChatBroadcast(t, client, 1, "hello from conformance")
	httpActor := createActor(t, engine.endpoint, "chat", chatRunner, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, httpActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})

	actionResult := make(chan gatewayResponse, 1)
	go func() {
		actionResult <- gatewayAction(
			t,
			engine.endpoint,
			chatActor.ActorID,
			"hold",
			[]any{map[string]int{"milliseconds": 1_500}},
			8*time.Second,
		)
	}()
	httpResult := make(chan gatewayHTTPResponse, 1)
	go func() {
		response, body, requestErr := gatewayHTTPRequest(
			engine.endpoint,
			httpActor.ActorID,
			"/request/hold?milliseconds=1500",
			http.MethodGet,
			nil,
			nil,
			8*time.Second,
		)
		httpResult <- gatewayHTTPResponse{response: response, body: body, err: requestErr}
	}()
	eventually(t, 5*time.Second, func() (bool, error) {
		data, readErr := os.ReadFile(chat.logPath)
		if readErr != nil {
			return false, readErr
		}
		return bytes.Contains(data, []byte("drain probe action started")) &&
			bytes.Contains(data, []byte("drain probe HTTP started")), nil
	})
	chat.signal(t, syscall.SIGTERM)
	select {
	case result := <-actionResult:
		assertActionOutput(t, result, http.StatusOK, 1)
	case <-time.After(8 * time.Second):
		t.Fatal("in-flight example action did not complete during SIGTERM drain")
	}
	select {
	case result := <-httpResult:
		if result.err != nil || result.response.StatusCode != http.StatusOK || string(result.body) != "1\n" {
			t.Fatalf("in-flight example HTTP drain: status=%s body=%q err=%v", responseStatus(result.response), result.body, result.err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("in-flight example HTTP request did not complete during SIGTERM drain")
	}
	assertGatewayWebSocketClose(t, client, 1001, "runner shutting down")
	if err := chat.wait(15 * time.Second); err != nil {
		t.Fatalf("chat example SIGTERM exit: %v\n%s", err, chat.logTail())
	}
	waitForRunner(t, engine.endpoint, chatRunner, false)

	forcedRunner := fmt.Sprintf("chat-forced-example-%d", time.Now().UnixNano())
	forced := startExample(t, chatBinary,
		"-endpoint", engine.endpoint,
		"-runner-name", forcedRunner,
		"-shutdown-timeout", "200ms",
	)
	waitForRunner(t, engine.endpoint, forcedRunner, true)
	forcedActionActor := createActor(t, engine.endpoint, "chat", forcedRunner, "destroy", nil, nil)
	forcedHTTPActor := createActor(t, engine.endpoint, "chat", forcedRunner, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, forcedActionActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	waitForActor(t, engine.endpoint, forcedHTTPActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	forcedClient := openGatewayWebSocket(t, engine.endpoint, forcedActionActor.ActorID, "forced-example-client", true)
	waitTextFrame(t, forcedClient, "connected")
	forcedActionResult := make(chan gatewayResponse, 1)
	go func() {
		forcedActionResult <- gatewayAction(
			t,
			engine.endpoint,
			forcedActionActor.ActorID,
			"hold",
			[]any{map[string]int{"milliseconds": 1_500}},
			8*time.Second,
		)
	}()
	forcedHTTPResult := make(chan gatewayHTTPResponse, 1)
	go func() {
		response, body, requestErr := gatewayHTTPRequest(
			engine.endpoint,
			forcedHTTPActor.ActorID,
			"/request/hold?milliseconds=1500",
			http.MethodGet,
			nil,
			nil,
			8*time.Second,
		)
		forcedHTTPResult <- gatewayHTTPResponse{response: response, body: body, err: requestErr}
	}()
	eventually(t, 5*time.Second, func() (bool, error) {
		data, readErr := os.ReadFile(forced.logPath)
		if readErr != nil {
			return false, readErr
		}
		return bytes.Contains(data, []byte("drain probe action started")) &&
			bytes.Contains(data, []byte("drain probe HTTP started")), nil
	})
	forced.signal(t, syscall.SIGTERM)
	assertGatewayWebSocketClose(t, forcedClient, 1001, "runner shutting down")
	if err := forced.wait(15 * time.Second); err == nil {
		t.Fatal("forced-deadline example exited successfully")
	} else {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
			t.Fatalf("forced-deadline example exit=%v, want code 1\n%s", err, forced.logTail())
		}
	}
	select {
	case result := <-forcedActionResult:
		if result.err == nil && result.response != nil && result.response.StatusCode == http.StatusOK {
			t.Fatalf("forced-deadline action completed successfully: %#v", result)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("forced-deadline action client did not resolve")
	}
	select {
	case result := <-forcedHTTPResult:
		if result.err == nil && result.response != nil && result.response.StatusCode == http.StatusOK {
			t.Fatalf("forced-deadline HTTP completed successfully: status=%s body=%q", result.response.Status, result.body)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("forced-deadline HTTP client did not resolve")
	}
	waitForRunner(t, engine.endpoint, forcedRunner, false)
}

func TestPortedRunnableExamples(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine example conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	t.Run("todo-sqlite", func(t *testing.T) {
		runnerName := fmt.Sprintf("todo-sqlite-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "todo-sqlite"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		actor := createActor(t, engine.endpoint, "todo-list", runnerName, "destroy", nil, nil)
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})

		type todoOutput struct {
			ID        int64  `json:"id"`
			Title     string `json:"title"`
			Completed bool   `json:"completed"`
			CreatedAt int64  `json:"createdAt"`
		}
		added := decodeActionOutput[todoOutput](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"add",
			[]any{map[string]any{"title": "port the SQLite example"}},
			10*time.Second,
		), http.StatusOK)
		if added.ID == 0 || added.Title != "port the SQLite example" || added.Completed || added.CreatedAt == 0 {
			t.Fatalf("added todo = %#v", added)
		}

		listed := decodeActionOutput[[]todoOutput](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"list",
			[]any{struct{}{}},
			10*time.Second,
		), http.StatusOK)
		if len(listed) != 1 || listed[0] != added {
			t.Fatalf("listed todos = %#v, want %#v", listed, []todoOutput{added})
		}

		toggled := decodeActionOutput[todoOutput](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"toggle",
			[]any{map[string]any{"id": added.ID}},
			10*time.Second,
		), http.StatusOK)
		if !toggled.Completed || toggled.ID != added.ID {
			t.Fatalf("toggled todo = %#v", toggled)
		}

		deleted := decodeActionOutput[bool](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"delete",
			[]any{map[string]any{"id": added.ID}},
			10*time.Second,
		), http.StatusOK)
		if !deleted {
			t.Fatal("delete action returned false")
		}

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})

	t.Run("http-counter", func(t *testing.T) {
		runnerName := fmt.Sprintf("http-counter-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "http-counter"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		actor := createActor(t, engine.endpoint, "http-counter", runnerName, "destroy", nil, nil)
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})

		headers := make(http.Header)
		headers.Set("Content-Type", "application/json")
		response, body, err := gatewayHTTPRequest(
			engine.endpoint,
			actor.ActorID,
			"/request/increment",
			http.MethodPost,
			[]byte(`{"amount":3}`),
			headers,
			10*time.Second,
		)
		assertHTTPCounter(t, response, body, err, http.StatusOK, 3)

		response, body, err = gatewayHTTPRequest(
			engine.endpoint,
			actor.ActorID,
			"/request/count",
			http.MethodGet,
			nil,
			nil,
			10*time.Second,
		)
		assertHTTPCounter(t, response, body, err, http.StatusOK, 3)

		response, body, err = gatewayHTTPRequest(
			engine.endpoint,
			actor.ActorID,
			"/request/increment",
			http.MethodPost,
			[]byte(`{"amount":1,"unknown":true}`),
			headers,
			10*time.Second,
		)
		if err != nil || response.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid HTTP counter request: status=%s body=%q err=%v", responseStatus(response), body, err)
		}

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})

	t.Run("reminder", func(t *testing.T) {
		runnerName := fmt.Sprintf("reminder-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "reminder"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		actor := createActor(t, engine.endpoint, "reminder", runnerName, "restart", nil, nil)
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})

		type reminderOutput struct {
			Message       string `json:"message"`
			Pending       bool   `json:"pending"`
			DueAtMS       int64  `json:"dueAtMs"`
			TriggeredAtMS int64  `json:"triggeredAtMs"`
		}
		scheduled := decodeActionOutput[reminderOutput](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"schedule",
			[]any{map[string]any{
				"message":           "wake from the example alarm",
				"delayMilliseconds": 6_000,
			}},
			10*time.Second,
		), http.StatusOK)
		if !scheduled.Pending || scheduled.Message != "wake from the example alarm" || scheduled.DueAtMS == 0 {
			t.Fatalf("scheduled reminder = %#v", scheduled)
		}
		decodeActionOutput[bool](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"sleep",
			[]any{struct{}{}},
			10*time.Second,
		), http.StatusOK)
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
		})
		eventually(t, 45*time.Second, func() (bool, error) {
			observed, err := getActor(engine.endpoint, actor.ActorID, false)
			if err != nil {
				return false, err
			}
			return observed.ConnectableTS != nil && observed.DestroyTS == nil, nil
		})

		triggered := decodeActionOutput[reminderOutput](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"status",
			[]any{struct{}{}},
			10*time.Second,
		), http.StatusOK)
		if triggered.Pending || triggered.TriggeredAtMS < triggered.DueAtMS || triggered.Message != scheduled.Message {
			t.Fatalf("triggered reminder = %#v, scheduled = %#v", triggered, scheduled)
		}

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})

	t.Run("scheduling", func(t *testing.T) {
		runnerName := fmt.Sprintf("scheduling-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "scheduling"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		actor := createActor(t, engine.endpoint, "scheduled-reminders", runnerName, "restart", nil, nil)
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})

		type reminderOutput struct {
			ID          string `json:"id"`
			ScheduleID  string `json:"scheduleId"`
			Message     string `json:"message"`
			ScheduledAt int64  `json:"scheduledAt"`
			CompletedAt int64  `json:"completedAt"`
		}
		first := decodeActionOutput[reminderOutput](t, gatewayAction(
			t, engine.endpoint, actor.ActorID, "scheduleReminder",
			[]any{map[string]any{"message": "first", "delayMilliseconds": 35_000}},
			10*time.Second,
		), http.StatusOK)
		second := decodeActionOutput[reminderOutput](t, gatewayAction(
			t, engine.endpoint, actor.ActorID, "scheduleReminder",
			[]any{map[string]any{"message": "cancelled", "delayMilliseconds": 55_000}},
			10*time.Second,
		), http.StatusOK)
		if first.ScheduleID == "" || second.ScheduleID == "" || first.ScheduleID == second.ScheduleID {
			t.Fatalf("created scheduling example reminders = %#v, %#v", first, second)
		}
		type scheduleOutput struct {
			ID     string `json:"id"`
			Action string `json:"action"`
			RunAt  int64  `json:"runAt"`
		}
		pending := decodeActionOutput[[]scheduleOutput](t, gatewayAction(
			t, engine.endpoint, actor.ActorID, "getPendingSchedules", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		if len(pending) != 2 || pending[0].ID != first.ScheduleID || pending[1].ID != second.ScheduleID {
			t.Fatalf("scheduling example pending records = %#v", pending)
		}
		type cancelOutput struct {
			Success bool `json:"success"`
		}
		cancelled := decodeActionOutput[cancelOutput](t, gatewayAction(
			t, engine.endpoint, actor.ActorID, "cancelReminder", []any{map[string]string{"id": second.ID}}, 10*time.Second,
		), http.StatusOK)
		if !cancelled.Success {
			t.Fatal("scheduling example did not cancel one reminder")
		}
		decodeActionOutput[bool](t, gatewayAction(
			t, engine.endpoint, actor.ActorID, "sleep", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
		})
		eventually(t, 75*time.Second, func() (bool, error) {
			observed, err := getActor(engine.endpoint, actor.ActorID, false)
			if err != nil {
				return false, err
			}
			return observed.ConnectableTS != nil && observed.DestroyTS == nil, nil
		})
		reminders := decodeActionOutput[[]reminderOutput](t, gatewayAction(
			t, engine.endpoint, actor.ActorID, "getReminders", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		if len(reminders) != 1 || reminders[0].ID != first.ID || reminders[0].CompletedAt < reminders[0].ScheduledAt {
			t.Fatalf("scheduling example reminders after wake = %#v", reminders)
		}

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})

	t.Run("per-tenant-database", func(t *testing.T) {
		runnerName := fmt.Sprintf("per-tenant-database-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "per-tenant-database"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		acmeKey := "acme"
		acme := createActor(t, engine.endpoint, "company-database", runnerName, "destroy", &acmeKey, nil)
		waitForActor(t, engine.endpoint, acme.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})

		type companyInfo struct {
			ActorID     string `json:"actorId"`
			ActorName   string `json:"actorName"`
			ActorKey    string `json:"actorKey"`
			CompanyName string `json:"companyName"`
		}
		info := decodeActionOutput[companyInfo](t, gatewayAction(
			t, engine.endpoint, acme.ActorID, "getCompany", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		if info.ActorID != acme.ActorID || info.ActorName != "company-database" ||
			info.ActorKey != acmeKey || info.CompanyName != acmeKey {
			t.Fatalf("company identity = %#v", info)
		}

		type employeeOutput struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			Role string `json:"role"`
		}
		employee := decodeActionOutput[employeeOutput](t, gatewayAction(
			t,
			engine.endpoint,
			acme.ActorID,
			"addEmployee",
			[]any{map[string]string{"name": "Ada", "role": "Engineer"}},
			10*time.Second,
		), http.StatusOK)
		if employee.ID == "" || employee.Name != "Ada" || employee.Role != "Engineer" {
			t.Fatalf("added employee = %#v", employee)
		}

		type projectOutput struct {
			ID     string `json:"id"`
			Name   string `json:"name"`
			Status string `json:"status"`
		}
		project := decodeActionOutput[projectOutput](t, gatewayAction(
			t,
			engine.endpoint,
			acme.ActorID,
			"addProject",
			[]any{map[string]string{"name": "Compiler", "status": "active"}},
			10*time.Second,
		), http.StatusOK)
		if project.ID == "" || project.Name != "Compiler" || project.Status != "active" {
			t.Fatalf("added project = %#v", project)
		}

		type companyStats struct {
			EmployeeCount int   `json:"employeeCount"`
			ProjectCount  int   `json:"projectCount"`
			CreatedAt     int64 `json:"createdAt"`
			UpdatedAt     int64 `json:"updatedAt"`
		}
		stats := decodeActionOutput[companyStats](t, gatewayAction(
			t, engine.endpoint, acme.ActorID, "getStats", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		if stats.EmployeeCount != 1 || stats.ProjectCount != 1 || stats.CreatedAt == 0 || stats.UpdatedAt < stats.CreatedAt {
			t.Fatalf("company stats = %#v", stats)
		}

		globexKey := "globex"
		globex := createActor(t, engine.endpoint, "company-database", runnerName, "destroy", &globexKey, nil)
		waitForActor(t, engine.endpoint, globex.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})
		globexEmployees := decodeActionOutput[[]employeeOutput](t, gatewayAction(
			t, engine.endpoint, globex.ActorID, "listEmployees", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		if len(globexEmployees) != 0 {
			t.Fatalf("second tenant employees = %#v, want empty", globexEmployees)
		}
		globexInfo := decodeActionOutput[companyInfo](t, gatewayAction(
			t, engine.endpoint, globex.ActorID, "getCompany", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		if globexInfo.CompanyName != globexKey || globexInfo.ActorKey != globexKey {
			t.Fatalf("second tenant identity = %#v", globexInfo)
		}

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})

	t.Run("actor-kv", func(t *testing.T) {
		runnerName := fmt.Sprintf("actor-kv-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "actor-kv"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		actor := createActor(t, engine.endpoint, "kv-store", runnerName, "destroy", nil, nil)
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})

		for key, value := range map[string]string{
			"greeting:ada":   "hello",
			"greeting:grace": "ahoy",
			"other":          "ignored",
		} {
			stored := decodeActionOutput[bool](t, gatewayAction(
				t,
				engine.endpoint,
				actor.ActorID,
				"putText",
				[]any{map[string]string{"key": key, "value": value}},
				10*time.Second,
			), http.StatusOK)
			if !stored {
				t.Fatalf("putText %q returned false", key)
			}
		}

		type textValue struct {
			Key   string `json:"key"`
			Value string `json:"value"`
			Found bool   `json:"found"`
		}
		got := decodeActionOutput[textValue](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"getText",
			[]any{map[string]string{"key": "greeting:ada"}},
			10*time.Second,
		), http.StatusOK)
		if !got.Found || got.Key != "greeting:ada" || got.Value != "hello" {
			t.Fatalf("getText = %#v", got)
		}

		type textEntry struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		listed := decodeActionOutput[[]textEntry](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"listText",
			[]any{map[string]any{"prefix": "greeting:", "reverse": true, "limit": 2}},
			10*time.Second,
		), http.StatusOK)
		if len(listed) != 2 || listed[0].Key != "greeting:grace" || listed[1].Key != "greeting:ada" {
			t.Fatalf("listText = %#v", listed)
		}

		bytesOutput := decodeActionOutput[[]int](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"roundtripBytes",
			[]any{map[string]any{"key": "avatar", "values": []int{0, 127, 255}}},
			10*time.Second,
		), http.StatusOK)
		if len(bytesOutput) != 3 || bytesOutput[0] != 0 || bytesOutput[1] != 127 || bytesOutput[2] != 255 {
			t.Fatalf("roundtripBytes = %#v", bytesOutput)
		}

		deleted := decodeActionOutput[bool](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"delete",
			[]any{map[string]string{"key": "greeting:ada"}},
			10*time.Second,
		), http.StatusOK)
		if !deleted {
			t.Fatal("delete returned false for an existing key")
		}
		missing := decodeActionOutput[textValue](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"getText",
			[]any{map[string]string{"key": "greeting:ada"}},
			10*time.Second,
		), http.StatusOK)
		if missing.Found || missing.Value != "" {
			t.Fatalf("deleted value = %#v", missing)
		}

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})

	t.Run("connection-admin", func(t *testing.T) {
		runnerName := fmt.Sprintf("connection-admin-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "connection-admin"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		actor := createActor(t, engine.endpoint, "connection-admin", runnerName, "destroy", nil, nil)
		waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
			return actor.ConnectableTS != nil && actor.DestroyTS == nil
		})

		alpha := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "alpha", true)
		waitTextFrame(t, alpha, "connected")
		beta := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "beta", true)
		waitTextFrame(t, beta, "connected")

		type connectionSummary struct {
			ID    string `json:"id"`
			Label string `json:"label"`
			Path  string `json:"path"`
		}
		connections := decodeActionOutput[[]connectionSummary](t, gatewayAction(
			t, engine.endpoint, actor.ActorID, "listConnections", []any{struct{}{}}, 10*time.Second,
		), http.StatusOK)
		if len(connections) != 2 {
			t.Fatalf("connection list = %#v, want two", connections)
		}
		ids := make(map[string]string, 2)
		for _, connection := range connections {
			if connection.ID == "" || !strings.Contains(connection.Path, "/websocket/chat") {
				t.Fatalf("connection summary = %#v", connection)
			}
			ids[connection.Label] = connection.ID
		}
		if ids["alpha"] == "" || ids["beta"] == "" || ids["alpha"] == ids["beta"] {
			t.Fatalf("connection IDs by label = %#v", ids)
		}

		sent := decodeActionOutput[bool](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"send",
			[]any{map[string]string{"connectionId": ids["beta"], "message": "private-message"}},
			10*time.Second,
		), http.StatusOK)
		if !sent {
			t.Fatal("targeted send returned false")
		}
		waitTextFrame(t, beta, "private-message")
		assertNoWebSocketFrame(t, alpha, 300*time.Millisecond)

		disconnected := decodeActionOutput[bool](t, gatewayAction(
			t,
			engine.endpoint,
			actor.ActorID,
			"disconnect",
			[]any{map[string]any{"connectionId": ids["alpha"], "code": 4001, "reason": "removed"}},
			10*time.Second,
		), http.StatusOK)
		if !disconnected {
			t.Fatal("disconnect returned false")
		}
		assertGatewayWebSocketClose(t, alpha, 4001, "removed")
		eventually(t, 5*time.Second, func() (bool, error) {
			connections = decodeActionOutput[[]connectionSummary](t, gatewayAction(
				t, engine.endpoint, actor.ActorID, "listConnections", []any{struct{}{}}, 10*time.Second,
			), http.StatusOK)
			return len(connections) == 1 && connections[0].ID == ids["beta"], nil
		})

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})

	t.Run("actor-actions", func(t *testing.T) {
		runnerName := fmt.Sprintf("actor-actions-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "actor-actions"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
			"-token", "dev",
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		client, err := rivet.NewClient(rivet.ClientConfig{
			Endpoint: engine.endpoint, RunnerName: runnerName, Token: "dev",
		})
		if err != nil {
			t.Fatal(err)
		}
		companyInput, err := json.Marshal(map[string]string{
			"name": "Growing Corp", "industry": "Technology",
		})
		if err != nil {
			t.Fatal(err)
		}
		company, err := client.Create(context.Background(), "company", rivet.CreateOptions{
			Key: []string{"34-5678901"}, Input: companyInput, CrashPolicy: rivet.CrashPolicyDestroy,
		})
		if err != nil {
			t.Fatalf("create company: %v", err)
		}
		type companyProfile struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			Industry string `json:"industry"`
		}
		profile, err := rivet.Call[companyProfile](context.Background(), company, "getProfile", struct{}{})
		if err != nil || profile.ID == "" || profile.Name != "Growing Corp" || profile.Industry != "Technology" {
			t.Fatalf("company profile = %#v, %v", profile, err)
		}
		resolved, err := client.Get(context.Background(), company.ID())
		if err != nil || resolved.ID() != company.ID() {
			t.Fatalf("resolve company by ID = %q, %v", resolved.ID(), err)
		}
		sameCompany, created, err := client.GetOrCreate(
			context.Background(), "company", []string{"34-5678901"}, rivet.CreateOptions{Input: companyInput},
		)
		if err != nil || created || sameCompany.ID() != company.ID() {
			t.Fatalf("get-or-create company = %q, created=%v, err=%v", sameCompany.ID(), created, err)
		}

		type employeeProfile struct {
			EmployeeID string `json:"employeeId"`
			Name       string `json:"name"`
			Email      string `json:"email"`
			Position   string `json:"position"`
			CompanyID  string `json:"companyId"`
			HiredAt    int64  `json:"hiredAt"`
		}
		employee, err := rivet.Call[employeeProfile](context.Background(), company, "createEmployee", map[string]string{
			"name": "Jane Smith", "email": "jane@growingcorp.com", "position": "Software Engineer",
		})
		if err != nil || employee.EmployeeID == "" || employee.CompanyID != profile.ID || employee.HiredAt == 0 {
			t.Fatalf("created employee = %#v, %v", employee, err)
		}
		employeeActor, err := client.GetByKey(context.Background(), "employee", []string{"jane@growingcorp.com"})
		if err != nil {
			t.Fatalf("resolve employee by key: %v", err)
		}
		employee, err = rivet.Call[employeeProfile](context.Background(), employeeActor, "updateProfile", map[string]string{
			"position": "Senior Engineer",
		})
		if err != nil || employee.Position != "Senior Engineer" || employee.Email != "jane@growingcorp.com" {
			t.Fatalf("updated employee = %#v, %v", employee, err)
		}
		employeesJSON, err := company.CallRaw(context.Background(), "getEmployees", json.RawMessage(`[{}]`))
		if err != nil || string(employeesJSON) != `["jane@growingcorp.com"]` {
			t.Fatalf("raw getEmployees = %s, %v", employeesJSON, err)
		}

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})

	t.Run("cross-actor-actions", func(t *testing.T) {
		runnerName := fmt.Sprintf("cross-actor-actions-example-%d", time.Now().UnixNano())
		process := startExample(t, buildExample(t, "cross-actor-actions"),
			"-endpoint", engine.endpoint,
			"-runner-name", runnerName,
			"-token", "dev",
		)
		waitForRunner(t, engine.endpoint, runnerName, true)
		client, err := rivet.NewClient(rivet.ClientConfig{
			Endpoint: engine.endpoint, RunnerName: runnerName, Token: "dev",
		})
		if err != nil {
			t.Fatal(err)
		}
		inventoryInput, err := json.Marshal(map[string]any{"itemName": "Laptop", "initialStock": 10})
		if err != nil {
			t.Fatal(err)
		}
		inventory, err := client.Create(context.Background(), "inventory", rivet.CreateOptions{
			Key: []string{"laptop"}, Input: inventoryInput, CrashPolicy: rivet.CrashPolicyDestroy,
		})
		if err != nil {
			t.Fatalf("create inventory: %v", err)
		}
		checkoutInput, err := json.Marshal(map[string]string{"customerName": "Alice"})
		if err != nil {
			t.Fatal(err)
		}
		checkout, err := client.Create(context.Background(), "checkout", rivet.CreateOptions{
			Key: []string{"checkout-1"}, Input: checkoutInput, CrashPolicy: rivet.CrashPolicyDestroy,
		})
		if err != nil {
			t.Fatalf("create checkout: %v", err)
		}
		type checkoutResult struct {
			Success        bool   `json:"success"`
			Message        string `json:"message"`
			RemainingStock int    `json:"remainingStock"`
		}
		added, err := rivet.Call[checkoutResult](context.Background(), checkout, "addItem", map[string]any{
			"itemId": "laptop", "quantity": 3,
		})
		if err != nil || !added.Success || added.RemainingStock != 7 || !strings.Contains(added.Message, "Laptop") {
			t.Fatalf("add item = %#v, %v", added, err)
		}
		type stockResult struct {
			ItemName string `json:"itemName"`
			Stock    int    `json:"stock"`
		}
		stock, err := rivet.Call[stockResult](context.Background(), inventory, "getStock", struct{}{})
		if err != nil || stock.ItemName != "Laptop" || stock.Stock != 7 {
			t.Fatalf("reserved stock = %#v, %v", stock, err)
		}
		type checkoutSummary struct {
			Items      []map[string]any `json:"items"`
			TotalItems int              `json:"totalItems"`
		}
		summary, err := rivet.Call[checkoutSummary](context.Background(), checkout, "getSummary", struct{}{})
		if err != nil || summary.TotalItems != 3 || len(summary.Items) != 1 {
			t.Fatalf("checkout summary = %#v, %v", summary, err)
		}
		canceled, err := rivet.Call[checkoutResult](context.Background(), checkout, "cancelCheckout", struct{}{})
		if err != nil || !canceled.Success {
			t.Fatalf("cancel checkout = %#v, %v", canceled, err)
		}
		stock, err = rivet.Call[stockResult](context.Background(), inventory, "getStock", struct{}{})
		if err != nil || stock.Stock != 10 {
			t.Fatalf("released stock = %#v, %v", stock, err)
		}

		stopExampleCleanly(t, engine.endpoint, runnerName, process)
	})
}

func decodeActionOutput[T any](t *testing.T, result gatewayResponse, status int) T {
	t.Helper()
	var zero T
	if result.err != nil {
		t.Fatalf("action request: %v", result.err)
	}
	if result.response == nil || result.response.StatusCode != status {
		t.Fatalf("action status = %s, want %d; body=%s", responseStatus(result.response), status, result.body)
	}
	var response struct {
		Output T `json:"output"`
	}
	if err := json.Unmarshal(result.body, &response); err != nil {
		t.Fatalf("decode action response: %v; body=%s", err, result.body)
		return zero
	}
	return response.Output
}

func assertHTTPCounter(
	t *testing.T,
	response *http.Response,
	body []byte,
	err error,
	status, count int,
) {
	t.Helper()
	if err != nil || response == nil || response.StatusCode != status {
		t.Fatalf("HTTP counter: status=%s body=%q err=%v", responseStatus(response), body, err)
	}
	var output struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(body, &output); err != nil {
		t.Fatalf("decode HTTP counter response: %v; body=%q", err, body)
	}
	if output.Count != count {
		t.Fatalf("HTTP counter count = %d, want %d", output.Count, count)
	}
}

func stopExampleCleanly(t *testing.T, endpoint, runnerName string, process *exampleProcess) {
	t.Helper()
	process.signal(t, syscall.SIGTERM)
	if err := process.wait(15 * time.Second); err != nil {
		t.Fatalf("example SIGTERM exit: %v\n%s", err, process.logTail())
	}
	waitForRunner(t, endpoint, runnerName, false)
}

type gatewayHTTPResponse struct {
	response *http.Response
	body     []byte
	err      error
}

func buildExample(t *testing.T, name string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), name)
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	command := exec.Command("go", "build", "-o", binary, "../examples/"+name)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s example: %v\n%s", name, err, output)
	}
	return binary
}

type exampleProcess struct {
	command *exec.Cmd
	logPath string
	done    chan struct{}
	mu      sync.Mutex
	err     error
}

func startExample(t *testing.T, binary string, arguments ...string) *exampleProcess {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "example.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open example log: %v", err)
	}
	command := exec.Command(binary, arguments...)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start example %s: %v", binary, err)
	}
	process := &exampleProcess{command: command, logPath: logPath, done: make(chan struct{})}
	go func() {
		waitErr := command.Wait()
		_ = logFile.Close()
		process.mu.Lock()
		process.err = waitErr
		process.mu.Unlock()
		close(process.done)
	}()
	t.Cleanup(func() {
		select {
		case <-process.done:
			return
		default:
		}
		_ = command.Process.Kill()
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
		}
	})
	return process
}

func (p *exampleProcess) signal(t *testing.T, signal os.Signal) {
	t.Helper()
	if err := p.command.Process.Signal(signal); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("signal example process: %v", err)
	}
}

func (p *exampleProcess) wait(timeout time.Duration) error {
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.err
	case <-time.After(timeout):
		return fmt.Errorf("process did not exit within %s", timeout)
	}
}

func (p *exampleProcess) logTail() string { return devLogTail(p.logPath) }

func devLogTail(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	if len(data) > 16<<10 {
		data = data[len(data)-(16<<10):]
	}
	return string(data)
}

func waitForRunner(t *testing.T, endpoint, runnerName string, present bool) {
	t.Helper()
	eventually(t, 20*time.Second, func() (bool, error) {
		envoys, err := listEnvoys(endpoint, runnerName)
		if err != nil {
			return false, err
		}
		for _, envoy := range envoys {
			if envoy.PoolName == runnerName && envoy.StopTS == nil {
				return present, nil
			}
		}
		return !present, nil
	})
}

func waitChatBroadcast(t *testing.T, client *gatewayWebSocket, sequence uint64, text string) {
	t.Helper()
	select {
	case frame := <-client.frames:
		if frame.kind != websocket.BinaryMessage {
			t.Fatalf("chat broadcast frame kind = %d, want binary", frame.kind)
		}
		var envelope actorConnectEventEnvelope
		if err := cbor.Unmarshal(frame.data, &envelope); err != nil {
			t.Fatalf("decode chat broadcast: %v", err)
		}
		if envelope.Body.Tag != "Event" || envelope.Body.Value.Name != "message" || len(envelope.Body.Value.Args) != 1 {
			t.Fatalf("chat broadcast envelope = %#v", envelope)
		}
		var payload struct {
			Sequence uint64 `json:"sequence"`
			Text     string `json:"text"`
		}
		if err := cbor.Unmarshal(envelope.Body.Value.Args[0], &payload); err != nil {
			t.Fatalf("decode chat payload: %v", err)
		}
		if payload.Sequence != sequence || payload.Text != text {
			t.Fatalf("chat payload = %#v, want sequence=%d text=%q", payload, sequence, text)
		}
	case err := <-client.closed:
		t.Fatalf("chat WebSocket closed before broadcast: %v", err)
	case <-time.After(websocketTestTimeout):
		t.Fatal("timed out waiting for chat broadcast")
	}
}
