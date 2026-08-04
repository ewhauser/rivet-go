package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

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
	eventually(t, 5*time.Second, func() (bool, error) {
		data, readErr := os.ReadFile(chat.logPath)
		if readErr != nil {
			return false, readErr
		}
		return bytes.Contains(data, []byte("drain probe action started")), nil
	})
	chat.signal(t, syscall.SIGTERM)
	select {
	case result := <-actionResult:
		assertActionOutput(t, result, http.StatusOK, 1)
	case <-time.After(8 * time.Second):
		t.Fatal("in-flight example action did not complete during SIGTERM drain")
	}
	assertGatewayWebSocketClose(t, client, 1001, "runner shutting down")
	if err := chat.wait(15 * time.Second); err != nil {
		t.Fatalf("chat example SIGTERM exit: %v\n%s", err, chat.logTail())
	}
	waitForRunner(t, engine.endpoint, chatRunner, false)
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
