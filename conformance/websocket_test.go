package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
)

const websocketTestTimeout = 15 * time.Second

type websocketFrame struct {
	kind int
	data []byte
}

type websocketHookObservation struct {
	label        string
	connectionID string
	path         string
	headers      map[string]string
	closeCode    *uint16
	closeReason  string
}

type websocketActionArgument struct {
	Event   string `json:"event"`
	Payload string `json:"payload"`
	Count   int    `json:"count"`
	Size    int    `json:"size"`
}

type rawActorEvent struct {
	Event string            `cbor:"event"`
	Args  []cbor.RawMessage `cbor:"args"`
}

type gatewayWebSocket struct {
	conn      *websocket.Conn
	frames    chan websocketFrame
	closed    chan error
	writeMu   sync.Mutex
	closeOnce sync.Once
}

func TestWebSocketsAndActorEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type websocketState struct{}
	opened := make(chan websocketHookObservation, 128)
	disconnected := make(chan websocketHookObservation, 128)
	handled := make(chan string, 128)
	handlerErrors := make(chan error, 128)
	var connections sync.Map

	registry := rivet.NewRegistry()
	err = rivet.Register(registry, "m4-websocket", rivet.Actor[websocketState]{
		Actions: rivet.Actions[websocketState]{
			"broadcast": rivet.Action(func(ctx *rivet.Context[websocketState], input websocketActionArgument) (bool, error) {
				return true, ctx.Broadcast(input.Event, input.Payload)
			}),
			"burst": rivet.Action(func(ctx *rivet.Context[websocketState], input websocketActionArgument) (int, error) {
				payload := strings.Repeat("s", input.Size)
				for index := range input.Count {
					if err := ctx.Broadcast(input.Event, fmt.Sprintf("%03d:%s", index, payload)); err != nil {
						return index, err
					}
				}
				if err := ctx.Broadcast("burstDone", input.Payload); err != nil {
					return input.Count, err
				}
				return input.Count, nil
			}),
		},
		OnConnect: func(_ *rivet.Context[websocketState], connection *rivet.Connection) error {
			label := headerValue(connection.Headers(), "x-client-label")
			if label == "" {
				return errors.New("missing x-client-label")
			}
			connections.Store(label, connection)
			opened <- websocketHookObservation{
				label:        label,
				connectionID: connection.ID(),
				path:         connection.Path(),
				headers:      connection.Headers(),
			}
			return nil
		},
		OnMessage: func(ctx *rivet.Context[websocketState], connection *rivet.Connection, message rivet.Message) {
			label := headerValue(connection.Headers(), "x-client-label")
			handled <- label
			if message.Binary {
				if err := connection.SendBinary(message.Data); err != nil {
					handlerErrors <- fmt.Errorf("echo binary for %s: %w", label, err)
				}
				return
			}
			text := string(message.Data)
			switch {
			case strings.HasPrefix(text, "broadcast-except:"):
				if err := ctx.BroadcastExcept("peerMessage", strings.TrimPrefix(text, "broadcast-except:"), connection); err != nil {
					handlerErrors <- fmt.Errorf("broadcast except for %s: %w", label, err)
				}
			case strings.HasPrefix(text, "broadcast:"):
				if err := ctx.Broadcast("chatMessage", strings.TrimPrefix(text, "broadcast:")); err != nil {
					handlerErrors <- fmt.Errorf("broadcast for %s: %w", label, err)
				}
			case strings.HasPrefix(text, "target:"):
				parts := strings.SplitN(text, ":", 3)
				if len(parts) != 3 {
					handlerErrors <- fmt.Errorf("target command has %d fields", len(parts))
					return
				}
				target, ok := connections.Load(parts[1])
				if !ok {
					handlerErrors <- fmt.Errorf("target %q is not connected", parts[1])
					return
				}
				if err := target.(*rivet.Connection).SendText(parts[2]); err != nil {
					handlerErrors <- fmt.Errorf("target send to %s: %w", parts[1], err)
				}
			case strings.HasPrefix(text, "echo:"):
				if err := connection.SendText(strings.TrimPrefix(text, "echo:")); err != nil {
					handlerErrors <- fmt.Errorf("echo text for %s: %w", label, err)
				}
			case text == "close-self":
				if err := connection.Close(4000, "actor closed"); err != nil {
					handlerErrors <- fmt.Errorf("actor close for %s: %w", label, err)
				}
			default:
				handlerErrors <- fmt.Errorf("unexpected text command %q", text)
			}
		},
		OnDisconnect: func(_ *rivet.Context[websocketState], connection *rivet.Connection) {
			label := headerValue(connection.Headers(), "x-client-label")
			connections.Delete(label)
			code, reason := connection.CloseInfo()
			disconnected <- websocketHookObservation{
				label:        label,
				connectionID: connection.ID(),
				closeCode:    code,
				closeReason:  reason,
			}
		},
	})
	if err != nil {
		t.Fatalf("register M4 WebSocket actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m4-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "m4-websocket", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})

	clientA := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "a", true)
	openA := waitWebSocketHook(t, opened, "a")
	clientB := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "b", true)
	openB := waitWebSocketHook(t, opened, "b")
	if openA.connectionID == "" || openB.connectionID == "" || openA.connectionID == openB.connectionID {
		t.Fatalf("connection IDs = %q and %q, want distinct non-empty values", openA.connectionID, openB.connectionID)
	}
	if !strings.Contains(openA.path, "/websocket/chat") || headerValue(openA.headers, "x-client-label") != "a" {
		t.Fatalf("OnConnect metadata = path %q headers %#v", openA.path, openA.headers)
	}

	clientA.write(t, websocket.TextMessage, []byte("broadcast:hello"))
	waitHandled(t, handled, "a")
	waitActorEvent(t, clientA, "chatMessage", "hello")
	waitActorEvent(t, clientB, "chatMessage", "hello")

	clientA.write(t, websocket.TextMessage, []byte("broadcast-except:peer-only"))
	waitHandled(t, handled, "a")
	waitActorEvent(t, clientB, "peerMessage", "peer-only")
	assertNoWebSocketFrame(t, clientA, 300*time.Millisecond)

	clientA.write(t, websocket.TextMessage, []byte("target:b:only-b"))
	waitHandled(t, handled, "a")
	waitTextFrame(t, clientB, "only-b")
	assertNoWebSocketFrame(t, clientA, 300*time.Millisecond)

	clientA.write(t, websocket.TextMessage, []byte("echo:text-frame"))
	waitHandled(t, handled, "a")
	waitTextFrame(t, clientA, "text-frame")
	largeBinary := bytes.Repeat([]byte{0x5a}, 1<<20)
	clientA.write(t, websocket.BinaryMessage, largeBinary)
	waitHandled(t, handled, "a")
	waitBinaryFrame(t, clientA, largeBinary)

	clientA.closeWithCode(t, websocket.CloseNormalClosure, "client closed")
	disconnectA := waitWebSocketHook(t, disconnected, "a")
	assertCloseObservation(t, disconnectA, websocket.CloseNormalClosure, "client closed")
	clientB.write(t, websocket.TextMessage, []byte("echo:b-survived-client-close"))
	waitHandled(t, handled, "b")
	waitTextFrame(t, clientB, "b-survived-client-close")

	clientC := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "c", true)
	waitWebSocketHook(t, opened, "c")
	clientB.write(t, websocket.TextMessage, []byte("close-self"))
	waitHandled(t, handled, "b")
	assertGatewayWebSocketClose(t, clientB, 4000, "actor closed")
	disconnectB := waitWebSocketHook(t, disconnected, "b")
	assertCloseObservation(t, disconnectB, 4000, "actor closed")
	clientC.write(t, websocket.TextMessage, []byte("echo:c-survived-actor-close"))
	waitHandled(t, handled, "c")
	waitTextFrame(t, clientC, "c-survived-actor-close")

	clientD := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "d", true)
	waitWebSocketHook(t, opened, "d")
	actionBroadcast := gatewayAction(t, engine.endpoint, actor.ActorID, "broadcast", []any{
		websocketActionArgument{Event: "actionEvent", Payload: "from-action"},
	}, websocketTestTimeout)
	assertSuccessfulAction(t, actionBroadcast)
	waitActorEvent(t, clientC, "actionEvent", "from-action")
	waitActorEvent(t, clientD, "actionEvent", "from-action")

	clientC.closeWithCode(t, websocket.CloseNormalClosure, "fanout phase")
	waitWebSocketHook(t, disconnected, "c")
	clientD.closeWithCode(t, websocket.CloseNormalClosure, "fanout phase")
	waitWebSocketHook(t, disconnected, "d")

	fanoutClients := make([]*gatewayWebSocket, 0, 50)
	for index := range 50 {
		label := fmt.Sprintf("fanout-%02d", index)
		fanoutClients = append(fanoutClients, openGatewayWebSocket(t, engine.endpoint, actor.ActorID, label, true))
		waitWebSocketHook(t, opened, label)
	}
	fanout := gatewayAction(t, engine.endpoint, actor.ActorID, "broadcast", []any{
		websocketActionArgument{Event: "fanout", Payload: "exactly-once"},
	}, websocketTestTimeout)
	assertSuccessfulAction(t, fanout)
	for _, client := range fanoutClients {
		waitActorEvent(t, client, "fanout", "exactly-once")
		assertNoWebSocketFrame(t, client, 100*time.Millisecond)
	}
	for index, client := range fanoutClients {
		client.closeWithCode(t, websocket.CloseNormalClosure, "fanout complete")
		waitWebSocketHook(t, disconnected, fmt.Sprintf("fanout-%02d", index))
	}

	// The slow peer deliberately has no read loop. The fast peer must still
	// receive the complete burst and terminal event. The native queue-overflow
	// close policy is exercised deterministically in the Rust unit suite.
	slow := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "slow", false)
	waitWebSocketHook(t, opened, "slow")
	fast := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "fast", true)
	waitWebSocketHook(t, opened, "fast")
	burst := gatewayAction(t, engine.endpoint, actor.ActorID, "burst", []any{
		websocketActionArgument{Event: "burst", Payload: "complete", Count: 16, Size: 64 << 10},
	}, websocketTestTimeout)
	assertSuccessfulAction(t, burst)
	for range 16 {
		waitActorEventName(t, fast, "burst")
	}
	waitActorEvent(t, fast, "burstDone", "complete")
	assertNoHandlerError(t, handlerErrors)

	slow.closeWithCode(t, websocket.CloseNormalClosure, "test complete")
	slow.close()
	waitWebSocketHook(t, disconnected, "slow")
	fast.closeWithCode(t, websocket.CloseNormalClosure, "test complete")
	waitWebSocketHook(t, disconnected, "fast")
	deleteActor(t, engine.endpoint, actor.ActorID)
}

func openGatewayWebSocket(
	t *testing.T,
	endpoint, actorID, label string,
	read bool,
) *gatewayWebSocket {
	t.Helper()
	websocketURL, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse engine endpoint: %v", err)
	}
	switch websocketURL.Scheme {
	case "http":
		websocketURL.Scheme = "ws"
	case "https":
		websocketURL.Scheme = "wss"
	default:
		t.Fatalf("unsupported engine endpoint scheme %q", websocketURL.Scheme)
	}
	websocketURL.Path = "/gateway/" + url.PathEscape(actorID) + "/websocket/chat"
	websocketURL.RawQuery = "client=" + url.QueryEscape(label)
	dialer := websocket.Dialer{
		HandshakeTimeout: websocketTestTimeout,
		Subprotocols: []string{
			"rivet",
			"rivet_target.actor",
			"rivet_actor." + actorID,
			"rivet_token.dev",
		},
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer dev")
	headers.Set("X-Client-Label", label)
	conn, response, err := dialer.Dial(websocketURL.String(), headers)
	if err != nil {
		status := "<no response>"
		if response != nil {
			status = response.Status
		}
		t.Fatalf("open gateway WebSocket %s: status=%s error=%v", label, status, err)
	}
	client := &gatewayWebSocket{
		conn:   conn,
		frames: make(chan websocketFrame, 256),
		closed: make(chan error, 1),
	}
	conn.SetReadLimit((1 << 20) + (64 << 10))
	if read {
		go client.readLoop()
	}
	t.Cleanup(func() { client.close() })
	return client
}

func (c *gatewayWebSocket) readLoop() {
	for {
		kind, data, err := c.conn.ReadMessage()
		if err != nil {
			c.closed <- err
			return
		}
		c.frames <- websocketFrame{kind: kind, data: data}
	}
}

func (c *gatewayWebSocket) write(t *testing.T, kind int, data []byte) {
	t.Helper()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(websocketTestTimeout)); err != nil {
		t.Fatalf("set WebSocket write deadline: %v", err)
	}
	if err := c.conn.WriteMessage(kind, data); err != nil {
		t.Fatalf("write WebSocket frame: %v", err)
	}
}

func (c *gatewayWebSocket) closeWithCode(t *testing.T, code int, reason string) {
	t.Helper()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	if err := c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline); err != nil {
		t.Fatalf("write WebSocket close: %v", err)
	}
}

func (c *gatewayWebSocket) close() {
	c.closeOnce.Do(func() { _ = c.conn.Close() })
}

func waitActorEvent(t *testing.T, client *gatewayWebSocket, event, payload string) {
	t.Helper()
	observedEvent, observedPayload := waitActorEventName(t, client, event)
	if observedEvent != event || observedPayload != payload {
		t.Fatalf("actor event = (%q, %q), want (%q, %q)", observedEvent, observedPayload, event, payload)
	}
}

func waitActorEventName(t *testing.T, client *gatewayWebSocket, wantEvent string) (string, string) {
	t.Helper()
	select {
	case frame := <-client.frames:
		if frame.kind != websocket.BinaryMessage {
			t.Fatalf("actor event frame kind = %d, want binary", frame.kind)
		}
		var event rawActorEvent
		if err := cbor.Unmarshal(frame.data, &event); err != nil {
			t.Fatalf("decode actor event: %v", err)
		}
		if event.Event != wantEvent || len(event.Args) != 1 {
			t.Fatalf("actor event = %q with %d args, want %q with one arg", event.Event, len(event.Args), wantEvent)
		}
		var payload string
		if err := cbor.Unmarshal(event.Args[0], &payload); err != nil {
			t.Fatalf("decode actor event payload: %v", err)
		}
		return event.Event, payload
	case err := <-client.closed:
		t.Fatalf("WebSocket closed while waiting for actor event %q: %v", wantEvent, err)
	case <-time.After(websocketTestTimeout):
		t.Fatalf("timed out waiting for actor event %q", wantEvent)
	}
	return "", ""
}

func waitTextFrame(t *testing.T, client *gatewayWebSocket, want string) {
	t.Helper()
	select {
	case frame := <-client.frames:
		if frame.kind != websocket.TextMessage || string(frame.data) != want {
			t.Fatalf("text frame = kind %d payload %q, want %q", frame.kind, frame.data, want)
		}
	case err := <-client.closed:
		t.Fatalf("WebSocket closed while waiting for text frame %q: %v", want, err)
	case <-time.After(websocketTestTimeout):
		t.Fatalf("timed out waiting for text frame %q", want)
	}
}

func waitBinaryFrame(t *testing.T, client *gatewayWebSocket, want []byte) {
	t.Helper()
	select {
	case frame := <-client.frames:
		if frame.kind != websocket.BinaryMessage || !bytes.Equal(frame.data, want) {
			t.Fatalf("binary frame = kind %d length %d, want length %d", frame.kind, len(frame.data), len(want))
		}
	case err := <-client.closed:
		t.Fatalf("WebSocket closed while waiting for binary frame: %v", err)
	case <-time.After(websocketTestTimeout):
		t.Fatalf("timed out waiting for %d-byte binary frame", len(want))
	}
}

func assertNoWebSocketFrame(t *testing.T, client *gatewayWebSocket, duration time.Duration) {
	t.Helper()
	select {
	case frame := <-client.frames:
		t.Fatalf("unexpected WebSocket frame kind %d length %d", frame.kind, len(frame.data))
	case err := <-client.closed:
		t.Fatalf("WebSocket closed unexpectedly: %v", err)
	case <-time.After(duration):
	}
}

func assertGatewayWebSocketClose(t *testing.T, client *gatewayWebSocket, code int, reason string) {
	t.Helper()
	select {
	case err := <-client.closed:
		var closeError *websocket.CloseError
		if !errors.As(err, &closeError) || closeError.Code != code || closeError.Text != reason {
			t.Fatalf("gateway WebSocket close = %v, want code %d reason %q", err, code, reason)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatalf("timed out waiting for gateway WebSocket close %d", code)
	}
}

func waitWebSocketHook(
	t *testing.T,
	observations <-chan websocketHookObservation,
	label string,
) websocketHookObservation {
	t.Helper()
	deadline := time.After(websocketTestTimeout)
	for {
		select {
		case observation := <-observations:
			if observation.label == label {
				return observation
			}
			t.Fatalf("WebSocket hook label = %q, want %q", observation.label, label)
		case <-deadline:
			t.Fatalf("timed out waiting for WebSocket hook for %q", label)
		}
	}
}

func waitHandled(t *testing.T, handled <-chan string, label string) {
	t.Helper()
	select {
	case observed := <-handled:
		if observed != label {
			t.Fatalf("OnMessage connection = %q, want %q", observed, label)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatalf("timed out waiting for OnMessage on %q", label)
	}
}

func assertCloseObservation(t *testing.T, observation websocketHookObservation, code int, reason string) {
	t.Helper()
	if observation.closeCode == nil || int(*observation.closeCode) != code || observation.closeReason != reason {
		t.Fatalf("OnDisconnect close = %#v %q, want %d %q", observation.closeCode, observation.closeReason, code, reason)
	}
}

func assertSuccessfulAction(t *testing.T, response gatewayResponse) {
	t.Helper()
	if response.err != nil || response.response == nil || response.response.StatusCode != http.StatusOK {
		t.Fatalf("action response: status=%s err=%v body=%s", responseStatus(response.response), response.err, response.body)
	}
}

func assertNoHandlerError(t *testing.T, errors <-chan error) {
	t.Helper()
	select {
	case err := <-errors:
		t.Fatalf("WebSocket handler: %v", err)
	default:
	}
}

func headerValue(headers map[string]string, name string) string {
	for key, value := range headers {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}
