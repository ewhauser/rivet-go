package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
	"go.uber.org/goleak"
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

type actorConnectEventEnvelope struct {
	Body actorConnectEventBody `cbor:"body"`
}

type actorConnectEventBody struct {
	Tag   string                 `cbor:"tag"`
	Value actorConnectEventValue `cbor:"val"`
}

type actorConnectEventValue struct {
	Name string            `cbor:"name"`
	Args []cbor.RawMessage `cbor:"args"`
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
	leakBaseline := goleak.IgnoreCurrent()

	type websocketState struct{}
	opened := make(chan websocketHookObservation, 128)
	disconnected := make(chan websocketHookObservation, 128)
	handled := make(chan string, 128)
	orderedMessages := make(chan int, 128)
	handlerErrors := make(chan error, 128)
	oversizeErrors := make(chan error, 8)
	var connections sync.Map

	registry := rivet.NewRegistry()
	err = rivet.Register(registry, "m4-websocket", rivet.Actor[websocketState]{
		Actions: rivet.Actions[websocketState]{
			"broadcast": rivet.Action(func(ctx *rivet.Context[websocketState], input websocketActionArgument) (bool, error) {
				return true, ctx.Broadcast(input.Event, input.Payload)
			}),
			"sendBurstTo": rivet.Action(func(_ *rivet.Context[websocketState], input websocketActionArgument) (int, error) {
				stored, ok := connections.Load(input.Payload)
				if !ok {
					return 0, fmt.Errorf("connection %q is not live", input.Payload)
				}
				connection := stored.(*rivet.Connection)
				payload := strings.Repeat("q", input.Size)
				var wait sync.WaitGroup
				errorsSeen := make(chan error, input.Count)
				for index := range input.Count {
					wait.Add(1)
					go func() {
						defer wait.Done()
						if err := connection.SendText(fmt.Sprintf("%06d:%s", index, payload)); err != nil {
							errorsSeen <- err
						}
					}()
				}
				wait.Wait()
				close(errorsSeen)
				for err := range errorsSeen {
					return 0, err
				}
				return input.Count, nil
			}),
		},
		OnConnect: func(_ *rivet.Context[websocketState], connection *rivet.Connection) error {
			label := headerValue(connection.Headers(), "x-client-label")
			if label == "" {
				return errors.New("missing x-client-label")
			}
			if label == "reject" {
				return errors.New("connection rejected by actor")
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
			case text == "":
				if err := connection.SendText(""); err != nil {
					handlerErrors <- fmt.Errorf("echo empty text for %s: %w", label, err)
				}
			case text == "ordered-broadcast":
				if err := ctx.Broadcast("ordered", "first"); err != nil {
					handlerErrors <- fmt.Errorf("first ordered broadcast: %w", err)
				}
				if err := ctx.Broadcast("ordered", "second"); err != nil {
					handlerErrors <- fmt.Errorf("second ordered broadcast: %w", err)
				}
			case strings.HasPrefix(text, "order:"):
				index, err := strconv.Atoi(strings.TrimPrefix(text, "order:"))
				if err != nil {
					handlerErrors <- fmt.Errorf("parse ordered message %q: %w", text, err)
					return
				}
				orderedMessages <- index
			case text == "send-oversize":
				err := connection.SendBinary(make([]byte, (1<<20)+1))
				oversizeErrors <- err
				if err := connection.SendText("oversize-rejected"); err != nil {
					handlerErrors <- fmt.Errorf("confirm outgoing oversize rejection: %w", err)
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
	served := startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "m4-websocket", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})

	rejected, response, dialErr := dialGatewayWebSocket(engine.endpoint, actor.ActorID, "reject", true)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if dialErr == nil {
		assertGatewayWebSocketClose(t, rejected, 1008, "actor.handler_error")
		if err := rejected.conn.WriteMessage(websocket.TextMessage, []byte("ignored")); err == nil {
			t.Fatal("rejected WebSocket accepted a later client message")
		}
		rejected.close()
	} else if response == nil || response.StatusCode == http.StatusSwitchingProtocols {
		t.Fatalf("rejected WebSocket did not produce a close or HTTP upgrade failure: response=%v error=%v", response, dialErr)
	}
	select {
	case label := <-handled:
		t.Fatalf("rejected WebSocket executed OnMessage for %q", label)
	case <-time.After(300 * time.Millisecond):
	}

	incomingOversize := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "incoming-oversize", true)
	waitWebSocketHook(t, opened, "incoming-oversize")
	incomingOversize.write(t, websocket.BinaryMessage, make([]byte, (1<<20)+1))
	assertGatewayWebSocketClose(t, incomingOversize, 1009, "message.incoming_too_long")
	disconnectOversize := waitWebSocketHook(t, disconnected, "incoming-oversize")
	assertCloseObservation(t, disconnectOversize, 1009, "message.incoming_too_long")
	select {
	case label := <-handled:
		t.Fatalf("incoming oversize frame executed OnMessage for %q", label)
	case <-time.After(300 * time.Millisecond):
	}

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
	clientA.write(t, websocket.TextMessage, nil)
	waitHandled(t, handled, "a")
	waitTextFrame(t, clientA, "")
	clientA.write(t, websocket.BinaryMessage, nil)
	waitHandled(t, handled, "a")
	waitBinaryFrame(t, clientA, nil)
	largeBinary := bytes.Repeat([]byte{0x5a}, 1<<20)
	clientA.write(t, websocket.BinaryMessage, largeBinary)
	waitHandled(t, handled, "a")
	waitBinaryFrame(t, clientA, largeBinary)

	for index := range 100 {
		clientA.write(t, websocket.TextMessage, []byte(fmt.Sprintf("order:%03d", index)))
	}
	for want := range 100 {
		select {
		case got := <-orderedMessages:
			if got != want {
				t.Fatalf("OnMessage order = %d at position %d", got, want)
			}
			waitHandled(t, handled, "a")
		case <-time.After(websocketTestTimeout):
			t.Fatalf("timed out waiting for ordered message %d", want)
		}
	}
	clientA.write(t, websocket.TextMessage, []byte("ordered-broadcast"))
	waitHandled(t, handled, "a")
	for _, client := range []*gatewayWebSocket{clientA, clientB} {
		waitActorEvent(t, client, "ordered", "first")
		waitActorEvent(t, client, "ordered", "second")
	}

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
	clientC.write(t, websocket.TextMessage, []byte("send-oversize"))
	waitHandled(t, handled, "c")
	select {
	case err := <-oversizeErrors:
		if err == nil || !strings.Contains(err.Error(), "maximum is 1048576") {
			t.Fatalf("actor outgoing oversize error = %v", err)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("timed out waiting for actor outgoing oversize result")
	}
	waitTextFrame(t, clientC, "oversize-rejected")

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
	concurrentResults := make(chan gatewayResponse, 2)
	startConcurrent := make(chan struct{})
	for _, value := range []string{"first", "second"} {
		value := value
		go func() {
			<-startConcurrent
			concurrentResults <- gatewayAction(t, engine.endpoint, actor.ActorID, "broadcast", []any{
				websocketActionArgument{Event: "concurrent", Payload: value},
			}, websocketTestTimeout)
		}()
	}
	close(startConcurrent)
	assertSuccessfulAction(t, <-concurrentResults)
	assertSuccessfulAction(t, <-concurrentResults)
	for _, client := range fanoutClients {
		seen := map[string]int{}
		for range 2 {
			event, payload := waitAnyActorEvent(t, client)
			if event != "concurrent" {
				t.Fatalf("concurrent broadcast event = %q, want concurrent", event)
			}
			seen[payload]++
		}
		if seen["first"] != 1 || seen["second"] != 1 || len(seen) != 2 {
			t.Fatalf("concurrent broadcast receipts = %#v", seen)
		}
		assertNoWebSocketFrame(t, client, 100*time.Millisecond)
	}
	for index, client := range fanoutClients {
		client.closeWithCode(t, websocket.CloseNormalClosure, "fanout complete")
		waitWebSocketHook(t, disconnected, fmt.Sprintf("fanout-%02d", index))
	}

	// The slow peer deliberately has no read loop until after its bounded native
	// queue overflows. The close must be visible through the real gateway while
	// the fast peer remains usable.
	slow := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "slow", false)
	waitWebSocketHook(t, opened, "slow")
	fast := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "fast", true)
	waitWebSocketHook(t, opened, "fast")
	burst := gatewayAction(t, engine.endpoint, actor.ActorID, "sendBurstTo", []any{
		websocketActionArgument{Payload: "slow", Count: 512, Size: 16 << 10},
	}, websocketTestTimeout)
	assertSuccessfulAction(t, burst)
	disconnectSlow := waitWebSocketHook(t, disconnected, "slow")
	assertCloseObservation(t, disconnectSlow, 1013, "outbound_backpressure")
	assertGatewayWebSocketCloseWithoutReadLoop(t, slow, 1013, "outbound_backpressure")
	fast.write(t, websocket.TextMessage, []byte("echo:fast-after-overflow"))
	waitHandled(t, handled, "fast")
	waitTextFrame(t, fast, "fast-after-overflow")
	assertNoHandlerError(t, handlerErrors)

	slow.close()
	fast.closeWithCode(t, websocket.CloseNormalClosure, "test complete")
	waitWebSocketHook(t, disconnected, "fast")
	deleteActor(t, engine.endpoint, actor.ActorID)
	served.stop(t)
	engine.stop()
	goleak.VerifyNone(t, leakBaseline)
}

func TestWebSocketLifecycleRacesAndHookBroadcasts(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type lifecycleState struct{}
	startBroadcast := make(chan error, 4)
	stopBroadcast := make(chan error, 4)
	connectEntered := make(chan struct{}, 1)
	connectRelease := make(chan struct{})
	raceEntered := make(chan struct{}, 1)
	raceRelease := make(chan struct{})
	opened := make(chan websocketHookObservation, 16)
	disconnected := make(chan websocketHookObservation, 16)
	var countMu sync.Mutex
	disconnectCounts := make(map[string]int)

	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m4-lifecycle", rivet.Actor[lifecycleState]{
		Actions: rivet.Actions[lifecycleState]{
			"health": rivet.Action(func(*rivet.Context[lifecycleState], struct{}) (int, error) {
				return 1, nil
			}),
		},
		OnStart: func(ctx *rivet.Context[lifecycleState]) error {
			startBroadcast <- ctx.Broadcast("lifecycle", "starting")
			return nil
		},
		OnConnect: func(_ *rivet.Context[lifecycleState], connection *rivet.Connection) error {
			label := headerValue(connection.Headers(), "x-client-label")
			if label == "during-connect" {
				connectEntered <- struct{}{}
				<-connectRelease
			}
			opened <- websocketHookObservation{label: label, connectionID: connection.ID()}
			return nil
		},
		OnMessage: func(_ *rivet.Context[lifecycleState], connection *rivet.Connection, message rivet.Message) {
			if string(message.Data) != "race-close" {
				return
			}
			raceEntered <- struct{}{}
			<-raceRelease
			_ = connection.Close(4001, "actor race close")
		},
		OnDisconnect: func(_ *rivet.Context[lifecycleState], connection *rivet.Connection) {
			label := headerValue(connection.Headers(), "x-client-label")
			countMu.Lock()
			disconnectCounts[label]++
			countMu.Unlock()
			code, reason := connection.CloseInfo()
			disconnected <- websocketHookObservation{label: label, connectionID: connection.ID(), closeCode: code, closeReason: reason}
		},
		OnStop: func(ctx *rivet.Context[lifecycleState]) error {
			stopBroadcast <- ctx.Broadcast("lifecycle", "stopping")
			return nil
		},
	}); err != nil {
		t.Fatalf("register lifecycle actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m4-lifecycle-%d", time.Now().UnixNano())
	served := startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "m4-lifecycle", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	select {
	case err := <-startBroadcast:
		if err != nil {
			t.Fatalf("OnStart zero-connection broadcast: %v", err)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("OnStart broadcast did not return")
	}

	type dialResult struct {
		client   *gatewayWebSocket
		response *http.Response
		err      error
	}
	dialCtx, cancelDial := context.WithCancel(context.Background())
	dialed := make(chan dialResult, 1)
	go func() {
		client, response, err := dialGatewayWebSocketContext(dialCtx, engine.endpoint, actor.ActorID, "during-connect", false)
		dialed <- dialResult{client: client, response: response, err: err}
	}()
	select {
	case <-connectEntered:
	case <-time.After(websocketTestTimeout):
		t.Fatal("OnConnect did not enter for disconnect race")
	}
	select {
	case result := <-dialed:
		if result.err != nil {
			t.Fatalf("disconnect-race WebSocket upgrade failed before close: %v", result.err)
		}
		result.client.close()
	case <-time.After(2 * time.Second):
		cancelDial()
		result := <-dialed
		if result.client != nil {
			result.client.close()
		}
		if result.response != nil && result.response.Body != nil {
			_ = result.response.Body.Close()
		}
	}
	cancelDial()
	close(connectRelease)
	waitWebSocketHook(t, opened, "during-connect")
	waitWebSocketHook(t, disconnected, "during-connect")
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, actor.ActorID, "health", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		1,
	)

	raceClient := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "race", true)
	waitWebSocketHook(t, opened, "race")
	raceClient.write(t, websocket.TextMessage, []byte("race-close"))
	select {
	case <-raceEntered:
	case <-time.After(websocketTestTimeout):
		t.Fatal("OnMessage did not enter close race")
	}
	clientClose := make(chan error, 1)
	go func() {
		clientClose <- raceClient.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(4002, "client race close"),
			time.Now().Add(2*time.Second),
		)
	}()
	close(raceRelease)
	if err := <-clientClose; err != nil {
		t.Fatalf("client side of close race: %v", err)
	}
	waitWebSocketHook(t, disconnected, "race")

	stopA := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "stop-a", true)
	waitWebSocketHook(t, opened, "stop-a")
	stopB := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "stop-b", true)
	waitWebSocketHook(t, opened, "stop-b")
	deleteActor(t, engine.endpoint, actor.ActorID)
	waitWebSocketHookSet(t, disconnected, "stop-a", "stop-b")
	select {
	case err := <-stopBroadcast:
		if err != nil {
			t.Fatalf("OnStop draining broadcast: %v", err)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("OnStop broadcast did not return")
	}
	for _, client := range []*gatewayWebSocket{stopA, stopB} {
		assertOnStopBroadcastAndClose(t, client, "lifecycle", "stopping", "actor stopped")
	}

	countMu.Lock()
	for _, label := range []string{"during-connect", "race", "stop-a", "stop-b"} {
		if disconnectCounts[label] != 1 {
			t.Fatalf("OnDisconnect count for %s = %d, want 1", label, disconnectCounts[label])
		}
	}
	countMu.Unlock()

	shutdownActor := createActor(t, engine.endpoint, "m4-lifecycle", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, shutdownActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	select {
	case err := <-startBroadcast:
		if err != nil {
			t.Fatalf("runner-shutdown actor OnStart broadcast: %v", err)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("runner-shutdown actor OnStart broadcast did not return")
	}
	shutdownClient := openGatewayWebSocket(t, engine.endpoint, shutdownActor.ActorID, "runner-shutdown", true)
	waitWebSocketHook(t, opened, "runner-shutdown")
	served.stop(t)
	select {
	case err := <-stopBroadcast:
		if err != nil {
			t.Fatalf("runner-shutdown actor OnStop broadcast: %v", err)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("runner-shutdown actor OnStop broadcast did not return")
	}
	waitForActor(t, engine.endpoint, shutdownActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && actor.SleepTS != nil && actor.DestroyTS == nil
	})
	shutdownDisconnect := waitWebSocketHook(t, disconnected, "runner-shutdown")
	assertCloseObservation(t, shutdownDisconnect, 1001, "runner shutting down")
	assertGatewayWebSocketClose(t, shutdownClient, 1001, "runner shutting down")
	countMu.Lock()
	if disconnectCounts["runner-shutdown"] != 1 {
		t.Fatalf("runner shutdown OnDisconnect count = %d, want 1", disconnectCounts["runner-shutdown"])
	}
	countMu.Unlock()
}

func TestWebSocketHookPanicsStopOnlyTheirActor(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type panicState struct{}
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m4-panic-connect", rivet.Actor[panicState]{
		OnConnect: func(*rivet.Context[panicState], *rivet.Connection) error {
			panic("intentional OnConnect panic")
		},
	}); err != nil {
		t.Fatalf("register OnConnect panic actor: %v", err)
	}
	if err := rivet.Register(registry, "m4-panic-message", rivet.Actor[panicState]{
		OnMessage: func(*rivet.Context[panicState], *rivet.Connection, rivet.Message) {
			panic("intentional OnMessage panic")
		},
	}); err != nil {
		t.Fatalf("register OnMessage panic actor: %v", err)
	}
	if err := rivet.Register(registry, "m4-panic-disconnect", rivet.Actor[panicState]{
		OnDisconnect: func(*rivet.Context[panicState], *rivet.Connection) {
			panic("intentional OnDisconnect panic")
		},
	}); err != nil {
		t.Fatalf("register OnDisconnect panic actor: %v", err)
	}
	if err := rivet.Register(registry, "m4-panic-peer", rivet.Actor[panicState]{
		Actions: rivet.Actions[panicState]{
			"health": rivet.Action(func(*rivet.Context[panicState], struct{}) (int, error) {
				return 1, nil
			}),
		},
	}); err != nil {
		t.Fatalf("register panic peer actor: %v", err)
	}

	runnerName := fmt.Sprintf("rivet-go-m4-panics-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	peer := createActor(t, engine.endpoint, "m4-panic-peer", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, peer.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	assertPeer := func() {
		assertActionOutput(
			t,
			gatewayAction(t, engine.endpoint, peer.ActorID, "health", []any{struct{}{}}, websocketTestTimeout),
			http.StatusOK,
			1,
		)
	}
	waitStopped := func(actorID string) {
		waitForActor(t, engine.endpoint, actorID, true, func(actor actorRecord) bool {
			return actor.ConnectableTS == nil || actor.DestroyTS != nil || (len(actor.Error) != 0 && string(actor.Error) != "null")
		})
	}

	connectActor := createActor(t, engine.endpoint, "m4-panic-connect", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, connectActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil
	})
	connectClient := openGatewayWebSocket(t, engine.endpoint, connectActor.ActorID, "panic-connect", true)
	assertGatewayWebSocketClose(t, connectClient, 1008, "actor.handler_panic")
	waitStopped(connectActor.ActorID)
	assertPeer()

	messageActor := createActor(t, engine.endpoint, "m4-panic-message", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, messageActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil
	})
	messageClient := openGatewayWebSocket(t, engine.endpoint, messageActor.ActorID, "panic-message", true)
	messageClient.write(t, websocket.TextMessage, []byte("panic"))
	assertGatewayWebSocketClose(t, messageClient, 1001, "actor stopped")
	waitStopped(messageActor.ActorID)
	assertPeer()

	disconnectActor := createActor(t, engine.endpoint, "m4-panic-disconnect", runnerName, "destroy", nil, nil)
	waitForActor(t, engine.endpoint, disconnectActor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil
	})
	disconnectClient := openGatewayWebSocket(t, engine.endpoint, disconnectActor.ActorID, "panic-disconnect", true)
	disconnectClient.closeWithCode(t, websocket.CloseNormalClosure, "trigger disconnect panic")
	assertGatewayWebSocketClose(t, disconnectClient, websocket.CloseNormalClosure, "trigger disconnect panic")
	waitStopped(disconnectActor.ActorID)
	assertPeer()
}

func TestWebSocketStateSavePersistsAcrossEngineRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type websocketState struct {
		Count int `json:"count"`
	}
	type observation struct {
		actorID    string
		generation uint64
		count      int
	}
	started := make(chan observation, 4)
	stopped := make(chan observation, 2)
	saved := make(chan error, 2)
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "m4-persistent-websocket", rivet.Actor[websocketState]{
		OnStart: func(ctx *rivet.Context[websocketState]) error {
			started <- observation{actorID: ctx.ActorID(), generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
		OnStop: func(ctx *rivet.Context[websocketState]) error {
			stopped <- observation{actorID: ctx.ActorID(), generation: ctx.Generation(), count: ctx.State().Count}
			return nil
		},
		OnMessage: func(ctx *rivet.Context[websocketState], connection *rivet.Connection, message rivet.Message) {
			if string(message.Data) != "save:41" {
				return
			}
			ctx.State().Count = 41
			saveErr := ctx.Save(context.Background())
			saved <- saveErr
			if saveErr == nil {
				_ = connection.SendText("saved")
			}
		},
		Actions: rivet.Actions[websocketState]{
			"get": rivet.Action(func(ctx *rivet.Context[websocketState], _ struct{}) (int, error) {
				return ctx.State().Count, nil
			}),
		},
	}); err != nil {
		t.Fatalf("register persistent WebSocket actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-m4-persistence-%d", time.Now().UnixNano())
	served := startRegistry(t, engine, runnerName, registry)
	key := "m4-websocket-persistence"
	actor := createActor(t, engine.endpoint, "m4-persistent-websocket", runnerName, "restart", &key, nil)

	var first observation
	select {
	case first = <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("persistent WebSocket actor did not start")
	}
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	client := openGatewayWebSocket(t, engine.endpoint, actor.ActorID, "persistence", true)
	client.write(t, websocket.TextMessage, []byte("save:41"))
	select {
	case err := <-saved:
		if err != nil {
			t.Fatalf("save state from OnMessage: %v", err)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("OnMessage state save did not complete")
	}
	waitTextFrame(t, client, "saved")

	engine.kill(t)
	select {
	case stoppedActor := <-stopped:
		if stoppedActor.actorID != actor.ActorID || stoppedActor.generation != first.generation || stoppedActor.count != 41 {
			t.Fatalf("pre-restart WebSocket stop observation = %#v", stoppedActor)
		}
	case err := <-served.result:
		served.stopOnce.Do(func() {
			served.cancel()
			served.stopErr = err
		})
		t.Fatalf("runner exited while the engine was stopped: %v", err)
	case <-time.After(disconnectLivenessWindow + 10*time.Second):
		t.Fatal("WebSocket actor worker did not stop after engine loss")
	}

	engine.start(t)
	waitForActor(t, engine.endpoint, actor.ActorID, true, func(actor actorRecord) bool {
		return actor.ConnectableTS == nil && len(actor.Error) != 0
	})
	wakeResult := wakeActor(engine.endpoint, actor.ActorID, rehydrateWindow)
	var rehydrated observation
	select {
	case rehydrated = <-started:
	case err := <-served.result:
		served.stopOnce.Do(func() {
			served.cancel()
			served.stopErr = err
		})
		t.Fatalf("runner exited before WebSocket actor rehydration: %v", err)
	case <-time.After(rehydrateWindow):
		t.Fatal("WebSocket actor was not rehydrated after engine restart")
	}
	if rehydrated.actorID != actor.ActorID || rehydrated.count != 41 || rehydrated.generation <= first.generation {
		t.Fatalf("rehydrated WebSocket actor = %#v, first = %#v", rehydrated, first)
	}
	select {
	case wakeErr := <-wakeResult:
		if wakeErr != nil {
			t.Fatalf("gateway wake request: %v", wakeErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("gateway wake request did not complete")
	}
	assertActionOutput(
		t,
		gatewayAction(t, engine.endpoint, actor.ActorID, "get", []any{struct{}{}}, websocketTestTimeout),
		http.StatusOK,
		41,
	)
	deleteActor(t, engine.endpoint, actor.ActorID)
}

func openGatewayWebSocket(
	t *testing.T,
	endpoint, actorID, label string,
	read bool,
) *gatewayWebSocket {
	t.Helper()
	client, response, err := dialGatewayWebSocket(endpoint, actorID, label, read)
	if err != nil {
		status := "<no response>"
		if response != nil {
			status = response.Status
			if response.Body != nil {
				_ = response.Body.Close()
			}
		}
		t.Fatalf("open gateway WebSocket %s: status=%s error=%v", label, status, err)
	}
	t.Cleanup(func() { client.close() })
	return client
}

func dialGatewayWebSocket(
	endpoint, actorID, label string,
	read bool,
) (*gatewayWebSocket, *http.Response, error) {
	return dialGatewayWebSocketContext(context.Background(), endpoint, actorID, label, read)
}

func dialGatewayWebSocketContext(
	ctx context.Context,
	endpoint, actorID, label string,
	read bool,
) (*gatewayWebSocket, *http.Response, error) {
	websocketURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("parse engine endpoint: %w", err)
	}
	switch websocketURL.Scheme {
	case "http":
		websocketURL.Scheme = "ws"
	case "https":
		websocketURL.Scheme = "wss"
	default:
		return nil, nil, fmt.Errorf("unsupported engine endpoint scheme %q", websocketURL.Scheme)
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
	conn, response, err := dialer.DialContext(ctx, websocketURL.String(), headers)
	if err != nil {
		return nil, response, err
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
	return client, response, nil
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
		var event actorConnectEventEnvelope
		if err := cbor.Unmarshal(frame.data, &event); err != nil {
			t.Fatalf("decode actor event: %v", err)
		}
		if event.Body.Tag != "Event" || event.Body.Value.Name != wantEvent || len(event.Body.Value.Args) != 1 {
			t.Fatalf("actor event = tag %q name %q with %d args, want Event/%q with one arg", event.Body.Tag, event.Body.Value.Name, len(event.Body.Value.Args), wantEvent)
		}
		var payload string
		if err := cbor.Unmarshal(event.Body.Value.Args[0], &payload); err != nil {
			t.Fatalf("decode actor event payload: %v", err)
		}
		return event.Body.Value.Name, payload
	case err := <-client.closed:
		t.Fatalf("WebSocket closed while waiting for actor event %q: %v", wantEvent, err)
	case <-time.After(websocketTestTimeout):
		t.Fatalf("timed out waiting for actor event %q", wantEvent)
	}
	return "", ""
}

func waitAnyActorEvent(t *testing.T, client *gatewayWebSocket) (string, string) {
	t.Helper()
	select {
	case frame := <-client.frames:
		if frame.kind != websocket.BinaryMessage {
			t.Fatalf("actor event frame kind = %d, want binary", frame.kind)
		}
		var event actorConnectEventEnvelope
		if err := cbor.Unmarshal(frame.data, &event); err != nil {
			t.Fatalf("decode actor event: %v", err)
		}
		if event.Body.Tag != "Event" || event.Body.Value.Name == "" || len(event.Body.Value.Args) != 1 {
			t.Fatalf("invalid actor event envelope: %#v", event)
		}
		var payload string
		if err := cbor.Unmarshal(event.Body.Value.Args[0], &payload); err != nil {
			t.Fatalf("decode actor event payload: %v", err)
		}
		return event.Body.Value.Name, payload
	case err := <-client.closed:
		t.Fatalf("WebSocket closed while waiting for actor event: %v", err)
	case <-time.After(websocketTestTimeout):
		t.Fatal("timed out waiting for actor event")
	}
	return "", ""
}

func assertGatewayWebSocketCloseWithoutReadLoop(
	t *testing.T,
	client *gatewayWebSocket,
	code int,
	reason string,
) {
	t.Helper()
	if err := client.conn.SetReadDeadline(time.Now().Add(websocketTestTimeout)); err != nil {
		t.Fatalf("set stalled WebSocket read deadline: %v", err)
	}
	for {
		_, _, err := client.conn.ReadMessage()
		if err == nil {
			continue
		}
		var closeError *websocket.CloseError
		if !errors.As(err, &closeError) || closeError.Code != code || closeError.Text != reason {
			t.Fatalf("stalled gateway WebSocket close = %v, want code %d reason %q", err, code, reason)
		}
		return
	}
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
		assertWebSocketCloseError(t, err, code, reason)
	case <-time.After(websocketTestTimeout):
		t.Fatalf("timed out waiting for gateway WebSocket close %d", code)
	}
}

// assertOnStopBroadcastAndClose verifies the pinned engine's stop behavior:
// the Go hook may enqueue its broadcast before the transport begins closing,
// but delivery is best-effort once the engine has started draining the actor.
// If the event is delivered, it must precede the documented close.
func assertOnStopBroadcastAndClose(
	t *testing.T,
	client *gatewayWebSocket,
	event, payload, reason string,
) {
	t.Helper()
	select {
	case frame := <-client.frames:
		if frame.kind != websocket.BinaryMessage {
			t.Fatalf("OnStop actor event frame kind = %d, want binary", frame.kind)
		}
		var envelope actorConnectEventEnvelope
		if err := cbor.Unmarshal(frame.data, &envelope); err != nil {
			t.Fatalf("decode OnStop actor event: %v", err)
		}
		if envelope.Body.Tag != "Event" || envelope.Body.Value.Name != event || len(envelope.Body.Value.Args) != 1 {
			t.Fatalf("OnStop actor event = %#v, want Event/%q with one arg", envelope, event)
		}
		var observedPayload string
		if err := cbor.Unmarshal(envelope.Body.Value.Args[0], &observedPayload); err != nil {
			t.Fatalf("decode OnStop actor event payload: %v", err)
		}
		if observedPayload != payload {
			t.Fatalf("OnStop actor event payload = %q, want %q", observedPayload, payload)
		}
		assertGatewayWebSocketClose(t, client, 1001, reason)
	case err := <-client.closed:
		assertWebSocketCloseError(t, err, 1001, reason)
	case <-time.After(websocketTestTimeout):
		t.Fatal("timed out waiting for OnStop actor event or gateway close")
	}
}

func assertWebSocketCloseError(t *testing.T, err error, code int, reason string) {
	t.Helper()
	var closeError *websocket.CloseError
	if !errors.As(err, &closeError) || closeError.Code != code || closeError.Text != reason {
		t.Fatalf("gateway WebSocket close = %v, want code %d reason %q", err, code, reason)
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

func waitWebSocketHookSet(
	t *testing.T,
	observations <-chan websocketHookObservation,
	labels ...string,
) map[string]websocketHookObservation {
	t.Helper()
	wanted := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		wanted[label] = struct{}{}
	}
	seen := make(map[string]websocketHookObservation, len(labels))
	deadline := time.After(websocketTestTimeout)
	for len(seen) < len(wanted) {
		select {
		case observation := <-observations:
			if _, ok := wanted[observation.label]; !ok {
				t.Fatalf("unexpected WebSocket hook label %q", observation.label)
			}
			if _, duplicate := seen[observation.label]; duplicate {
				t.Fatalf("duplicate WebSocket hook for %q", observation.label)
			}
			seen[observation.label] = observation
		case <-deadline:
			t.Fatalf("timed out waiting for WebSocket hooks %v", labels)
		}
	}
	return seen
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
