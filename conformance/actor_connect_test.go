package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
)

type actorConnectClient struct {
	conn         *websocket.Conn
	connectionID string
	nextActionID uint64
	writeMu      sync.Mutex
	events       []actorConnectMessage
}

type actorConnectMessage struct {
	tag       string
	actorID   string
	connID    string
	actionID  uint64
	output    cbor.RawMessage
	eventName string
	eventArgs []cbor.RawMessage
	errorCode string
	errorText string
}

type actorConnectAtomicState struct {
	Count int `json:"count"`
}

func (s actorConnectAtomicState) MarshalBinary() ([]byte, error) {
	return []byte{byte(s.Count)}, nil
}

func (s *actorConnectAtomicState) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		s.Count = 0
		return nil
	}
	if len(data) != 1 {
		return fmt.Errorf("actor state length = %d, want 1", len(data))
	}
	s.Count = int(data[0])
	return nil
}

type actorConnectOversizedState struct {
	Size int
}

func (s actorConnectOversizedState) MarshalBinary() ([]byte, error) {
	return bytes.Repeat([]byte{'s'}, s.Size), nil
}

func (s *actorConnectOversizedState) UnmarshalBinary(data []byte) error {
	s.Size = len(data)
	return nil
}

func openActorConnect(
	t *testing.T,
	endpoint, actorID string,
	parameters any,
) *actorConnectClient {
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
	websocketURL.Path = "/gateway/" + url.PathEscape(actorID) + "/connect"
	encodedParameters, err := json.Marshal(parameters)
	if err != nil {
		t.Fatalf("encode ActorConnect parameters: %v", err)
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: websocketTestTimeout,
		Subprotocols: []string{
			"rivet",
			"rivet_target.actor",
			"rivet_actor." + actorID,
			"rivet_encoding.cbor",
			"rivet_conn_params." + url.QueryEscape(string(encodedParameters)),
			"rivet_token.dev",
		},
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer dev")
	connection, response, err := dialer.Dial(websocketURL.String(), headers)
	if err != nil {
		status := "<no response>"
		if response != nil {
			status = response.Status
			if response.Body != nil {
				_ = response.Body.Close()
			}
		}
		t.Fatalf("open ActorConnect WebSocket: status=%s error=%v", status, err)
	}
	client := &actorConnectClient{conn: connection, nextActionID: 1}
	t.Cleanup(client.close)
	message := client.read(t)
	if message.tag != "Init" || message.actorID != actorID || message.connID == "" {
		client.close()
		t.Fatalf("ActorConnect Init = %#v", message)
	}
	client.connectionID = message.connID
	return client
}

func (c *actorConnectClient) subscribe(t *testing.T, eventName string) {
	t.Helper()
	c.write(t, map[string]any{
		"body": map[string]any{
			"tag": "SubscriptionRequest",
			"val": map[string]any{"eventName": eventName, "subscribe": true},
		},
	})
}

func actorConnectCall[T any](
	t *testing.T,
	client *actorConnectClient,
	action string,
	args ...any,
) T {
	t.Helper()
	var zero T
	actionID := client.nextActionID
	client.nextActionID++
	client.write(t, map[string]any{
		"body": map[string]any{
			"tag": "ActionRequest",
			"val": map[string]any{"id": actionID, "name": action, "args": args},
		},
	})
	for {
		message := client.read(t)
		switch message.tag {
		case "Event":
			client.events = append(client.events, message)
		case "ActionResponse":
			if message.actionID != actionID {
				t.Fatalf("ActorConnect response ID = %d, want %d", message.actionID, actionID)
			}
			if err := cbor.Unmarshal(message.output, &zero); err != nil {
				t.Fatalf("decode ActorConnect %s output: %v", action, err)
			}
			return zero
		case "Error":
			if message.actionID != actionID {
				t.Fatalf("ActorConnect error ID = %d, want %d", message.actionID, actionID)
			}
			t.Fatalf("ActorConnect %s failed: %s: %s", action, message.errorCode, message.errorText)
		default:
			t.Fatalf("unexpected ActorConnect message while calling %s: %#v", action, message)
		}
	}
}

func actorConnectCallError(
	t *testing.T,
	client *actorConnectClient,
	action string,
	args ...any,
) actorConnectMessage {
	t.Helper()
	actionID := client.nextActionID
	client.nextActionID++
	client.write(t, map[string]any{
		"body": map[string]any{
			"tag": "ActionRequest",
			"val": map[string]any{"id": actionID, "name": action, "args": args},
		},
	})
	for {
		message := client.read(t)
		switch message.tag {
		case "Event":
			client.events = append(client.events, message)
		case "Error":
			if message.actionID != actionID {
				t.Fatalf("ActorConnect error ID = %d, want %d", message.actionID, actionID)
			}
			return message
		case "ActionResponse":
			t.Fatalf("ActorConnect %s unexpectedly succeeded: %#v", action, message)
		default:
			t.Fatalf("unexpected ActorConnect message while calling %s: %#v", action, message)
		}
	}
}

func (c *actorConnectClient) nextEvent(t *testing.T, eventName string) []cbor.RawMessage {
	t.Helper()
	for index, event := range c.events {
		if event.eventName == eventName {
			c.events = append(c.events[:index], c.events[index+1:]...)
			return event.eventArgs
		}
	}
	for {
		message := c.read(t)
		if message.tag != "Event" {
			t.Fatalf("waiting for %s received %#v", eventName, message)
		}
		if message.eventName == eventName {
			return message.eventArgs
		}
		c.events = append(c.events, message)
	}
}

func (c *actorConnectClient) write(t *testing.T, value any) {
	t.Helper()
	payload, err := cbor.Marshal(value)
	if err != nil {
		t.Fatalf("encode ActorConnect message: %v", err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(websocketTestTimeout)); err != nil {
		t.Fatalf("set ActorConnect write deadline: %v", err)
	}
	if err := c.conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatalf("write ActorConnect message: %v", err)
	}
}

func (c *actorConnectClient) read(t *testing.T) actorConnectMessage {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(websocketTestTimeout)); err != nil {
		t.Fatalf("set ActorConnect read deadline: %v", err)
	}
	kind, payload, err := c.conn.ReadMessage()
	if err != nil {
		t.Fatalf("read ActorConnect message: %v", err)
	}
	if kind != websocket.BinaryMessage {
		t.Fatalf("ActorConnect message kind = %d, want binary", kind)
	}
	var envelope struct {
		Body cbor.RawMessage `cbor:"body"`
	}
	if err := cbor.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode ActorConnect envelope: %v", err)
	}
	var body struct {
		Tag   string          `cbor:"tag"`
		Value cbor.RawMessage `cbor:"val"`
	}
	if err := cbor.Unmarshal(envelope.Body, &body); err != nil {
		t.Fatalf("decode ActorConnect body: %v", err)
	}
	message := actorConnectMessage{tag: body.Tag}
	switch body.Tag {
	case "Init":
		var init struct {
			ActorID      string `cbor:"actorId"`
			ConnectionID string `cbor:"connectionId"`
		}
		if err := cbor.Unmarshal(body.Value, &init); err != nil {
			t.Fatalf("decode ActorConnect Init: %v", err)
		}
		message.actorID = init.ActorID
		message.connID = init.ConnectionID
	case "ActionResponse":
		var response struct {
			ID     uint64          `cbor:"id"`
			Output cbor.RawMessage `cbor:"output"`
		}
		if err := cbor.Unmarshal(body.Value, &response); err != nil {
			t.Fatalf("decode ActorConnect action response: %v", err)
		}
		message.actionID = response.ID
		message.output = response.Output
	case "Event":
		var event struct {
			Name string            `cbor:"name"`
			Args []cbor.RawMessage `cbor:"args"`
		}
		if err := cbor.Unmarshal(body.Value, &event); err != nil {
			t.Fatalf("decode ActorConnect event: %v", err)
		}
		message.eventName = event.Name
		message.eventArgs = event.Args
	case "Error":
		var response struct {
			ActionID *uint64 `cbor:"actionId"`
			Code     string  `cbor:"code"`
			Message  string  `cbor:"message"`
		}
		if err := cbor.Unmarshal(body.Value, &response); err != nil {
			t.Fatalf("decode ActorConnect error: %v", err)
		}
		if response.ActionID != nil {
			message.actionID = *response.ActionID
		}
		message.errorCode = response.Code
		message.errorText = response.Message
	default:
		t.Fatalf("unknown ActorConnect tag %q", body.Tag)
	}
	return message
}

func (c *actorConnectClient) close() {
	if c == nil || c.conn == nil {
		return
	}
	_ = c.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"),
		time.Now().Add(time.Second),
	)
	_ = c.conn.Close()
	c.conn = nil
}

func TestActorConnectRejectedStateDoesNotPersistActorMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
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
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "actor-connect-atomic", rivet.Actor[actorConnectAtomicState]{
		OnStart: func(ctx *rivet.Context[actorConnectAtomicState]) error {
			started <- observation{
				actorID: ctx.ActorID(), generation: ctx.Generation(), count: ctx.State().Count,
			}
			return nil
		},
		OnStop: func(ctx *rivet.Context[actorConnectAtomicState]) error {
			stopped <- observation{
				actorID: ctx.ActorID(), generation: ctx.Generation(), count: ctx.State().Count,
			}
			return nil
		},
		ConnectionState: rivet.NewConnectionState(func(
			*rivet.Context[actorConnectAtomicState], *rivet.Connection,
		) (actorConnectOversizedState, error) {
			return actorConnectOversizedState{Size: 1}, nil
		}),
		Actions: rivet.Actions[actorConnectAtomicState]{
			"overflow": rivet.Action(func(
				ctx *rivet.Context[actorConnectAtomicState], _ struct{},
			) (int, error) {
				ctx.State().Count++
				connectionState, stateErr := rivet.GetConnectionState[actorConnectOversizedState](
					ctx.CurrentConnection(),
				)
				if stateErr != nil {
					return 0, stateErr
				}
				connectionState.Size = (1 << 20) + 1
				return ctx.State().Count, nil
			}),
			"get": rivet.Action(func(
				ctx *rivet.Context[actorConnectAtomicState], _ struct{},
			) (int, error) {
				return ctx.State().Count, nil
			}),
		},
	}); err != nil {
		t.Fatalf("register ActorConnect atomicity actor: %v", err)
	}
	runnerName := fmt.Sprintf("rivet-go-actor-connect-atomic-%d", time.Now().UnixNano())
	served := startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "actor-connect-atomic", runnerName, "restart", nil, nil)

	var first observation
	select {
	case first = <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("ActorConnect atomicity actor did not start")
	}
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	client := openActorConnect(t, engine.endpoint, actor.ActorID, struct{}{})
	failure := actorConnectCallError(t, client, "overflow", struct{}{})
	if failure.errorCode != "connection_state_too_large" {
		t.Fatalf("ActorConnect overflow error = %#v, want connection_state_too_large", failure)
	}

	engine.kill(t)
	select {
	case stoppedActor := <-stopped:
		if stoppedActor.actorID != actor.ActorID || stoppedActor.generation != first.generation ||
			stoppedActor.count != 1 {
			t.Fatalf("pre-restart ActorConnect atomicity stop = %#v, first = %#v", stoppedActor, first)
		}
	case err := <-served.result:
		served.stopOnce.Do(func() {
			served.cancel()
			served.stopErr = err
		})
		t.Fatalf("runner exited while the engine was stopped: %v", err)
	case <-time.After(disconnectLivenessWindow + 10*time.Second):
		t.Fatal("ActorConnect atomicity actor did not stop after engine loss")
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
		t.Fatalf("runner exited before ActorConnect atomicity rehydration: %v", err)
	case <-time.After(rehydrateWindow):
		t.Fatal("ActorConnect atomicity actor was not rehydrated after engine restart")
	}
	if rehydrated.actorID != actor.ActorID || rehydrated.count != 0 ||
		rehydrated.generation <= first.generation {
		t.Fatalf("rehydrated ActorConnect atomicity actor = %#v, first = %#v", rehydrated, first)
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
		gatewayAction(t, engine.endpoint, actor.ActorID, "get", []any{struct{}{}}, 10*time.Second),
		http.StatusOK,
		0,
	)
	deleteActor(t, engine.endpoint, actor.ActorID)
}

func TestActorConnectConnectionContextPersistsAcrossSleep(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	type parameters struct {
		Label string `json:"label"`
	}
	type connectionState struct {
		Label string `json:"label"`
		Calls int    `json:"calls"`
	}
	type observation struct {
		ConnectionID string   `json:"connectionId"`
		Label        string   `json:"label"`
		Calls        int      `json:"calls"`
		Resumed      bool     `json:"resumed"`
		Connections  []string `json:"connections"`
	}
	type yieldObservation struct {
		RunConnectionID    string `json:"runConnectionId"`
		ActionConnectionID string `json:"actionConnectionId"`
	}
	connected := make(chan string, 4)
	disconnected := make(chan string, 4)
	scheduledCurrent := make(chan bool, 1)
	registry := rivet.NewRegistry()
	err = rivet.Register(registry, "actor-connect-state", rivet.Actor[struct{}]{
		HibernateWebSockets: true,
		Run: func(ctx context.Context, actor *rivet.RunContext[struct{}]) error {
			for {
				message, runErr := actor.Queue().Next(ctx, rivet.QueueNextOptions{
					Names: []string{"current-connection-probe"}, Completable: true,
				})
				if runErr != nil {
					return runErr
				}
				connectionID := ""
				if connection := actor.CurrentConnection(); connection != nil {
					connectionID = connection.ID()
				}
				if completeErr := message.Complete(ctx, connectionID); completeErr != nil {
					return completeErr
				}
			}
		},
		ConnectionState: rivet.NewConnectionState(func(
			_ *rivet.Context[struct{}], connection *rivet.Connection,
		) (connectionState, error) {
			decoded, decodeErr := rivet.DecodeConnectionParameters[parameters](connection)
			if decodeErr != nil {
				return connectionState{}, decodeErr
			}
			return connectionState{Label: decoded.Label}, nil
		}),
		OnActorConnect: func(_ *rivet.Context[struct{}], connection *rivet.Connection) error {
			if connection.CanHibernate() {
				connected <- connection.ID()
			}
			return nil
		},
		OnActorDisconnect: func(_ *rivet.Context[struct{}], connection *rivet.Connection) {
			if connection.CanHibernate() {
				disconnected <- connection.ID()
			}
		},
		Actions: rivet.Actions[struct{}]{
			"yieldCurrent": rivet.ActionWithContext(func(
				ctx context.Context,
				actor *rivet.Context[struct{}],
				_ struct{},
			) (yieldObservation, error) {
				response, queueErr := actor.Queue().SendAndWait(
					ctx, "current-connection-probe", struct{}{},
					rivet.QueueWaitOptions{Timeout: websocketTestTimeout},
				)
				if queueErr != nil {
					return yieldObservation{}, queueErr
				}
				var runConnectionID string
				if decodeErr := response.Decode(&runConnectionID); decodeErr != nil {
					return yieldObservation{}, decodeErr
				}
				actionConnectionID := ""
				if connection := actor.CurrentConnection(); connection != nil {
					actionConnectionID = connection.ID()
				}
				return yieldObservation{
					RunConnectionID: runConnectionID, ActionConnectionID: actionConnectionID,
				}, nil
			}),
			"observe": rivet.Action(func(ctx *rivet.Context[struct{}], _ struct{}) (observation, error) {
				connection := ctx.CurrentConnection()
				if connection == nil {
					return observation{}, rivet.ActionError{Code: "connection_required", Message: "missing calling connection"}
				}
				state, stateErr := rivet.GetConnectionState[connectionState](connection)
				if stateErr != nil {
					return observation{}, stateErr
				}
				state.Calls++
				connections := ctx.Connections()
				ids := make([]string, len(connections))
				for index, live := range connections {
					ids[index] = live.ID()
				}
				return observation{
					ConnectionID: connection.ID(), Label: state.Label, Calls: state.Calls,
					Resumed: connection.Resumed(), Connections: ids,
				}, nil
			}),
			"sleep": rivet.Action(func(ctx *rivet.Context[struct{}], _ struct{}) (bool, error) {
				if ctx.CurrentConnection() == nil {
					return false, rivet.ActionError{Code: "connection_required", Message: "missing calling connection"}
				}
				return true, ctx.Sleep()
			}),
			"hasCurrent": rivet.Action(func(ctx *rivet.Context[struct{}], _ struct{}) (bool, error) {
				hasCurrent := ctx.CurrentConnection() != nil
				scheduledCurrent <- hasCurrent
				return hasCurrent, nil
			}),
			"directCurrent": rivet.Action(func(ctx *rivet.Context[struct{}], _ struct{}) (bool, error) {
				return ctx.CurrentConnection() != nil, nil
			}),
			"scheduleConnectionless": rivet.Action(func(ctx *rivet.Context[struct{}], _ struct{}) (bool, error) {
				_, scheduleErr := ctx.Schedules().After(
					context.Background(), time.Millisecond, "hasCurrent", struct{}{},
				)
				return scheduleErr == nil, scheduleErr
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runnerName := fmt.Sprintf("rivet-go-actor-connect-%d", time.Now().UnixNano())
	startRegistry(t, engine, runnerName, registry)
	actor := createActor(t, engine.endpoint, "actor-connect-state", runnerName, "restart", nil, nil)
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.ConnectableTS != nil && actor.DestroyTS == nil
	})
	directCurrent := decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "directCurrent", []any{struct{}{}}, websocketTestTimeout,
	), http.StatusOK)
	if directCurrent {
		t.Fatal("gateway HTTP action reported an ActorConnect current connection")
	}

	scheduled := decodeActionOutput[bool](t, gatewayAction(
		t, engine.endpoint, actor.ActorID, "scheduleConnectionless", []any{struct{}{}}, websocketTestTimeout,
	), http.StatusOK)
	if !scheduled {
		t.Fatal("connectionless action was not scheduled")
	}
	select {
	case hasCurrent := <-scheduledCurrent:
		if hasCurrent {
			t.Fatal("scheduled action reported a current connection")
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("timed out waiting for scheduled connectionless action")
	}
	client := openActorConnect(t, engine.endpoint, actor.ActorID, parameters{Label: "Ada"})
	select {
	case connectionID := <-connected:
		if connectionID != client.connectionID {
			t.Fatalf("connect hook ID = %q, Init ID = %q", connectionID, client.connectionID)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("timed out waiting for ActorConnect hook")
	}
	first := actorConnectCall[observation](t, client, "observe", struct{}{})
	if first.ConnectionID != client.connectionID || first.Label != "Ada" || first.Calls != 1 ||
		first.Resumed || len(first.Connections) != 1 || first.Connections[0] != client.connectionID {
		t.Fatalf("first ActorConnect observation = %#v", first)
	}
	yielded := actorConnectCall[yieldObservation](t, client, "yieldCurrent", struct{}{})
	if yielded.RunConnectionID != "" {
		t.Fatalf("Actor.Run observed connected action caller %q", yielded.RunConnectionID)
	}
	if yielded.ActionConnectionID != client.connectionID {
		t.Fatalf("resumed action connection = %q, want %q", yielded.ActionConnectionID, client.connectionID)
	}
	if !actorConnectCall[bool](t, client, "sleep", struct{}{}) {
		t.Fatal("sleep action returned false")
	}
	waitForActor(t, engine.endpoint, actor.ActorID, false, func(actor actorRecord) bool {
		return actor.SleepTS != nil && actor.DestroyTS == nil
	})
	second := actorConnectCall[observation](t, client, "observe", struct{}{})
	if second.ConnectionID != client.connectionID || second.Label != "Ada" || second.Calls != 2 ||
		!second.Resumed || len(second.Connections) != 1 || second.Connections[0] != client.connectionID {
		t.Fatalf("restored ActorConnect observation = %#v", second)
	}
	client.close()
	select {
	case connectionID := <-disconnected:
		if connectionID != client.connectionID {
			t.Fatalf("disconnect hook ID = %q, want %q", connectionID, client.connectionID)
		}
	case <-time.After(websocketTestTimeout):
		t.Fatal("timed out waiting for ActorDisconnect hook")
	}
}
