package rivet

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestActorKeySerdeMatchesRivetKit(t *testing.T) {
	tests := []struct {
		key       []string
		formatted string
	}{
		{[]string{}, "/"},
		{[]string{"test"}, "test"},
		{[]string{"a", "b", "c"}, "a/b/c"},
		{[]string{""}, `\0`},
		{[]string{"a", "", "b"}, `a/\0/b`},
		{[]string{"a/b", "/", "c/d"}, `a\/b/\//c\/d`},
		{[]string{`special\chars`, "more:complex,keys", "final key"}, `special\\chars/more:complex,keys/final key`},
		{[]string{`\0`, ""}, `\\0/\0`},
		{[]string{`abc\`}, `abc\\`},
	}
	for _, test := range tests {
		t.Run(test.formatted, func(t *testing.T) {
			if got := SerializeActorKey(test.key); got != test.formatted {
				t.Fatalf("SerializeActorKey(%#v) = %q, want %q", test.key, got, test.formatted)
			}
			if got := DeserializeActorKey(test.formatted); !reflect.DeepEqual(got, test.key) {
				t.Fatalf("DeserializeActorKey(%q) = %#v, want %#v", test.formatted, got, test.key)
			}
		})
	}
}

func TestClientCreateResolveAndCall(t *testing.T) {
	input := []byte{0, 1, 2, 255}
	connectable := int64(12)
	actorJSON := func(id, key string) map[string]any {
		return map[string]any{
			"actor_id":             id,
			"name":                 "counter",
			"key":                  key,
			"namespace_id":         "ns-id",
			"runner_name_selector": "runner-override",
			"create_ts":            10,
			"connectable_ts":       connectable,
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.URL.Query().Get("namespace"); got != "tenant" {
			t.Errorf("namespace = %q, want tenant", got)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q", got)
		}
		if got := request.Header.Get("X-Test"); got != "original" {
			t.Errorf("X-Test = %q, want cloned original", got)
		}
		writer.Header().Set("Content-Type", "application/json")

		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/base/actors":
			var body struct {
				Region             string  `json:"datacenter"`
				Name               string  `json:"name"`
				RunnerNameSelector string  `json:"runner_name_selector"`
				CrashPolicy        string  `json:"crash_policy"`
				Key                *string `json:"key"`
				Input              *string `json:"input"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode create body: %v", err)
			}
			if body.Region != "ord" || body.Name != "counter" || body.RunnerNameSelector != "runner-override" ||
				body.CrashPolicy != "restart" || body.Key == nil || *body.Key != `account\/west/\0` ||
				body.Input == nil || *body.Input != base64.StdEncoding.EncodeToString(input) {
				t.Errorf("create body = %#v", body)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"actor": actorJSON("actor-created", `account\/west/\0`)})

		case request.Method == http.MethodPut && request.URL.Path == "/base/actors":
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode get-or-create body: %v", err)
			}
			if body["key"] != "/" || body["crash_policy"] != "sleep" || body["runner_name_selector"] != "runner-default" {
				t.Errorf("get-or-create body = %#v", body)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"actor":   actorJSON("actor-keyed", "/"),
				"created": true,
			})

		case request.Method == http.MethodGet && request.URL.Path == "/base/actors":
			query := request.URL.Query()
			switch {
			case query.Get("actor_ids") == "actor-created":
				_ = json.NewEncoder(writer).Encode(map[string]any{"actors": []any{actorJSON("actor-created", "key")}})
			case query.Get("name") == "counter" && query.Get("key") == `account\/west/\0`:
				_ = json.NewEncoder(writer).Encode(map[string]any{"actors": []any{actorJSON("actor-keyed", `account\/west/\0`)}})
			default:
				t.Errorf("unexpected actor query: %s", request.URL.RawQuery)
				_ = json.NewEncoder(writer).Encode(map[string]any{"actors": []any{}})
			}

		case request.Method == http.MethodPost && request.URL.Path == "/base/gateway/actor-created/action/increment":
			var body struct {
				Args []map[string]int `json:"args"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode action body: %v", err)
			}
			if len(body.Args) != 1 || body.Args[0]["amount"] != 3 {
				t.Errorf("action body = %#v", body)
			}
			_, _ = writer.Write([]byte(`{"output":7}`))

		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.String())
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)

	headers := http.Header{"X-Test": []string{"original"}}
	client, err := NewClient(ClientConfig{
		Endpoint:   server.URL + "/base/",
		Namespace:  "tenant",
		RunnerName: "runner-default",
		Token:      "secret",
		Headers:    headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	headers.Set("X-Test", "mutated")

	created, err := client.Create(context.Background(), "counter", CreateOptions{
		Key:         []string{"account/west", ""},
		Input:       input,
		Region:      "ord",
		CrashPolicy: CrashPolicyRestart,
		RunnerName:  "runner-override",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID() != "actor-created" {
		t.Fatalf("created actor ID = %q", created.ID())
	}
	metadata := created.Metadata()
	if metadata.Name != "counter" || metadata.NamespaceID != "ns-id" || metadata.ConnectableTimestamp == nil ||
		*metadata.ConnectableTimestamp != connectable || !reflect.DeepEqual(metadata.Key, []string{"account/west", ""}) {
		t.Fatalf("created metadata = %#v", metadata)
	}
	metadata.Key[0] = "mutated"
	*metadata.ConnectableTimestamp = 999
	if got := created.Metadata(); got.Key[0] != "account/west" || *got.ConnectableTimestamp != connectable {
		t.Fatalf("metadata was not defensively copied: %#v", got)
	}

	resolved, err := client.Get(context.Background(), "actor-created")
	if err != nil || resolved.ID() != "actor-created" {
		t.Fatalf("Get = id %q, err %v", resolved.ID(), err)
	}
	byKey, err := client.GetByKey(context.Background(), "counter", []string{"account/west", ""})
	if err != nil || byKey.ID() != "actor-keyed" {
		t.Fatalf("GetByKey = id %q, err %v", byKey.ID(), err)
	}
	keyed, wasCreated, err := client.GetOrCreate(context.Background(), "counter", []string{}, CreateOptions{})
	if err != nil || !wasCreated || keyed.ID() != "actor-keyed" {
		t.Fatalf("GetOrCreate = id %q, created %v, err %v", keyed.ID(), wasCreated, err)
	}

	result, err := Call[int](context.Background(), created, "increment", map[string]int{"amount": 3})
	if err != nil || result != 7 {
		t.Fatalf("Call = %d, %v", result, err)
	}
}

func TestClientStructuredErrorsAndNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("X-Rivet-Ray-ID", "ray-1")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{
			"group":"actor",
			"code":"not_found",
			"message":"gone",
			"metadata":{"reason":"destroyed"},
			"actor":{"actorId":"actor-1","generation":4,"key":"key"}
		}`))
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(ClientConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Get(context.Background(), "actor-1")
	if !errors.Is(err, ErrActorNotFound) {
		t.Fatalf("Get error = %v, want ErrActorNotFound", err)
	}
	var clientError *ClientError
	if !errors.As(err, &clientError) {
		t.Fatalf("Get error = %T, want *ClientError", err)
	}
	if clientError.StatusCode != http.StatusConflict || clientError.Group != "actor" ||
		clientError.Code != "not_found" || clientError.Message != "gone" || clientError.RayID != "ray-1" ||
		clientError.Actor == nil || clientError.Actor.Generation != 4 || string(clientError.Metadata) != `{"reason":"destroyed"}` {
		t.Fatalf("structured error = %#v", clientError)
	}
}

func TestClientEmptyResolutionAndCancellation(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write([]byte(`{"actors":[]}`))
		}))
		t.Cleanup(server.Close)
		client, err := NewClient(ClientConfig{Endpoint: server.URL})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Get(context.Background(), "missing"); !errors.Is(err, ErrActorNotFound) {
			t.Fatalf("Get missing error = %v", err)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		entered := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			close(entered)
			<-request.Context().Done()
		}))
		t.Cleanup(server.Close)
		client, err := NewClient(ClientConfig{Endpoint: server.URL})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, requestErr := client.Get(ctx, "actor")
			done <- requestErr
		}()
		<-entered
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled Get error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("canceled Get did not return")
		}
	})
}

func TestActorHandleRawCallsSelfCallAndEscapedPath(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if got, want := request.URL.EscapedPath(), "/gateway/actor%2Fid/action/action%2Fname"; got != want {
			t.Errorf("escaped path = %q, want %q", got, want)
		}
		body := json.RawMessage{}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode raw call: %v", err)
		}
		if string(body) != `{"args":[1,{"raw":true}]}` {
			t.Errorf("raw call body = %s", body)
		}
		_, _ = writer.Write([]byte(`{"output":{"ok":true}}`))
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(ClientConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	handle := client.newHandle(actorMetadataJSON{ActorID: "actor/id"})
	output, err := handle.CallRaw(context.Background(), "action/name", json.RawMessage(`[1,{"raw":true}]`))
	if err != nil || string(output) != `{"ok":true}` {
		t.Fatalf("CallRaw = %s, %v", output, err)
	}
	if _, err := handle.CallRaw(context.Background(), "bad", json.RawMessage(`{"not":"array"}`)); err == nil {
		t.Fatal("CallRaw accepted a non-array argument document")
	}

	self := client.withSourceActor("actor/id").newHandle(actorMetadataJSON{ActorID: "actor/id"})
	if _, err := self.Call(context.Background(), "action", 1); !errors.Is(err, ErrSelfCall) {
		t.Fatalf("self Call error = %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("self call reached server; requests = %d", requests.Load())
	}
}

func TestClientConcurrentCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Args []int `json:"args"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || len(body.Args) != 1 {
			http.Error(writer, fmt.Sprintf("bad body: %#v, %v", body, err), http.StatusBadRequest)
			return
		}
		_, _ = fmt.Fprintf(writer, `{"output":%d}`, body.Args[0])
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(ClientConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	handle := client.newHandle(actorMetadataJSON{ActorID: "actor"})
	var wait sync.WaitGroup
	errorsChannel := make(chan error, 64)
	for index := range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, callErr := Call[int](context.Background(), handle, "echo", index)
			if callErr != nil || result != index {
				errorsChannel <- fmt.Errorf("call %d = %d, %v", index, result, callErr)
			}
		}()
	}
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Error(err)
	}
}

func TestNewClientValidation(t *testing.T) {
	for _, endpoint := range []string{"relative", "ftp://example.com", "http://example.com?query=1", "http://user@example.com"} {
		if _, err := NewClient(ClientConfig{Endpoint: endpoint}); err == nil {
			t.Errorf("NewClient accepted endpoint %q", endpoint)
		}
	}
	client, err := NewClient(ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if client.endpoint.String() != defaultEndpoint || client.namespace != defaultNamespace || client.runnerName != defaultRunnerName {
		t.Fatalf("client defaults = endpoint %s, namespace %s, runner %s", client.endpoint, client.namespace, client.runnerName)
	}
}
