package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The engine can answer 503 "Actor not found" for an intact sleeping actor
// while a replacement engine process is still failing over its workflow
// worker. gatewayActionEventually must ride out that window and still fail
// fast on genuine action errors.
func TestGatewayActionEventuallyRetries503(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte("Actor not found"))
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"output":{"armed":4,"fired":4,"lastNonce":"n","lastFiredNonce":"n"}}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state, err := gatewayActionEventually[alarmState](ctx, newEngineAPI(server.URL), "actor", "resleep", struct{}{})
	if err != nil {
		t.Fatalf("gatewayActionEventually: %v", err)
	}
	if state.Armed != 4 || state.Fired != 4 {
		t.Fatalf("state=%#v", state)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("engine calls=%d want=3", got)
	}
}

func TestGatewayActionEventuallyRetriesTransientActorCodes(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		switch calls.Add(1) {
		case 1:
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = writer.Write([]byte(`{"group":"actor","code":"stopping","message":"An internal error occurred"}`))
		case 2:
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = writer.Write([]byte(`{"group":"actor","code":"not_ready","message":"Actor is not ready."}`))
		default:
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{"output":{"armed":1,"fired":1,"lastNonce":"n","lastFiredNonce":"n"}}`))
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state, err := gatewayActionEventually[alarmState](ctx, newEngineAPI(server.URL), "actor", "resleep", struct{}{})
	if err != nil {
		t.Fatalf("gatewayActionEventually: %v", err)
	}
	if state.Armed != 1 || state.Fired != 1 {
		t.Fatalf("state=%#v", state)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("engine calls=%d want=3", got)
	}
}

func TestGatewayActionEventuallyFailsFastOnActionError(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = writer.Write([]byte(`{"group":"actor","code":"handler_panic","message":"boom"}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := gatewayActionEventually[alarmState](ctx, newEngineAPI(server.URL), "actor", "resleep", struct{}{})
	if err == nil || !strings.Contains(err.Error(), "actor/handler_panic") {
		t.Fatalf("err=%v, want actor/handler_panic", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("engine calls=%d want=1", got)
	}
}

// A 503 with any body other than the failover signature "Actor not found"
// is a terminal engine answer and must fail fast.
func TestGatewayActionEventuallyFailsFastOnOther503(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte("upstream connect error"))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := gatewayActionEventually[alarmState](ctx, newEngineAPI(server.URL), "actor", "resleep", struct{}{})
	if err == nil || !strings.Contains(err.Error(), "returned 503") {
		t.Fatalf("err=%v, want terminal 503", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("engine calls=%d want=1", got)
	}
}

// While the engine process is down entirely mid-replacement, transport
// errors retry until the budget expires and surface in the joined error.
func TestGatewayActionEventuallyRetriesUnreachableEngine(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	endpoint := "http://" + listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	_, err = gatewayActionEventually[alarmState](ctx, newEngineAPI(endpoint), "actor", "resleep", struct{}{})
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("err=%v, want deadline cause", err)
	}
	if !strings.Contains(err.Error(), "connect") {
		t.Fatalf("err=%v, want transport detail", err)
	}
}

func TestGatewayActionEventuallyReportsPersistent503(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte("Actor not found"))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	_, err := gatewayActionEventually[alarmState](ctx, newEngineAPI(server.URL), "actor", "resleep", struct{}{})
	if err == nil || !strings.Contains(err.Error(), "returned 503") {
		t.Fatalf("err=%v, want joined 503 detail", err)
	}
	if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("err=%v, want deadline cause", err)
	}
}
