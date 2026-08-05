package sqlitesocket

import (
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestHelloNegotiatesLocalFrameCeiling(t *testing.T) {
	e := encoder{data: []byte{byte(protocolVersion), byte(protocolVersion >> 8)}}
	e.uint(0)
	e.u32(maxSupportedFrame + 1024)

	got, err := decodeHello(e.data)
	if err != nil {
		t.Fatal(err)
	}
	if got != maxSupportedFrame {
		t.Fatalf("negotiated maxFrameBytes = %d, want %d", got, maxSupportedFrame)
	}
}

func TestReadLoopExitsWhenEndpointCloses(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	clientConn, endpointConn := net.Pipe()
	client := &Client{
		conn:     clientConn,
		maxFrame: defaultMaxFrame,
		pending:  make(map[uint32]chan response),
		closed:   make(chan struct{}),
	}
	go client.readLoop()

	if err := endpointConn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.closed:
	case <-time.After(time.Second):
		t.Fatal("socket read loop did not close after the endpoint closed")
	}
	goleak.VerifyNone(t, baseline)
}

func TestRequestIDsRemainUniqueAcrossUint32Wrap(t *testing.T) {
	client := &Client{pending: make(map[uint32]chan response)}
	client.nextID.Store(^uint32(0) - 1)
	client.pending[1] = make(chan response, 1)

	first := client.addPending(make(chan response, 1))
	second := client.addPending(make(chan response, 1))
	if first != ^uint32(0) || second != 2 {
		t.Fatalf("wrapped request IDs = (%d, %d), want (%d, 2)", first, second, ^uint32(0))
	}

	client = &Client{pending: make(map[uint32]chan response)}
	client.nextID.Store(^uint32(0) - 32)
	const callers = 128
	ids := make(chan uint32, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ids <- client.addPending(make(chan response, 1))
		}()
	}
	wait.Wait()
	close(ids)
	seen := make(map[uint32]struct{}, callers)
	for id := range ids {
		if id == 0 {
			t.Fatal("allocated reserved request ID zero")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("allocated duplicate request ID %d", id)
		}
		seen[id] = struct{}{}
	}
}
