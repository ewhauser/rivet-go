package sqlitesocket

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

type observedWriteConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

type observedReadConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *observedReadConn) Read(data []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Read(data)
}

func (c *observedWriteConn) Write(data []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(data)
}

func newRequestTestClient(conn net.Conn) *Client {
	return &Client{
		conn:     conn,
		maxFrame: defaultMaxFrame,
		writeSem: make(chan struct{}, 1),
		pending:  make(map[uint32]chan response),
		closed:   make(chan struct{}),
	}
}

func readRequestIDForTest(t *testing.T, conn net.Conn) uint32 {
	t.Helper()
	payload, err := readFrame(conn, defaultMaxFrame)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := versionedDecoder(payload)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := decoder.uint()
	if err != nil || tag != 0 {
		t.Fatalf("ClientFrame tag = %d, err %v", tag, err)
	}
	id, err := decoder.u32()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func writeQueryResponseForTest(t *testing.T, conn net.Conn, id uint32, changes int64) {
	t.Helper()
	encoder := encoder{data: []byte{byte(protocolVersion), byte(protocolVersion >> 8)}}
	encoder.uint(0) // ServerFrame.Response
	encoder.u32(id)
	encoder.uint(1) // ResponsePayload.SqliteQueryOk
	encoder.uint(0) // columns
	encoder.uint(0) // rows
	encoder.i64(changes)
	encoder.data = append(encoder.data, 0) // lastInsertRowId
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(encoder.data)))
	if err := writeAll(conn, header[:]); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(conn, encoder.data); err != nil {
		t.Fatal(err)
	}
}

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

func TestHandshakeReadHonorsCancellationWithoutDeadline(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	clientConn, endpointConn := net.Pipe()
	observed := &observedReadConn{Conn: clientConn, started: make(chan struct{})}
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = endpointConn.Close()
		goleak.VerifyNone(t, baseline)
	})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := readFrameContext(ctx, observed, defaultMaxFrame)
		result <- err
	}()
	select {
	case <-observed.started:
	case <-time.After(time.Second):
		t.Fatal("handshake did not start its read")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handshake read error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake read ignored context cancellation")
	}

	payload := []byte{1, 2, 3}
	writeResult := make(chan error, 1)
	go func() {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		if err := writeAll(endpointConn, header[:]); err != nil {
			writeResult <- err
			return
		}
		writeResult <- writeAll(endpointConn, payload)
	}()
	got, err := readFrame(clientConn, defaultMaxFrame)
	if err != nil {
		t.Fatalf("read after canceled handshake: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload after canceled handshake = %v, want %v", got, payload)
	}
	if err := <-writeResult; err != nil {
		t.Fatalf("write after canceled handshake: %v", err)
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

func TestRequestWriteHonorsContextCancellation(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	clientConn, endpointConn := net.Pipe()
	observed := &observedWriteConn{Conn: clientConn, started: make(chan struct{})}
	client := newRequestTestClient(observed)
	t.Cleanup(func() {
		_ = client.Close()
		_ = endpointConn.Close()
		goleak.VerifyNone(t, baseline)
	})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Query(ctx, "SELECT 1", nil, nil)
		result <- err
	}()

	select {
	case <-observed.started:
	case <-time.After(time.Second):
		t.Fatal("socket request did not start its write")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Query error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked socket write ignored context cancellation")
	}
	select {
	case <-client.closed:
	case <-time.After(time.Second):
		t.Fatal("client remained open after a potentially partial frame")
	}
}

func TestQueuedWriteCancellationDoesNotCloseActiveConnection(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	clientConn, endpointConn := net.Pipe()
	observed := &observedWriteConn{Conn: clientConn, started: make(chan struct{})}
	client := newRequestTestClient(observed)
	t.Cleanup(func() {
		_ = client.Close()
		_ = endpointConn.Close()
		goleak.VerifyNone(t, baseline)
	})

	activeCtx, cancelActive := context.WithCancel(context.Background())
	activeResult := make(chan error, 1)
	go func() {
		_, err := client.Query(activeCtx, "SELECT 1", nil, nil)
		activeResult <- err
	}()
	select {
	case <-observed.started:
	case <-time.After(time.Second):
		t.Fatal("first socket request did not start its write")
	}

	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	cancelQueued()
	if _, err := client.Query(queuedCtx, "SELECT 2", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued Query error = %v, want context.Canceled", err)
	}
	select {
	case <-client.closed:
		t.Fatal("canceling a queued write closed the active connection")
	default:
	}

	cancelActive()
	select {
	case err := <-activeResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active Query error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active socket write ignored context cancellation")
	}
}

func TestAdmittedMutationWaitsForSocketSettlementAfterCancellation(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	clientConn, endpointConn := net.Pipe()
	client := newRequestTestClient(clientConn)
	go client.readLoop()
	t.Cleanup(func() {
		_ = client.Close()
		_ = endpointConn.Close()
		goleak.VerifyNone(t, baseline)
	})

	ctx, cancel := context.WithCancel(context.Background())
	callResult := make(chan error, 1)
	go func() {
		_, err := client.Exec(ctx, "INSERT INTO todos(label) VALUES ('once')", nil, nil)
		callResult <- err
	}()
	id := readRequestIDForTest(t, endpointConn)
	cancel()
	select {
	case err := <-callResult:
		t.Fatalf("mutation returned before socket settlement: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	writeQueryResponseForTest(t, endpointConn, id, 1)
	if err := <-callResult; err != nil {
		t.Fatalf("late successful mutation = %v, want success", err)
	}
}
