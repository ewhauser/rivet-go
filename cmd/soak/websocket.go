package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
)

type actorEventEnvelope struct {
	Body struct {
		Tag   string `cbor:"tag"`
		Value struct {
			Name string            `cbor:"name"`
			Args []cbor.RawMessage `cbor:"args"`
		} `cbor:"val"`
	} `cbor:"body"`
}

type wsClient struct {
	label  string
	conn   *websocket.Conn
	oracle *chatOracle
	errors *errorSink

	writeMu  sync.Mutex
	stateMu  sync.Mutex
	paused   bool
	wake     chan struct{}
	closing  bool
	expect   bool
	closeErr error
	done     chan struct{}
}

func dialWSClient(
	ctx context.Context,
	endpoint, actorID, label string,
	oracle *chatOracle,
	errorsSeen *errorSink,
) (*wsClient, error) {
	websocketURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse engine endpoint: %w", err)
	}
	if websocketURL.Scheme == "http" {
		websocketURL.Scheme = "ws"
	} else if websocketURL.Scheme == "https" {
		websocketURL.Scheme = "wss"
	} else {
		return nil, fmt.Errorf("unsupported engine endpoint scheme %q", websocketURL.Scheme)
	}
	websocketURL.Path = "/gateway/" + url.PathEscape(actorID) + "/websocket/soak"
	websocketURL.RawQuery = "client=" + url.QueryEscape(label)
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer dev")
	headers.Set("X-Client-Label", label)
	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
		Subprotocols: []string{
			"rivet",
			"rivet_target.actor",
			"rivet_actor." + actorID,
			"rivet_token.dev",
		},
	}
	conn, response, err := dialer.DialContext(ctx, websocketURL.String(), headers)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, fmt.Errorf("dial WebSocket client %s: %w", label, err)
	}
	client := &wsClient{
		label:  label,
		conn:   conn,
		oracle: oracle,
		errors: errorsSeen,
		wake:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	conn.SetReadLimit(1 << 20)
	// The dial context bounds only connection establishment. The read loop is
	// owned by the client and ends when close closes the socket; using a short
	// chaos-operation context here would silently stop receipt accounting.
	go client.readLoop(context.Background())
	return client, nil
}

func (c *wsClient) readLoop(ctx context.Context) {
	defer close(c.done)
	for {
		if err := c.waitReadable(ctx); err != nil {
			return
		}
		kind, data, err := c.conn.ReadMessage()
		if err != nil {
			c.stateMu.Lock()
			c.closeErr = err
			expected := c.closing || c.expect
			c.stateMu.Unlock()
			if !expected {
				c.errors.report(fmt.Errorf("WebSocket client %s closed unexpectedly: %w", c.label, err))
			}
			return
		}
		switch kind {
		case websocket.TextMessage:
			if !strings.HasPrefix(string(data), "ready:") {
				c.errors.report(fmt.Errorf("WebSocket client %s received unexpected text %q", c.label, data))
				return
			}
		case websocket.BinaryMessage:
			if err := c.recordBroadcast(data); err != nil {
				c.errors.report(err)
				return
			}
		default:
			c.errors.report(fmt.Errorf("WebSocket client %s received frame kind %d", c.label, kind))
			return
		}
	}
}

func (c *wsClient) waitReadable(ctx context.Context) error {
	for {
		c.stateMu.Lock()
		if !c.paused || c.closing {
			c.stateMu.Unlock()
			return nil
		}
		wake := c.wake
		c.stateMu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *wsClient) recordBroadcast(data []byte) error {
	var envelope actorEventEnvelope
	if err := cbor.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode broadcast for client %s: %w", c.label, err)
	}
	if envelope.Body.Tag != "Event" || envelope.Body.Value.Name != "chat" || len(envelope.Body.Value.Args) != 1 {
		return fmt.Errorf("client %s received unexpected broadcast envelope %#v", c.label, envelope)
	}
	var receipt chatReceipt
	if err := cbor.Unmarshal(envelope.Body.Value.Args[0], &receipt); err != nil {
		return fmt.Errorf("decode broadcast receipt for client %s: %w", c.label, err)
	}
	return c.oracle.record(c.label, receipt)
}

func (c *wsClient) write(token string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, []byte(token)); err != nil {
		return fmt.Errorf("write WebSocket client %s: %w", c.label, err)
	}
	return nil
}

func (c *wsClient) setPaused(paused bool) {
	c.stateMu.Lock()
	if c.paused && !paused {
		c.paused = false
		close(c.wake)
		c.wake = make(chan struct{})
	} else if !c.paused && paused {
		c.paused = true
	}
	c.stateMu.Unlock()
}

func (c *wsClient) close(ctx context.Context) error {
	c.stateMu.Lock()
	c.closing = true
	if c.paused {
		c.paused = false
		close(c.wake)
		c.wake = make(chan struct{})
	}
	c.stateMu.Unlock()
	c.writeMu.Lock()
	_ = c.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "soak client disconnect"),
		time.Now().Add(time.Second),
	)
	c.writeMu.Unlock()
	_ = c.conn.Close()
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *wsClient) expectServerClose() {
	c.stateMu.Lock()
	c.expect = true
	c.stateMu.Unlock()
}

func (c *wsClient) waitServerClose(ctx context.Context, code int, reason string) error {
	select {
	case <-c.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	var closeError *websocket.CloseError
	if !errors.As(c.closeErr, &closeError) {
		return fmt.Errorf("client %s close=%v, want WebSocket close %d %q", c.label, c.closeErr, code, reason)
	}
	if closeError.Code != code || closeError.Text != reason {
		return fmt.Errorf("client %s close=%d %q, want=%d %q", c.label, closeError.Code, closeError.Text, code, reason)
	}
	return nil
}

type clientManager struct {
	mu       sync.Mutex
	endpoint string
	actorID  string
	oracle   *chatOracle
	errors   *errorSink
	nextID   int
	clients  map[string]*wsClient
}

func newClientManager(endpoint, actorID string, oracle *chatOracle, errorsSeen *errorSink) *clientManager {
	return &clientManager{
		endpoint: endpoint,
		actorID:  actorID,
		oracle:   oracle,
		errors:   errorsSeen,
		clients:  make(map[string]*wsClient),
	}
}

func (m *clientManager) connect(ctx context.Context) (*wsClient, error) {
	m.mu.Lock()
	label := fmt.Sprintf("client-%06d", m.nextID)
	m.nextID++
	m.mu.Unlock()
	client, err := dialWSClient(ctx, m.endpoint, m.actorID, label, m.oracle, m.errors)
	if err != nil {
		return nil, err
	}
	if err := waitUntil(ctx, 50*time.Millisecond, func() (bool, error) {
		return m.oracle.connected(label), nil
	}); err != nil {
		_ = client.close(ctx)
		return nil, fmt.Errorf("wait for client %s OnConnect: %w", label, err)
	}
	if err := m.oracle.startExpecting(label); err != nil {
		_ = client.close(ctx)
		return nil, err
	}
	m.mu.Lock()
	m.clients[label] = client
	m.mu.Unlock()
	return client, nil
}

func (m *clientManager) labels() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	labels := make([]string, 0, len(m.clients))
	for label := range m.clients {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels
}

func (m *clientManager) client(label string) *wsClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[label]
}

func (m *clientManager) disconnectAndReplace(ctx context.Context, index int) error {
	labels := m.labels()
	if len(labels) == 0 {
		return errors.New("no WebSocket clients are available for disconnect")
	}
	label := labels[index%len(labels)]
	client := m.client(label)
	m.mu.Lock()
	delete(m.clients, label)
	m.mu.Unlock()
	m.oracle.stopExpecting(label)
	if err := waitUntil(ctx, 50*time.Millisecond, func() (bool, error) {
		err := m.oracle.convergence(label)
		return err == nil, err
	}); err != nil {
		return fmt.Errorf("drain client %s ledger before disconnect: %w", label, err)
	}
	if err := client.close(ctx); err != nil {
		return fmt.Errorf("disconnect client %s: %w", label, err)
	}
	if err := waitUntil(ctx, 50*time.Millisecond, func() (bool, error) {
		return !m.oracle.connected(label), nil
	}); err != nil {
		return fmt.Errorf("wait for client %s OnDisconnect: %w", label, err)
	}
	_, err := m.connect(ctx)
	return err
}

func (m *clientManager) stall(ctx context.Context, gate *workGate, index int, duration time.Duration) error {
	labels := m.labels()
	if len(labels) == 0 {
		return errors.New("no WebSocket clients are available to stall")
	}
	label := labels[index%len(labels)]
	client := m.client(label)
	before := m.oracle.expectedCount(label)
	client.setPaused(true)
	timer := time.NewTimer(duration)
	select {
	case <-timer.C:
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		client.setPaused(false)
		return ctx.Err()
	}
	if err := gate.pause(ctx); err != nil {
		client.setPaused(false)
		return err
	}
	defer gate.resume()
	client.setPaused(false)
	if err := waitUntil(ctx, 50*time.Millisecond, func() (bool, error) {
		after := m.oracle.expectedCount(label)
		if after <= before {
			return false, fmt.Errorf("client %s accumulated no broadcasts while stalled", label)
		}
		err := m.oracle.convergence(label)
		return err == nil, err
	}); err != nil {
		return err
	}
	return nil
}

func (m *clientManager) quiesce(ctx context.Context) error {
	for _, label := range m.labels() {
		m.oracle.stopExpecting(label)
	}
	return waitUntil(ctx, 50*time.Millisecond, func() (bool, error) {
		err := m.oracle.convergence("")
		return err == nil, err
	})
}

func (m *clientManager) closeAll(ctx context.Context) error {
	if err := m.quiesce(ctx); err != nil {
		return err
	}
	labels := m.labels()
	for _, label := range labels {
		client := m.client(label)
		if err := client.close(ctx); err != nil {
			return err
		}
		m.mu.Lock()
		delete(m.clients, label)
		m.mu.Unlock()
	}
	return nil
}

func (m *clientManager) prepareRunnerDrain(ctx context.Context) error {
	if err := m.quiesce(ctx); err != nil {
		return err
	}
	for _, label := range m.labels() {
		m.client(label).expectServerClose()
	}
	return nil
}

func (m *clientManager) waitRunnerDrain(ctx context.Context) error {
	labels := m.labels()
	for _, label := range labels {
		client := m.client(label)
		if err := client.waitServerClose(ctx, 1001, "runner shutting down"); err != nil {
			return err
		}
		m.mu.Lock()
		delete(m.clients, label)
		m.mu.Unlock()
	}
	return nil
}
