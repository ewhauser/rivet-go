package rivet

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/ewhauser/rivet-go/internal/wire"
	"github.com/fxamacker/cbor/v2"
)

const maxWebSocketMessage = 1 << 20

var errConnectionClosed = errors.New("rivet: WebSocket connection is closed")

// Message is one complete gateway WebSocket message. Text data is valid UTF-8;
// binary data is copied before the handler runs.
type Message struct {
	Data         []byte
	Binary       bool
	MessageIndex uint16
}

// Connection is one live raw gateway WebSocket. Methods are safe to call from
// handler-owned goroutines; native submission is serialized away from the
// polling goroutine.
type Connection struct {
	session      *pump.ActorSession
	id           string
	path         string
	headers      map[string]string
	canHibernate bool
	closed       atomic.Bool
	closeMu      sync.Mutex
	closeCode    *uint16
	closeReason  string
}

func newConnection(session *pump.ActorSession, event wire.Event) *Connection {
	return &Connection{
		session:      session,
		id:           event.WSID,
		path:         event.Path,
		headers:      cloneStringMap(event.Headers),
		canHibernate: event.CanHibernate,
	}
}

// ID is the core-assigned connection identifier.
func (c *Connection) ID() string {
	if c == nil {
		return ""
	}
	return c.id
}

// Path is the raw actor WebSocket request path, including its query string.
func (c *Connection) Path() string {
	if c == nil {
		return ""
	}
	return c.path
}

// Headers returns a copy of the WebSocket upgrade headers.
func (c *Connection) Headers() map[string]string {
	if c == nil {
		return nil
	}
	return cloneStringMap(c.headers)
}

// CanHibernate reports whether core can preserve this gateway connection
// while the actor sleeps. Hibernation is otherwise transparent to handlers.
func (c *Connection) CanHibernate() bool {
	return c != nil && c.canHibernate
}

// Send sends one text frame. Data must be valid UTF-8 and no larger than one
// MiB. Use SendBinary for a binary frame.
func (c *Connection) Send(data []byte) error {
	if !utf8.Valid(data) {
		return errors.New("WebSocket text data must be valid UTF-8")
	}
	return c.SendContext(context.Background(), data, false)
}

// SendText sends one UTF-8 text frame.
func (c *Connection) SendText(data string) error {
	return c.Send([]byte(data))
}

// SendBinary sends one binary frame.
func (c *Connection) SendBinary(data []byte) error {
	return c.SendContext(context.Background(), data, true)
}

// SendContext sends one complete frame and bounds native backpressure by ctx.
func (c *Connection) SendContext(ctx context.Context, data []byte, binary bool) error {
	if c == nil || c.session == nil {
		return errors.New("WebSocket connection is unavailable")
	}
	if ctx == nil {
		return errors.New("WebSocket send context is nil")
	}
	if c.closed.Load() {
		return errConnectionClosed
	}
	if len(data) > maxWebSocketMessage {
		return fmt.Errorf("WebSocket message is %d bytes, maximum is %d", len(data), maxWebSocketMessage)
	}
	if !binary && !utf8.Valid(data) {
		return errors.New("WebSocket text data must be valid UTF-8")
	}
	return c.session.SendWebSocket(ctx, c.id, data, binary)
}

// Close initiates a normal actor-side close with the supplied WebSocket code
// and UTF-8 reason.
func (c *Connection) Close(code uint16, reason string) error {
	return c.CloseContext(context.Background(), code, reason)
}

// CloseContext is Close with caller-controlled native backpressure bounds.
func (c *Connection) CloseContext(ctx context.Context, code uint16, reason string) error {
	if c == nil || c.session == nil {
		return errors.New("WebSocket connection is unavailable")
	}
	if ctx == nil {
		return errors.New("WebSocket close context is nil")
	}
	if c.closed.Load() {
		return errConnectionClosed
	}
	if !validCloseCode(code) {
		return fmt.Errorf("invalid WebSocket close code %d", code)
	}
	if !utf8.ValidString(reason) {
		return errors.New("WebSocket close reason must be valid UTF-8")
	}
	if len(reason) > 123 {
		return errors.New("WebSocket close reason exceeds 123 bytes")
	}
	if err := c.session.CloseWebSocket(ctx, c.id, code, reason); err != nil {
		return err
	}
	c.markClosed()
	return nil
}

// Closed reports whether the connection has closed or an actor-side close was
// accepted for submission.
func (c *Connection) Closed() bool { return c == nil || c.closed.Load() }

// CloseInfo returns the close code when one was supplied and the close reason.
func (c *Connection) CloseInfo() (*uint16, string) {
	if c == nil {
		return nil, ""
	}
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	var code *uint16
	if c.closeCode != nil {
		value := *c.closeCode
		code = &value
	}
	return code, c.closeReason
}

func (c *Connection) markClosed() { c.closed.Store(true) }

func (c *Connection) setClose(code *uint16, reason string) {
	c.closed.Store(true)
	c.closeMu.Lock()
	if code != nil {
		value := *code
		c.closeCode = &value
	}
	c.closeReason = reason
	c.closeMu.Unlock()
}

// Connections returns a snapshot of this actor generation's live raw
// WebSocket connections, sorted by connection ID. The returned slice can be
// modified by the caller; a connection in the snapshot may close afterward.
func (c *Context[T]) Connections() []*Connection {
	if c == nil {
		return nil
	}
	c.connectionsMu.Lock()
	connections := make([]*Connection, 0, len(c.connections))
	for _, connection := range c.connections {
		connections = append(connections, connection)
	}
	c.connectionsMu.Unlock()
	slices.SortFunc(connections, func(left, right *Connection) int {
		return strings.Compare(left.ID(), right.ID())
	})
	return connections
}

// Broadcast emits a named event with one typed payload to every live actor
// connection. Raw WebSocket clients receive the pinned actor-connect CBOR
// event envelope; actor-connect clients receive the same event through core's
// native subscription path.
func (c *Context[T]) Broadcast(event string, payload any) error {
	return c.broadcast(event, payload, nil)
}

// BroadcastExcept is Broadcast excluding one raw WebSocket connection.
func (c *Context[T]) BroadcastExcept(event string, payload any, connection *Connection) error {
	if connection == nil {
		return errors.New("excluded WebSocket connection is nil")
	}
	if c == nil || c.session == nil || connection.session != c.session {
		return errors.New("excluded WebSocket connection belongs to another actor generation")
	}
	id := connection.ID()
	return c.broadcast(event, payload, &id)
}

func (c *Context[T]) broadcast(event string, payload any, exclude *string) error {
	if c == nil || c.session == nil {
		return errors.New("actor context is unavailable")
	}
	if strings.TrimSpace(event) == "" {
		return errors.New("broadcast event must not be empty")
	}
	encoded, err := cbor.Marshal([]any{payload})
	if err != nil {
		return fmt.Errorf("encode broadcast payload: %w", err)
	}
	if len(encoded) > maxWebSocketMessage {
		return fmt.Errorf("broadcast payload is %d bytes, maximum is %d", len(encoded), maxWebSocketMessage)
	}
	return c.session.Broadcast(context.Background(), event, encoded, exclude)
}

func validCloseCode(code uint16) bool {
	return code >= 1000 && code <= 1003 || code >= 1007 && code <= 1014 || code >= 3000 && code <= 4999
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
