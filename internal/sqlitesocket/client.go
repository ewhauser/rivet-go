package sqlitesocket

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ewhauser/rivet-go/internal/wire"
)

type Result struct {
	Columns      []string
	Values       []wire.SQLiteValue
	RowsAffected int64
	LastInsertID *int64
}

type Client struct {
	conn      net.Conn
	maxFrame  uint32
	nextID    atomic.Uint32
	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[uint32]chan response
	closed    chan struct{}
	closeOnce sync.Once
}

func Dial(ctx context.Context, path string) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("SQLite socket dial context is nil")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("dial Actor Runtime Socket: %w", err)
	}
	client := &Client{
		conn:     conn,
		maxFrame: defaultMaxFrame,
		pending:  make(map[uint32]chan response),
		closed:   make(chan struct{}),
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("set Actor Runtime Socket handshake deadline: %w", err)
		}
	}
	if err := client.writeFrame(encodeHello()); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write Actor Runtime Socket hello: %w", err)
	}
	payload, err := readFrame(conn, defaultMaxFrame)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read Actor Runtime Socket hello: %w", err)
	}
	client.maxFrame, err = decodeHello(payload)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clear Actor Runtime Socket handshake deadline: %w", err)
	}
	go client.readLoop()
	return client, nil
}

func (c *Client) Exec(ctx context.Context, sql string, args []wire.SQLiteValue, leaseKey *string) (Result, error) {
	return c.request(ctx, request{kind: requestQuery, sql: sql, args: args, leaseKey: leaseKey})
}

func (c *Client) Query(ctx context.Context, sql string, args []wire.SQLiteValue, leaseKey *string) (Result, error) {
	return c.request(ctx, request{kind: requestQuery, sql: sql, args: args, leaseKey: leaseKey})
}

func (c *Client) Begin(ctx context.Context, leaseKey string, timeout time.Duration) error {
	timeoutMS := uint64(timeout.Milliseconds())
	if timeout > 0 && timeout%time.Millisecond != 0 {
		timeoutMS++
	}
	_, err := c.request(ctx, request{kind: requestBegin, leaseKey: &leaseKey, timeoutMS: &timeoutMS})
	return err
}

func (c *Client) Commit(ctx context.Context, leaseKey string) error {
	_, err := c.request(ctx, request{kind: requestCommit, leaseKey: &leaseKey})
	return err
}

func (c *Client) Rollback(ctx context.Context, leaseKey string) error {
	_, err := c.request(ctx, request{kind: requestRollback, leaseKey: &leaseKey})
	return err
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	var err error
	c.closeOnce.Do(func() {
		err = c.conn.Close()
		close(c.closed)
		c.failPending(errors.New("Actor Runtime Socket is closed"))
	})
	return err
}

func (c *Client) request(ctx context.Context, value request) (Result, error) {
	if c == nil {
		return Result{}, errors.New("Actor Runtime Socket client is nil")
	}
	if ctx == nil {
		return Result{}, errors.New("SQLite socket context is nil")
	}
	result := make(chan response, 1)
	value.id = c.addPending(result)
	payload, err := encodeRequest(value)
	if err != nil {
		c.removePending(value.id)
		return Result{}, err
	}
	if uint64(len(payload)) > uint64(c.maxFrame) {
		c.removePending(value.id)
		return Result{}, &wire.WireError{Code: "sqlite_request_too_large", Message: "SQLite request exceeds Actor Runtime Socket maxFrameBytes"}
	}
	if err := c.writeFrame(payload); err != nil {
		c.removePending(value.id)
		_ = c.Close()
		return Result{}, err
	}
	select {
	case response := <-result:
		if response.err != nil {
			return Result{}, response.err
		}
		return Result{
			Columns:      response.columns,
			Values:       response.values,
			RowsAffected: response.changes,
			LastInsertID: response.lastInsertID,
		}, nil
	case <-ctx.Done():
		c.abandonPending(value.id)
		return Result{}, ctx.Err()
	case <-c.closed:
		return Result{}, errors.New("Actor Runtime Socket is closed")
	}
}

func (c *Client) writeFrame(payload []byte) error {
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return errors.New("Actor Runtime Socket frame exceeds u32 length")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeAll(c.conn, header[:]); err != nil {
		return err
	}
	return writeAll(c.conn, payload)
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func (c *Client) readLoop() {
	for {
		payload, err := readFrame(c.conn, c.maxFrame)
		if err != nil {
			_ = c.Close()
			return
		}
		value, err := decodeResponse(payload)
		if err != nil {
			_ = c.Close()
			return
		}
		c.pendingMu.Lock()
		waiter, exists := c.pending[value.id]
		delete(c.pending, value.id)
		c.pendingMu.Unlock()
		if exists && waiter != nil {
			waiter <- value
		}
	}
}

func readFrame(reader io.Reader, maxFrame uint32) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length > maxFrame {
		return nil, fmt.Errorf("Actor Runtime Socket frame length %d exceeds maxFrameBytes %d", length, maxFrame)
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *Client) addPending(result chan response) uint32 {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for {
		id := c.nextID.Add(1)
		if id == 0 {
			continue
		}
		if _, exists := c.pending[id]; exists {
			continue
		}
		c.pending[id] = result
		return id
	}
}

func (c *Client) removePending(id uint32) {
	c.pendingMu.Lock()
	delete(c.pending, id)
	c.pendingMu.Unlock()
}

func (c *Client) abandonPending(id uint32) {
	c.pendingMu.Lock()
	if _, exists := c.pending[id]; exists {
		c.pending[id] = nil
	}
	c.pendingMu.Unlock()
}

func (c *Client) failPending(err error) {
	c.pendingMu.Lock()
	pending := c.pending
	c.pending = make(map[uint32]chan response)
	c.pendingMu.Unlock()
	for _, waiter := range pending {
		if waiter != nil {
			waiter <- response{err: err}
		}
	}
}
