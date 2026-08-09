package rivet

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/fxamacker/cbor/v2"
)

const (
	maxQueuePayload = 1 << 20
	maxQueueNames   = 1_024
)

var (
	ErrQueueFull            = errors.New("queue is full")
	ErrQueueMessageTooLarge = errors.New("queue message is too large")
	ErrQueueTimedOut        = errors.New("queue wait timed out")
	ErrActorAborted         = errors.New("actor generation was aborted")
	ErrActorStopping        = errors.New("actor generation is stopping")
)

// QueueNextOptions filters and controls one durable receive. A zero Timeout
// waits without a deadline; TryNext performs an immediate poll.
type QueueNextOptions struct {
	Names       []string
	Timeout     time.Duration
	Completable bool
}

// QueueWaitOptions controls SendAndWait. A zero Timeout waits without a
// deadline. The sending context always remains cancellable.
type QueueWaitOptions struct {
	Timeout time.Duration
}

// Queue exposes the actor's durable core queue.
type Queue struct {
	session    *pump.ActorSession
	beforeWait func()
	afterWait  func()
}

// QueueMessage is one durable record. Body is CBOR encoded. Completable
// messages remain in core until Complete succeeds. Retry releases a message
// for another receive; generation shutdown also releases unsettled messages.
type QueueMessage struct {
	ID        uint64
	Name      string
	Body      []byte
	CreatedAt time.Time

	queue       *Queue
	completable bool
	completeMu  sync.Mutex
	settled     bool
}

// QueueResponse is the optional CBOR response from SendAndWait. Present is
// false when the consumer completed the message without a response.
type QueueResponse struct {
	Present bool
	Data    []byte
}

func newQueue(session *pump.ActorSession) *Queue {
	return &Queue{session: session}
}

func (q *Queue) forRun(beforeWait, afterWait func()) *Queue {
	if q == nil {
		return nil
	}
	return &Queue{session: q.session, beforeWait: beforeWait, afterWait: afterWait}
}

// Send appends a typed CBOR value and waits for core to durably accept it.
func (q *Queue) Send(ctx context.Context, name string, body any) (*QueueMessage, error) {
	if q == nil || q.session == nil {
		return nil, errors.New("actor queue is unavailable")
	}
	encoded, err := encodeQueueValue(name, body)
	if err != nil {
		return nil, err
	}
	message, err := q.session.QueueSend(ctx, name, encoded)
	if err != nil {
		return nil, queueError(err)
	}
	return q.message(message), nil
}

// SendRaw appends an already encoded CBOR value.
func (q *Queue) SendRaw(ctx context.Context, name string, body []byte) (*QueueMessage, error) {
	if q == nil || q.session == nil {
		return nil, errors.New("actor queue is unavailable")
	}
	if err := validateQueueName(name); err != nil {
		return nil, err
	}
	if err := cbor.Valid(body); err != nil {
		return nil, fmt.Errorf("queue %q body is not valid CBOR: %w", name, err)
	}
	if len(body) > maxQueuePayload {
		return nil, fmt.Errorf("queue %q body is %d bytes, maximum is %d: %w", name, len(body), maxQueuePayload, ErrQueueMessageTooLarge)
	}
	message, err := q.session.QueueSend(ctx, name, body)
	if err != nil {
		return nil, queueError(err)
	}
	return q.message(message), nil
}

// SendAndWait appends a message and waits until a completable consumer replies.
func (q *Queue) SendAndWait(
	ctx context.Context,
	name string,
	body any,
	options QueueWaitOptions,
) (QueueResponse, error) {
	if q == nil || q.session == nil {
		return QueueResponse{}, errors.New("actor queue is unavailable")
	}
	encoded, err := encodeQueueValue(name, body)
	if err != nil {
		return QueueResponse{}, err
	}
	if options.Timeout < 0 {
		return QueueResponse{}, errors.New("queue wait timeout is negative")
	}
	var timeout *time.Duration
	if options.Timeout != 0 {
		timeout = &options.Timeout
	}
	q.yieldBeforeWait()
	response, present, err := q.session.QueueEnqueueWait(ctx, name, encoded, timeout)
	q.resumeAfterWait()
	if err != nil {
		return QueueResponse{}, queueError(err)
	}
	return QueueResponse{Present: present, Data: response}, nil
}

// Next waits for one matching message. A nil message with a nil error means
// the optional timeout elapsed.
func (q *Queue) Next(ctx context.Context, options QueueNextOptions) (*QueueMessage, error) {
	return q.next(ctx, options, false)
}

// TryNext performs an immediate receive.
func (q *Queue) TryNext(ctx context.Context, options QueueNextOptions) (*QueueMessage, error) {
	return q.next(ctx, options, true)
}

func (q *Queue) next(ctx context.Context, options QueueNextOptions, immediate bool) (*QueueMessage, error) {
	if q == nil || q.session == nil {
		return nil, errors.New("actor queue is unavailable")
	}
	if ctx == nil {
		return nil, errors.New("queue context is nil")
	}
	if options.Timeout < 0 {
		return nil, errors.New("queue timeout is negative")
	}
	names, err := normalizeQueueNames(options.Names)
	if err != nil {
		return nil, err
	}
	var timeout *time.Duration
	if immediate {
		zero := time.Duration(0)
		timeout = &zero
	} else if options.Timeout != 0 {
		timeout = &options.Timeout
	}
	q.yieldBeforeWait()
	message, err := q.session.QueueNext(ctx, names, timeout, options.Completable)
	q.resumeAfterWait()
	if err != nil {
		return nil, queueError(err)
	}
	if message == nil {
		return nil, nil
	}
	return q.message(*message), nil
}

func (q *Queue) message(message pump.QueueMessage) *QueueMessage {
	return &QueueMessage{
		ID: message.ID, Name: message.Name, Body: append([]byte(nil), message.Body...),
		CreatedAt: time.UnixMilli(message.CreatedAt), queue: q, completable: message.Completable,
	}
}

func (q *Queue) yieldBeforeWait() {
	if q != nil && q.beforeWait != nil {
		q.beforeWait()
	}
}

func (q *Queue) resumeAfterWait() {
	if q != nil && q.afterWait != nil {
		q.afterWait()
	}
}

// Completable reports whether Complete is valid for this message.
func (m *QueueMessage) Completable() bool { return m != nil && m.completable }

// DecodeBody decodes the message's CBOR value into destination.
func (m *QueueMessage) DecodeBody(destination any) error {
	if m == nil {
		return errors.New("queue message is nil")
	}
	if destination == nil {
		return errors.New("queue body destination is nil")
	}
	if err := cbor.Unmarshal(m.Body, destination); err != nil {
		return fmt.Errorf("decode queue %q body: %w", m.Name, err)
	}
	return nil
}

// Complete removes a completable message and optionally returns one CBOR
// response to SendAndWait. Pass no response or exactly one response.
func (m *QueueMessage) Complete(ctx context.Context, response ...any) error {
	if m == nil || m.queue == nil || m.queue.session == nil {
		return errors.New("queue message is unavailable")
	}
	if !m.completable {
		return errors.New("queue message was not received as completable")
	}
	if len(response) > 1 {
		return errors.New("queue Complete accepts at most one response")
	}
	var encoded *[]byte
	if len(response) == 1 {
		value, err := cbor.Marshal(response[0])
		if err != nil {
			return fmt.Errorf("encode queue completion response: %w", err)
		}
		if len(value) > maxQueuePayload {
			return fmt.Errorf("queue completion response is %d bytes, maximum is %d: %w", len(value), maxQueuePayload, ErrQueueMessageTooLarge)
		}
		encoded = &value
	}
	m.completeMu.Lock()
	defer m.completeMu.Unlock()
	if m.settled {
		return errors.New("queue message was already completed or retried")
	}
	if err := m.queue.session.QueueComplete(ctx, m.ID, encoded); err != nil {
		return queueError(err)
	}
	m.settled = true
	return nil
}

// Retry releases a completable message without removing it from the durable
// queue. A later Next call, including in a later generation, can receive it
// again.
func (m *QueueMessage) Retry(ctx context.Context) error {
	if m == nil || m.queue == nil || m.queue.session == nil {
		return errors.New("queue message is unavailable")
	}
	if !m.completable {
		return errors.New("queue message was not received as completable")
	}
	m.completeMu.Lock()
	defer m.completeMu.Unlock()
	if m.settled {
		return errors.New("queue message was already completed or retried")
	}
	if err := m.queue.session.QueueRetry(ctx, m.ID); err != nil {
		return queueError(err)
	}
	m.settled = true
	return nil
}

// Decode decodes a present SendAndWait response.
func (r QueueResponse) Decode(destination any) error {
	if !r.Present {
		return errors.New("queue response is absent")
	}
	if destination == nil {
		return errors.New("queue response destination is nil")
	}
	if err := cbor.Unmarshal(r.Data, destination); err != nil {
		return fmt.Errorf("decode queue response: %w", err)
	}
	return nil
}

func encodeQueueValue(name string, body any) ([]byte, error) {
	if err := validateQueueName(name); err != nil {
		return nil, err
	}
	encoded, err := cbor.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode queue %q body: %w", name, err)
	}
	if len(encoded) > maxQueuePayload {
		return nil, fmt.Errorf("queue %q body is %d bytes, maximum is %d: %w", name, len(encoded), maxQueuePayload, ErrQueueMessageTooLarge)
	}
	return encoded, nil
}

func normalizeQueueNames(names []string) ([]string, error) {
	if len(names) > maxQueueNames {
		return nil, fmt.Errorf("queue name filter contains %d names, maximum is %d", len(names), maxQueueNames)
	}
	seen := make(map[string]struct{}, len(names))
	normalized := make([]string, 0, len(names))
	for _, name := range names {
		if err := validateQueueName(name); err != nil {
			return nil, err
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized, nil
}

func validateQueueName(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("queue name must not be empty")
	}
	if len(name) > maxQueuePayload {
		return fmt.Errorf("queue name is %d bytes, maximum is %d", len(name), maxQueuePayload)
	}
	return nil
}

func queueError(err error) error {
	var handler pump.HandlerError
	if !errors.As(err, &handler) {
		return err
	}
	switch handler.Code {
	case "full":
		return fmt.Errorf("%s: %w", handler.Message, ErrQueueFull)
	case "message_too_large":
		return fmt.Errorf("%s: %w", handler.Message, ErrQueueMessageTooLarge)
	case "timed_out":
		return fmt.Errorf("%s: %w", handler.Message, ErrQueueTimedOut)
	case "aborted":
		return fmt.Errorf("%s: %w", handler.Message, ErrActorAborted)
	case "actor_stopping", "actor_generation_stale":
		return fmt.Errorf("%s: %w", handler.Message, ErrActorStopping)
	default:
		return err
	}
}
