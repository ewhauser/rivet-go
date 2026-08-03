// Package pump turns the blocking native event pump into Go subscriptions and
// a concurrent, batched submit API.
package pump

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ewhauser/rivet-go/internal/wire"
)

const (
	defaultPollTimeout     = 100 * time.Millisecond
	defaultShutdownTimeout = 10 * time.Second
	defaultSubmitQueue     = 1_024
	maxSubmitBatch         = 64
)

var (
	ErrAlreadyStarted = errors.New("pump already started")
	ErrNotStarted     = errors.New("pump is not running")
	ErrShuttingDown   = errors.New("pump is shutting down")
)

// Runner is implemented by the owned internal/ffi runner handle.
type Runner interface {
	Poll(time.Duration) ([]byte, error)
	Submit([]byte) error
	Shutdown(time.Duration) error
	Close()
}

type submitRequest struct {
	commands []wire.Command
	result   chan error
}

type subscriber struct {
	mu     sync.Mutex
	closed bool
	events chan wire.Event
}

// Subscription receives every event after it is created. Cancel is safe from
// any goroutine and closes Events.
type Subscription struct {
	Events <-chan wire.Event
	cancel func()
	once   sync.Once
}

func (s *Subscription) Cancel() {
	if s != nil && s.cancel != nil {
		s.once.Do(s.cancel)
	}
}

// Pump owns runner and closes it after RunnerStopped has been dispatched.
type Pump struct {
	runner          Runner
	pollTimeout     time.Duration
	shutdownTimeout time.Duration

	started      atomic.Bool
	shuttingDown atomic.Bool
	submitQueue  chan submitRequest
	submitStop   chan struct{}
	submitOnce   sync.Once
	submitWG     sync.WaitGroup
	done         chan struct{}

	subsMu    sync.Mutex
	subs      map[uint64]*subscriber
	nextSubID uint64
	resultMu  sync.Mutex
	result    error
	seenSeq   bool
	lastSeq   uint64
}

func New(runner Runner) *Pump {
	return &Pump{
		runner:          runner,
		pollTimeout:     defaultPollTimeout,
		shutdownTimeout: defaultShutdownTimeout,
		submitQueue:     make(chan submitRequest, defaultSubmitQueue),
		submitStop:      make(chan struct{}),
		done:            make(chan struct{}),
		subs:            make(map[uint64]*subscriber),
	}
}

// Subscribe registers a lossless event subscriber. A slow subscriber applies
// backpressure to polling, so callers should choose a buffer appropriate for
// their event handler and continue consuming until Events closes.
func (p *Pump) Subscribe(buffer int) *Subscription {
	if buffer < 0 {
		buffer = 0
	}
	sub := &subscriber{events: make(chan wire.Event, buffer)}
	p.subsMu.Lock()
	id := p.nextSubID
	p.nextSubID++
	p.subs[id] = sub
	p.subsMu.Unlock()
	return &Subscription{
		Events: sub.events,
		cancel: func() {
			p.subsMu.Lock()
			delete(p.subs, id)
			p.subsMu.Unlock()
			sub.close()
		},
	}
}

// Run starts exactly one native polling goroutine and blocks until clean
// shutdown or a fatal pump error.
func (p *Pump) Run(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	if p.runner == nil {
		return errors.New("pump runner is nil")
	}

	p.submitWG.Add(1)
	go p.submitLoop()
	go p.pollLoop(ctx)
	<-p.done

	p.resultMu.Lock()
	defer p.resultMu.Unlock()
	return p.result
}

// Submit batches commands from concurrent callers before crossing the FFI.
func (p *Pump) Submit(ctx context.Context, commands ...wire.Command) error {
	if !p.started.Load() {
		return ErrNotStarted
	}
	if p.shuttingDown.Load() {
		return ErrShuttingDown
	}
	request := submitRequest{
		commands: append([]wire.Command(nil), commands...),
		result:   make(chan error, 1),
	}
	select {
	case p.submitQueue <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.submitStop:
		return ErrShuttingDown
	case <-p.done:
		return ErrShuttingDown
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		select {
		case err := <-request.result:
			return err
		default:
			return ErrShuttingDown
		}
	}
}

func (p *Pump) pollLoop(ctx context.Context) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer func() {
		p.stopSubmissions()
		p.submitWG.Wait()
		p.runner.Close()
		p.closeSubscribers()
		close(p.done)
	}()

	shutdownRequested := false
	for {
		if !shutdownRequested {
			select {
			case <-ctx.Done():
				shutdownRequested = true
				p.shuttingDown.Store(true)
				p.stopSubmissions()
				p.submitWG.Wait()
				if err := p.runner.Shutdown(p.shutdownTimeout); err != nil {
					p.setResult(fmt.Errorf("shutdown native runner: %w", err))
					return
				}
			default:
			}
		}

		data, err := p.runner.Poll(p.pollTimeout)
		if err != nil {
			p.setResult(fmt.Errorf("poll native runner: %w", err))
			return
		}
		batch, err := wire.DecodeEventBatch(data)
		if err != nil {
			p.setResult(err)
			return
		}
		if p.seenSeq && batch.Seq <= p.lastSeq {
			p.setResult(fmt.Errorf(
				"non-monotonic event batch sequence: got %d after %d",
				batch.Seq,
				p.lastSeq,
			))
			return
		}
		p.seenSeq = true
		p.lastSeq = batch.Seq
		for _, event := range batch.Events {
			if !p.dispatch(event) {
				return
			}
			if event.Kind == wire.EventRunnerStopped {
				return
			}
		}
	}
}

func (p *Pump) submitLoop() {
	defer p.submitWG.Done()
	for {
		select {
		case <-p.submitStop:
			p.rejectQueuedSubmissions()
			return
		case first := <-p.submitQueue:
			requests := []submitRequest{first}
			commands := append([]wire.Command(nil), first.commands...)
		collect:
			for len(requests) < maxSubmitBatch {
				select {
				case request := <-p.submitQueue:
					requests = append(requests, request)
					commands = append(commands, request.commands...)
				default:
					break collect
				}
			}
			encoded, err := wire.EncodeCommandBatch(wire.CommandBatch{Commands: commands})
			if err == nil {
				err = p.runner.Submit(encoded)
			}
			for _, request := range requests {
				request.result <- err
			}
		}
	}
}

func (p *Pump) stopSubmissions() {
	p.submitOnce.Do(func() {
		p.shuttingDown.Store(true)
		close(p.submitStop)
	})
}

func (p *Pump) rejectQueuedSubmissions() {
	for {
		select {
		case request := <-p.submitQueue:
			request.result <- ErrShuttingDown
		default:
			return
		}
	}
}

func (p *Pump) dispatch(event wire.Event) bool {
	p.subsMu.Lock()
	subscribers := make([]*subscriber, 0, len(p.subs))
	for _, sub := range p.subs {
		subscribers = append(subscribers, sub)
	}
	p.subsMu.Unlock()
	for _, sub := range subscribers {
		if !sub.send(event, p.done) {
			return false
		}
	}
	return true
}

func (s *subscriber) send(event wire.Event, done <-chan struct{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return true
	}
	select {
	case s.events <- event:
		return true
	case <-done:
		return false
	}
}

func (s *subscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
}

func (p *Pump) closeSubscribers() {
	p.subsMu.Lock()
	subscribers := make([]*subscriber, 0, len(p.subs))
	for id, sub := range p.subs {
		delete(p.subs, id)
		subscribers = append(subscribers, sub)
	}
	p.subsMu.Unlock()
	for _, sub := range subscribers {
		sub.close()
	}
}

func (p *Pump) setResult(err error) {
	p.resultMu.Lock()
	if p.result == nil {
		p.result = err
	}
	p.resultMu.Unlock()
}
