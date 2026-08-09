package rivet

import (
	"context"
	"errors"
	"time"
)

const managedDrainTimeout = 6 * time.Second

// RunContext is the serialized context for Actor.Run. Queue waits and
// KeepAwake work temporarily yield the actor turn so actions and lifecycle
// events can make progress while Run is blocked outside Go actor state.
type RunContext[T any] struct {
	*Context[T]
	queue *Queue
}

// Queue returns the Run-aware durable queue. Its blocking operations yield the
// actor turn and reacquire it before returning to user code.
func (c *RunContext[T]) Queue() *Queue {
	if c == nil {
		return nil
	}
	return c.queue
}

// KeepAwake registers the work with core and yields the Run actor turn while
// work executes. Copy any state-derived input before entering work and apply
// its result after work returns.
func (c *RunContext[T]) KeepAwake(
	ctx context.Context,
	work func(context.Context) error,
) error {
	if c == nil || c.Context == nil {
		return errors.New("actor Run context is unavailable")
	}
	return c.Context.keepAwake(ctx, work, c.Context.turnMu.Unlock, c.Context.turnMu.Lock)
}

// WaitUntil registers detached work with core's shutdown task tracker. The
// callback receives only a generation-scoped context: it must not capture or
// mutate actor state, connections, queue, DB, or KV. Use Run for serialized
// actor work. Admission is complete before WaitUntil returns. Callback errors
// and panics end that detached task without stopping the actor.
func (c *Context[T]) WaitUntil(
	ctx context.Context,
	work func(context.Context) error,
) error {
	if c == nil || c.session == nil {
		return errors.New("actor context is unavailable")
	}
	if ctx == nil {
		return errors.New("wait-until context is nil")
	}
	if work == nil {
		return errors.New("wait-until work is nil")
	}
	c.managedMu.Lock()
	defer c.managedMu.Unlock()
	if c.managedStopping {
		return ErrActorStopping
	}
	workID, err := c.session.BeginManagedWork(ctx, "wait_until")
	if err != nil {
		return queueError(err)
	}
	c.managedWG.Add(1)
	go func() {
		defer c.managedWG.Done()
		defer func() { _ = c.session.EndManagedWork(workID) }()
		defer func() { _ = recover() }()
		_ = work(c.managedCtx)
	}()
	return nil
}

// KeepAwake runs work synchronously while a core keep-awake region is held.
// The work is cancelled when its context is done; callers must cooperate with
// cancellation before the actor generation can finish shutdown.
func (c *Context[T]) KeepAwake(
	ctx context.Context,
	work func(context.Context) error,
) error {
	return c.keepAwake(ctx, work, nil, nil)
}

func (c *Context[T]) keepAwake(
	ctx context.Context,
	work func(context.Context) error,
	beforeWork func(),
	afterWork func(),
) (resultErr error) {
	if c == nil || c.session == nil {
		return errors.New("actor context is unavailable")
	}
	if ctx == nil {
		return errors.New("keep-awake context is nil")
	}
	if work == nil {
		return errors.New("keep-awake work is nil")
	}
	c.managedMu.Lock()
	if c.managedStopping {
		c.managedMu.Unlock()
		return ErrActorStopping
	}
	workID, err := c.session.BeginManagedWork(ctx, "keep_awake")
	c.managedMu.Unlock()
	if err != nil {
		return queueError(err)
	}
	if beforeWork != nil {
		beforeWork()
		if afterWork != nil {
			defer afterWork()
		}
	}
	defer func() {
		resultErr = errors.Join(resultErr, c.session.EndManagedWork(workID))
	}()
	return work(ctx)
}

func (c *Context[T]) drainManaged() error {
	if c == nil {
		return nil
	}
	c.stopManagedAdmission()
	done := make(chan struct{})
	go func() {
		c.managedWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(managedDrainTimeout):
		c.managedCancel()
		select {
		case <-done:
			return nil
		case <-time.After(time.Second):
			return errors.New("wait-until work did not stop after generation cancellation")
		}
	}
}

func (c *Context[T]) stopManagedAdmission() {
	if c == nil {
		return
	}
	c.managedMu.Lock()
	c.managedStopping = true
	c.managedMu.Unlock()
}
