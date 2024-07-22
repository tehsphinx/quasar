package cond

import (
	"context"
	"sync"
	"sync/atomic"
)

// Cond is like sync.Cond with a WaitContext method.
type Cond struct {
	L sync.Locker

	ch atomic.Pointer[chan struct{}]
}

// New returns a new Cond with Locker l.
func New(l sync.Locker) *Cond {
	return &Cond{
		L: l,
	}
}

// Broadcast wakes all goroutines waiting on c.
//
// It is allowed but not required for the caller to hold c.L
// during the call.
func (c *Cond) Broadcast() {
	ch := c.ch.Swap(nil)
	if ch != nil {
		close(*ch)
	}
}

// Wait atomically unlocks c.L and suspends execution
// of the calling goroutine. After later resuming execution,
// Wait locks c.L before returning. Unlike in other systems,
// Wait cannot return unless awoken by Broadcast.
//
// Because c.L is not locked while Wait is waiting, the caller
// typically cannot assume that the condition is true when
// Wait returns. Instead, the caller should Wait in a loop:
//
//	c.L.Lock()
//	for !condition() {
//	    c.Wait()
//	}
//	... make use of condition ...
//	c.L.Unlock()
func (c *Cond) Wait() {
	_ = c.WaitContext(context.Background())
}

// WaitContext is like Wait, but aborts if the given context is cancelled.
// A non-nil error is returned iff the context was cancelled.
func (c *Cond) WaitContext(ctx context.Context) error {
	ch := c.getWaitChan()

	c.L.Unlock()
	defer c.L.Lock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Cond) getWaitChan() chan struct{} {
	ch := c.ch.Load()
	if ch == nil {
		chNew := make(chan struct{})
		if c.ch.CompareAndSwap(nil, &chNew) {
			ch = &chNew
		} else {
			// another channel was set meanwhile, load and use.
			ch = c.ch.Load()
		}
	}

	return *ch
}
