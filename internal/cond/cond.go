// Package cond implements a lock-free broadcast primitive.
package cond

import (
	"sync/atomic"
)

// Cond wakes every goroutine waiting on it when Broadcast is called.
//
// Unlike sync.Cond it owns no lock. A waiter takes the channel returned by
// WaitChan and selects on it itself, so the broadcaster never blocks on a lock
// held by a waiter — the raft FSM goroutine broadcasts on every applied entry
// and must not be gated on client-held locks (RT-13896).
//
// The zero value is ready to use.
type Cond struct {
	ch atomic.Pointer[chan struct{}]
}

// New returns a new Cond.
func New() *Cond {
	return &Cond{}
}

// Broadcast wakes all goroutines waiting on c.
//
// The state change a waiter is waiting for must be published BEFORE calling
// Broadcast; see WaitChan for the other half of the ordering.
func (c *Cond) Broadcast() {
	ch := c.ch.Swap(nil)
	if ch != nil {
		close(*ch)
	}
}

// WaitChan returns a channel that the next Broadcast closes.
//
// The caller must re-check its condition AFTER calling WaitChan and before
// selecting on the returned channel. Together with publishing the state change
// before Broadcast, that makes the two orderings exhaustive: either the swap
// happened first — then this call installs a fresh channel and the re-check
// observes the new state — or this call happened first, and the Broadcast
// closes exactly this channel. Neither can be missed.
//
// The returned channel may also be one this call loaded an instant before a
// concurrent Broadcast swapped it out and closed it. That yields a spurious
// wake, never a lost one: the caller falls straight through its select and
// re-checks. Callers must therefore treat a wake as "re-evaluate", not as
// "the condition now holds".
func (c *Cond) WaitChan() <-chan struct{} {
	for {
		ch := c.ch.Load()
		if ch != nil {
			return *ch
		}

		chNew := make(chan struct{})
		if c.ch.CompareAndSwap(nil, &chNew) {
			return chNew
		}
		// another channel was set meanwhile (or a Broadcast cleared it again);
		// retry the load/CAS so we never dereference a nil pointer.
	}
}
