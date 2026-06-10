package cond_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/cond"
)

// TestWaitContextCancel covers RT-13042 m24: WaitContext must return the
// context error when the context is cancelled before a Broadcast, and must
// re-lock c.L on return.
func TestWaitContextCancel(t *testing.T) {
	var m sync.Mutex
	c := cond.New(&m)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m.Lock()
	err := c.WaitContext(ctx)
	m.Unlock() // WaitContext must have re-locked; this would panic otherwise.

	if err == nil {
		t.Fatal("WaitContext returned nil error on cancelled context")
	}
}

// TestBroadcastWaitRace covers RT-13042 m24: callers racing getWaitChan against
// Broadcast (the unenforced sync.Cond contract) must not nil-deref. Run with
// -race. Without the retry loop in getWaitChan this can panic.
func TestBroadcastWaitRace(t *testing.T) {
	var m sync.Mutex
	c := cond.New(&m)

	const (
		waiters = 16
		rounds  = 1000
	)
	done := make(chan struct{})

	// Hammer Broadcast in several goroutines to keep clearing the channel
	// pointer right after a losing CAS, maximizing the nil-deref window.
	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					c.Broadcast()
				}
			}
		}()
	}

	// Each round releases all waiters at once via a barrier so their
	// getWaitChan calls contend on the same CAS, producing losing CASes that
	// race the concurrent Broadcasts.
	for r := 0; r < rounds; r++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < waiters; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
				m.Lock()
				_ = c.WaitContext(ctx)
				m.Unlock()
				cancel()
			}()
		}
		close(start)
		wg.Wait()
	}
	close(done)
}
