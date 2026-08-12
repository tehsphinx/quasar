package cond_test

import (
	"sync"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/internal/cond"
)

func TestBroadcastWakesEveryWaiter(t *testing.T) {
	c := cond.New()

	const waiters = 200
	var wg sync.WaitGroup
	ready := make(chan struct{}, waiters)

	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ch := c.WaitChan()
			ready <- struct{}{}
			<-ch
		}()
	}
	for i := 0; i < waiters; i++ {
		<-ready
	}

	c.Broadcast()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Broadcast did not wake all waiters")
	}
}

func TestBroadcastBeforeWaitChanIsNotMissed(t *testing.T) {
	c := cond.New()

	// A Broadcast with no waiter installed must leave no closed channel behind:
	// the next WaitChan has to hand out a fresh, open one.
	c.Broadcast()

	select {
	case <-c.WaitChan():
		t.Fatal("WaitChan returned an already-closed channel")
	default:
	}
}

// TestBroadcastWaitChanRace covers RT-13042 m24: callers racing WaitChan against
// Broadcast must not nil-deref. Run with -race. Without the retry loop in
// WaitChan this can panic.
func TestBroadcastWaitChanRace(t *testing.T) {
	c := cond.New()

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

	// Each round releases all waiters at once via a barrier so their WaitChan
	// calls contend on the same CAS, producing losing CASes that race the
	// concurrent Broadcasts.
	for r := 0; r < rounds; r++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < waiters; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				<-start
				timer := time.NewTimer(time.Microsecond)
				defer timer.Stop()

				select {
				case <-c.WaitChan():
				case <-timer.C:
				}
			}()
		}
		close(start)
		wg.Wait()
	}
	close(done)
}
