package quasar

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/cond"
)

// TestWaitForRechecksOnContextError is the RT-13042 m25 regression test.
// When the wait channel closes at the same instant the context expires,
// select picks ctx.Done() at random; WaitFor must re-check the applied index
// before reporting a timeout so a wait that actually succeeded at the deadline
// is not reported as a failure.
func TestWaitForRechecksOnContextError(t *testing.T) {
	mu := &sync.Mutex{}
	fsm := &fsmWrapper{condM: mu, cond: cond.New(mu)}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- fsm.WaitFor(ctx, 5) }()

	// Let WaitFor reach WaitContext, which releases the mutex while it selects.
	time.Sleep(100 * time.Millisecond)

	// The condition becomes true at the same moment the wait is canceled:
	// advance lastApplied WITHOUT broadcasting (so the wait channel stays open
	// and WaitContext returns via ctx.Done()), then cancel. Pre-fix this is
	// reported as a timeout; the re-check must turn it into success.
	fsm.setLastApplied(5)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitFor should re-check and succeed when the condition was met at the deadline, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitFor did not return")
	}
}
