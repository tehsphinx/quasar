package quasar

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/internal/cond"
)

// TestWaitForRechecksOnContextError is the RT-13042 m25 regression test.
// When the wait channel closes at the same instant the context expires,
// select picks ctx.Done() at random; WaitFor must re-check the applied index
// before reporting a timeout so a wait that actually succeeded at the deadline
// is not reported as a failure.
func TestWaitForRechecksOnContextError(t *testing.T) {
	fsm := &fsmWrapper{cond: cond.New()}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- fsm.WaitFor(ctx, 5) }()

	// Let WaitFor reach its select.
	time.Sleep(100 * time.Millisecond)

	// The condition becomes true at the same moment the wait is canceled:
	// advance lastApplied WITHOUT broadcasting (so the wait channel stays open
	// and the select returns via ctx.Done()), then cancel. Pre-fix this is
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

// TestWaitForWakesEveryWaiterUnderApplyStream is the RT-13896 regression test for
// the lock-free wake path: many waiters on staggered uids against a continuous
// apply stream must all be woken. Run with -race.
func TestWaitForWakesEveryWaiterUnderApplyStream(t *testing.T) {
	fsm := &fsmWrapper{cond: cond.New()}

	const (
		entries = 500
		waiters = 200
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Every waiter reports that it is about to enter WaitFor, and the apply
	// stream only starts once all of them have. Without the barrier the stream
	// can finish before a waiter ever calls WaitFor, which then satisfies the
	// entry check and returns having never touched the wake path — the test
	// would pass while testing less than its name claims.
	entered := make(chan struct{}, waiters)

	errs := make(chan error, waiters)
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		uid := uint64(i%entries) + 1
		wg.Add(1)
		go func() {
			defer wg.Done()

			entered <- struct{}{}
			if err := fsm.WaitFor(ctx, uid); err != nil {
				errs <- err
			}
		}()
	}

	for i := 0; i < waiters; i++ {
		<-entered
	}

	go func() {
		for uid := uint64(1); uid <= entries; uid++ {
			fsm.uidApplied(uid)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("not all waiters returned: a wakeup was lost")
	}

	close(errs)
	for err := range errs {
		t.Errorf("WaitFor: %v", err)
	}
}

// TestWaitForNoLostWakeupDuringRegistration races an apply against a waiter
// entering the wait. The window between the waiter's first lastApplied check and
// it installing the wait channel is exactly where a naive lock-free broadcast
// loses the wakeup; the waiter would then sleep until the next applied entry,
// which in this test never comes.
func TestWaitForNoLostWakeupDuringRegistration(t *testing.T) {
	const rounds = 2000

	for r := 0; r < rounds; r++ {
		fsm := &fsmWrapper{cond: cond.New()}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		start := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-start
			done <- fsm.WaitFor(ctx, 1)
		}()
		go func() {
			<-start
			fsm.uidApplied(1)
		}()
		close(start)

		err := <-done
		cancel()
		if err != nil {
			t.Fatalf("round %d: lost wakeup: %v", r, err)
		}
	}
}

// TestWaitForWokenByStoreConfiguration covers the second broadcast site: a
// configuration entry registered at commit time advances lastApplied and must
// wake waiters through the same lock-free path.
func TestWaitForWokenByStoreConfiguration(t *testing.T) {
	fsm := &fsmWrapper{cond: cond.New()}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- fsm.WaitFor(ctx, 1) }()

	time.Sleep(50 * time.Millisecond)
	fsm.regSystemUID(1)

	if err := <-done; err != nil {
		t.Fatalf("WaitFor was not woken by regSystemUID: %v", err)
	}
}
