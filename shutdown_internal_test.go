package quasar

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/transports"
	"go.uber.org/goleak"
)

// closeCountingTransport wraps a Transport and counts Close calls so a test
// can assert shutdown is not run twice.
type closeCountingTransport struct {
	transports.Transport
	closes atomic.Int32
}

func (t *closeCountingTransport) Close() error {
	t.closes.Add(1)
	if c, ok := t.Transport.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// TestShutdownClosesTransportOnce is the L6 regression test. Shutdown cancels
// the cache ctx — which makes the consume goroutine call shutdown — and then
// calls shutdown itself. Without the idempotency guard the transport Close()
// and raft.Shutdown() ran twice, concurrently. Close must run exactly once.
func TestShutdownClosesTransportOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, inmem := transports.NewInmemTransport("")
	tr := &closeCountingTransport{Transport: inmem}

	c, err := NewCache(ctx, &stubFSM{},
		WithLocalID("solo"),
		WithTransport(tr),
		WithBootstrap(true),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	if err := c.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// Give the consume goroutine time to observe ctx cancellation and take
	// its own shutdown path. A single shutdown body closes the transport
	// exactly once: raft holds a wrapper that hides Close() from it, so
	// shutdownFuture.Error() no longer closes it a second time (RT-14147).
	time.Sleep(200 * time.Millisecond)

	if got := tr.closes.Load(); got != 1 {
		t.Fatalf("expected the transport closed by a single shutdown body (1), got %d (shutdown ran more than once)", got)
	}
}

// TestShutdownLeavesNoGoroutines is the RT-14152 leak check. The equivalent
// checks in the NATS discovery tests were commented out because a bare
// goleak.VerifyNone also sees goroutines left by earlier tests; this one
// snapshots the ignore set before the cache exists. It runs on InmemTransport,
// so it needs no NATS server and runs on every machine.
func TestShutdownLeavesNoGoroutines(t *testing.T) {
	ignore := goleak.IgnoreCurrent()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, inmem := transports.NewInmemTransport("")

	c, err := NewCache(ctx, &stubFSM{},
		WithLocalID("solo"),
		WithTransport(inmem),
		WithBootstrap(true),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	if err := c.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	goleak.VerifyNone(t, ignore)
}

// TestWaitForLeaderReturnsAfterShutdown is the regression test for the
// unbounded recursion in waitForLeader's ctxRaft.Done() branch. That branch
// exists to re-observe the raft instance a rebuild installs, but after
// Shutdown no replacement is coming and ctxRaft stays done, so every call
// recursed immediately into the next one until the goroutine's stack blew
// (fatal error: stack overflow). Any Store still in flight when a node goes
// down reaches it — flare's best-effort cache fills and blacklist markers
// outlive the request that triggered them, so they hit this routinely.
func TestWaitForLeaderReturnsAfterShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, tr := transports.NewInmemTransport("")

	c, err := NewCache(ctx, &stubFSM{},
		WithLocalID("solo"),
		WithTransport(tr),
		WithBootstrap(true),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if err := c.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- c.waitForLeader(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("waitForLeader after shutdown: want an error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitForLeader did not return after shutdown")
	}
}
