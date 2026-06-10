package quasar

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/transports"
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
	// twice — once explicitly and once via raft.Shutdown().Error() (raft's
	// shutdownFuture.Error() calls Close on the transport). The once-guard
	// keeps it at one body (2 closes) instead of two bodies (4).
	time.Sleep(200 * time.Millisecond)

	if got := tr.closes.Load(); got != 2 {
		t.Fatalf("expected the transport closed by a single shutdown body (2), got %d (shutdown ran more than once)", got)
	}
}
