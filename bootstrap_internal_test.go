package quasar

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// syncBuffer is a goroutine-safe io.Writer so raft's background logging
// goroutines and the test goroutine can share one log sink without racing.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestBootstrapClusterLogsFutureError is the RT-13042 m2 regression test.
// BootstrapCluster futures used to be discarded, silently swallowing failures.
// A second bootstrap of an already-bootstrapped raft fails with
// raft.ErrCantBootstrap and must now be logged.
func TestBootstrapClusterLogsFutureError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out := &syncBuffer{}
	logger := hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, newSnapFSM(),
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
		WithHclogLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	// Already bootstrapped: this second attempt fails and must be logged.
	c.bootstrapCluster(c.raft(), raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID(c.localID), Address: tr.LocalAddr()}},
	})

	if !strings.Contains(out.String(), "bootstrap cluster failed") {
		t.Fatalf("expected a bootstrap-failure log, got:\n%s", out.String())
	}
}
