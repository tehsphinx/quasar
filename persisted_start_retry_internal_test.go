// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

var errStartRefused = errors.New("JetStream system temporarily unavailable")

// failingStartTransport refuses the first `fail` StartPersistedConsumer calls
// with err, errStartRefused if unset, standing in for a JetStream 503 while the
// node becomes leader (RT-14526).
type failingStartTransport struct {
	*transports.InmemTransport

	err   error
	fail  atomic.Int64
	calls atomic.Int64
}

func (f *failingStartTransport) StartPersistedConsumer(ctx context.Context, apply transports.PersistedApplyFunc,
) (<-chan struct{}, error) {
	f.calls.Add(1)
	if f.fail.Add(-1) >= 0 {
		return nil, cmp.Or(f.err, errStartRefused)
	}
	return f.InmemTransport.StartPersistedConsumer(ctx, apply)
}

func newFailingStartLeaderCache(ctx context.Context, t *testing.T, fail int64, startErr error, opts ...Option,
) (*Cache, *failingStartTransport) {
	t.Helper()

	_, inner := transports.NewInmemTransport("")
	transports.ConnectInmemQueueHub(transports.NewInmemQueueHub(), inner)
	tr := &failingStartTransport{InmemTransport: inner, err: startErr}
	tr.fail.Store(fail)

	c, err := NewCache(ctx, &stubFSM{}, append([]Option{
		WithLocalID("solo"),
		WithTransport(tr),
		WithBootstrap(true),
	}, opts...)...)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if !c.IsLeader() {
		t.Fatal("lone bootstrapped voter is not leader")
	}
	return c, tr
}

// TestPersistedConsumerStartRetriedWhileLeader is the RT-14526 regression: a
// consumer start that fails while this node stays leader must be retried.
// Before the fix the failure was logged once and the queue was never drained
// again for the rest of the term, so this write waited out its deadline.
func TestPersistedConsumerStartRetriedWhileLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, tr := newFailingStartLeaderCache(ctx, t, 1, nil)

	storeCtx, storeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer storeCancel()
	if _, err := c.store(storeCtx, "key", []byte("v")); err != nil {
		t.Fatalf("store after a failed consumer start: %v (start calls %d)", err, tr.calls.Load())
	}
	if n := tr.calls.Load(); n < 2 {
		t.Fatalf("start calls = %d, want >= 2", n)
	}
}

// TestPersistedConsumerStartRetryBacksOff pins the retry cadence under a
// sustained start failure: a single retry chain with a growing delay, not a
// fixed-rate loop or one chain per leadership observation.
func TestPersistedConsumerStartRetryBacksOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, tr := newFailingStartLeaderCache(ctx, t, 1<<30, nil)

	// Attempts land at ~0, 0.5s, 1.5s and 3.5s; a fixed 500ms loop would be
	// at 7 by now.
	time.Sleep(3 * time.Second)
	if n := tr.calls.Load(); n < 2 || n > 4 {
		t.Fatalf("start calls after 3s = %d, want 2..4", n)
	}
}

// TestPersistedConsumerStartLeadershipLossLoggedAsWarning: a start that fails
// because leadership moved, the way a barrier answered with ErrLeadershipLost
// does on step-down, is routine. It is retried like any failed start, since a
// failed leadership transfer emits no observation that would, but it is logged
// as a warning, not an error (RT-14687).
func TestPersistedConsumerStartLeadershipLossLoggedAsWarning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out := &syncBuffer{}
	logger := hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})
	_, tr := newFailingStartLeaderCache(ctx, t, 1, fmt.Errorf("barrier: %w", raft.ErrLeadershipLost),
		WithHclogLogger(logger))

	if !eventually(ctx, func() bool { return tr.calls.Load() >= 2 }) {
		t.Fatal("failed start never retried")
	}
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.Contains(line, "failed to start persisted consumer") {
			if !strings.Contains(line, "[WARN]") {
				t.Fatalf("leadership loss logged above warning:\n%s", line)
			}
			return
		}
	}
	t.Fatalf("failed start not logged:\n%s", out.String())
}
