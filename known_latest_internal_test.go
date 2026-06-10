package quasar

import (
	"context"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/transports"
)

// TestWaitForKnownLatestAnchorsOnCommitIndex is the M4 regression test. On a
// healthy single-voter leader every committed entry is applied, so
// WaitForKnownLatest must return promptly. The substance of M4 — that the
// anchor is the committed index and not LastIndex (which includes
// stored-but-uncommitted entries that may never commit after a leadership
// loss) — mirrors the localLastIndex/RT-13042 M10 reasoning; this guards the
// wiring so the call completes against the applied state.
func TestWaitForKnownLatestAnchorsOnCommitIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, tr := transports.NewInmemTransport("")
	c, err := NewKVCache(ctx,
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
	)
	if err != nil {
		t.Fatalf("NewKVCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	rft := c.raft()
	if commit, applied := rft.CommitIndex(), rft.AppliedIndex(); commit < applied {
		t.Fatalf("commit index %d behind applied index %d on a healthy leader", commit, applied)
	}

	inj := &FSMInjector{cache: c.Cache}
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	if err := inj.WaitForKnownLatest(waitCtx); err != nil {
		t.Fatalf("WaitForKnownLatest on a healthy leader: %v", err)
	}
}
