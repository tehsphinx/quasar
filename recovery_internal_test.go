package quasar

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/transports"
)

// TestRecoverQuorumNoOpReturnsSentinel is the RT-13042 m6 regression test.
// When the configuration has no peer voters to drop, recoverQuorum used to
// return nil without swapping raft, so the watcher's success path assumed a
// swap and sat idle while still leaderless. recoverQuorum must now return
// errNoVotersToRecover so the watcher re-arms.
func TestRecoverQuorumNoOpReturnsSentinel(t *testing.T) {
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
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	// Single-voter configuration: nothing to drop.
	if err := c.recoverQuorum(); !errors.Is(err, errNoVotersToRecover) {
		t.Fatalf("expected errNoVotersToRecover for a single-voter cluster, got %v", err)
	}

	// recoverQuorum must not have torn raft down on the no-op path.
	if !c.IsLeader() {
		t.Fatal("expected the node to remain leader after a no-op recoverQuorum")
	}
}
