package quasar

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// TestRecoverQuorumResetsFSMWhenNoSnapshot is the M2 regression test. When
// no snapshot exists yet, RecoverCluster re-applies the whole log on top of
// the live FSM's current state, double-applying every command. recoverQuorum
// must reset the FSM first so the full-log replay rebuilds state exactly.
// stubFSM counts Reset calls; pre-fix this stays 0.
func TestRecoverQuorumResetsFSMWhenNoSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	addr, tr := transports.NewInmemTransport("")
	fsm := &stubFSM{}
	c, err := NewCache(ctx, fsm,
		WithLocalID("self"),
		WithTransport(tr),
		// Two voters but only this node runs: the phantom peer is a voter
		// recoverQuorum will drop, so the recovery path actually executes.
		WithServers([]raft.Server{
			{ID: "self", Address: addr, Suffrage: raft.Voter},
			{ID: "phantom", Address: raft.ServerAddress("phantom:0"), Suffrage: raft.Voter},
		}),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	// Snapshot store must be empty for the replay-on-live-state path.
	snaps, err := c.snapshotStore.List()
	if err != nil {
		t.Fatalf("List snapshots: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected an empty snapshot store, got %d snapshots", len(snaps))
	}

	// RecoverCluster snapshots the FSM at the end, which stubFSM cannot do,
	// so recoverQuorum may return an error — but the FSM reset happens first,
	// which is what we assert.
	_ = c.recoverQuorum()

	if got := fsm.resets.Load(); got != 1 {
		t.Fatalf("expected the FSM to be reset once before log replay, got %d resets", got)
	}
}
