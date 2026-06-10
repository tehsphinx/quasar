package quasar

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// TestProbeVotersSelfQuorum is the M1 regression test. A single-voter
// configuration (self only) trivially satisfies quorum, but probeVoters
// used to only check alive >= quorum inside the result-receive loop, which
// never runs when there are no peers to probe. The function must report
// reachable up front so getLeaderWait does not fail with ErrNoLeader.
func TestProbeVotersSelfQuorum(t *testing.T) {
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

	voters := []raft.Server{{ID: raft.ServerID(c.localID), Suffrage: raft.Voter}}
	if !c.probeVoters(ctx, 100*time.Millisecond, voters) {
		t.Fatal("expected a single-voter (self) set to satisfy quorum")
	}
}
