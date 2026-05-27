package quasar

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// TestPruneExpiredServersCleansFollowerState is the RT-12926 regression test.
//
// Discovery bookkeeping and pruning used to be leader-only: pruneExpiredServers
// returned early on non-leaders, and the servers/applied maps are only cleaned
// from raft PeerObservations, which raft emits exclusively on the leader. So a
// node acting as a follower accumulated every peer it ever heard in its servers
// map and never dropped them. When that follower later won an election,
// addMissingServers re-AddNonvotered peers a previous leader had already pruned
// out — including pgwatchers that are fully down — restarting a doomed
// snapshot/heartbeat storm against them.
//
// The fix lets a non-leader still expire its *local* discovery bookkeeping on
// the prune ticker (it just skips the leader-only raft RemoveServer). This test
// drives a lone nonvoter — whose raft never leaves Follower state, so IsLeader()
// is deterministically false — seeds a stale peer into its discovery state, and
// asserts pruneExpiredServers removes it locally despite the node not being the
// leader.
func TestPruneExpiredServersCleansFollowerState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, tr := transports.NewInmemTransport("")
	c, err := NewKVCache(ctx,
		WithLocalID("follower"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
		WithAutoPrune(6*time.Second),
	)
	if err != nil {
		t.Fatalf("NewKVCache: %v", err)
	}
	defer func() { _ = c.Shutdown() }()

	// A lone nonvoter never campaigns, so it sits in Follower state.
	if c.IsLeader() {
		t.Fatal("lone nonvoter unexpectedly reports IsLeader() == true")
	}

	// Seed a peer into the follower's discovery bookkeeping, as a discovery
	// ping would. setServer records it in both the servers and lastSeen maps.
	ghost := raft.Server{ID: "ghost", Address: "ghost-addr", Suffrage: raft.Nonvoter}
	c.discovery.setServer(ghost)
	c.discovery.setApplied(ghost.ID)

	if !containsServer(c.discovery.getServers(), ghost.ID) {
		t.Fatal("precondition failed: ghost not present in discovery servers map")
	}

	// Prune with a cutoff in the future so every known peer counts as expired.
	// Before the fix this was a no-op on a non-leader; with the fix the follower
	// drops the peer from its local state (without touching the raft config).
	c.discovery.pruneExpiredServers(time.Now().Add(time.Hour))

	// deleteServer runs in pruneExpiredServers' goroutine, so poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if !containsServer(c.discovery.getServers(), ghost.ID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower did not expire the stale peer from its local discovery state")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if c.discovery.hasApplied(ghost.ID) {
		t.Fatal("follower did not clear the stale peer from its applied set")
	}
}

func containsServer(servers []raft.Server, id raft.ServerID) bool {
	for _, s := range servers {
		if s.ID == id {
			return true
		}
	}
	return false
}
