package quasar

import (
	"context"
	"runtime"
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

// TestPruneExpiredServersRemovesSilentConfigMember is the RT-12926 follow-up
// regression test.
//
// The first fix stopped a follower from re-AddNonvotering peers out of stale
// discovery state, but a peer that is a configured raft member yet never pings
// discovery (a slave-site pgwatcher that is down by design, or a stale
// random-UUID gate id) is absent from the ping-driven servers map. The
// servers-map-driven prune therefore never sees it, so the leader keeps a raft
// replication goroutine heartbeating it forever (the residual QA log:
// "failed to heartbeat ... peer=siteb-pgwatcher" at a parked backoff).
//
// The config-driven sweep added here reconciles the committed configuration on
// the leader: any configured member (voter or nonvoter) that has not pinged
// within the prune window is RemoveServer'd. This test bootstraps a lone voter
// (so it is leader), AddNonvoters a ghost that never pings, and asserts the
// leader removes it from the raft configuration.
func TestPruneExpiredServersRemovesSilentConfigMember(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, tr := transports.NewInmemTransport("")
	c, err := NewKVCache(ctx,
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
		WithAutoPrune(6*time.Second),
	)
	if err != nil {
		t.Fatalf("NewKVCache: %v", err)
	}
	defer func() { _ = c.Shutdown() }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if !c.IsLeader() {
		t.Fatal("bootstrapped lone voter did not become leader")
	}

	// Put a ghost into the committed raft configuration as a previous leader's
	// AddNonvoter would have, but never call setServer for it: it is a
	// configured member that does not ping discovery, so it is absent from the
	// servers/lastSeen maps -- exactly the down pgwatcher case.
	ghost := raft.Server{ID: "ghost", Address: "ghost-addr", Suffrage: raft.Nonvoter}
	if err := c.raft().AddNonvoter(ghost.ID, ghost.Address, 0, 0).Error(); err != nil {
		t.Fatalf("AddNonvoter(ghost): %v", err)
	}

	srvs, err := c.GetServerList()
	if err != nil {
		t.Fatalf("GetServerList: %v", err)
	}
	if !containsServer(srvs, ghost.ID) {
		t.Fatal("precondition failed: ghost not present in raft configuration")
	}

	// Prune with a cutoff in the future so the ghost counts as never-seen, and
	// far enough ahead that startedAt predates it (grace window satisfied).
	// Before the fix the leader only iterated the servers map -- which the
	// ghost is not in -- so this was a no-op; with the fix the config sweep
	// removes it from raft.
	c.discovery.pruneExpiredServers(time.Now().Add(time.Hour))

	// removeServer runs in pruneExpiredServers' goroutine, so poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for {
		srvs, err := c.GetServerList()
		if err != nil {
			t.Fatalf("GetServerList: %v", err)
		}
		if !containsServer(srvs, ghost.ID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader did not remove the silent config member from raft")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestGraceWindowSparesSilentConfigMember asserts the grace window guards a
// freshly (re)started leader: before a full prune window has elapsed, a
// configured member that has not pinged yet must NOT be removed (it may simply
// not have announced itself since the restart). pruneBefore is set just after
// startedAt, so the cutoff is in the past and graceElapsed is false.
func TestGraceWindowSparesSilentConfigMember(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, tr := transports.NewInmemTransport("")
	c, err := NewKVCache(ctx,
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
		WithAutoPrune(6*time.Second),
	)
	if err != nil {
		t.Fatalf("NewKVCache: %v", err)
	}
	defer func() { _ = c.Shutdown() }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	ghost := raft.Server{ID: "ghost", Address: "ghost-addr", Suffrage: raft.Nonvoter}
	if err := c.raft().AddNonvoter(ghost.ID, ghost.Address, 0, 0).Error(); err != nil {
		t.Fatalf("AddNonvoter(ghost): %v", err)
	}

	// Cutoff before startedAt, as the production ticker computes it right
	// after a (re)start (pruneBefore = now-pruneAfter, with now still close to
	// startedAt). graceElapsed is false, so the config sweep must not run and
	// the ghost must survive.
	started := time.Unix(0, c.discovery.startedAt.Load())
	c.discovery.pruneExpiredServers(started.Add(-time.Second))

	time.Sleep(200 * time.Millisecond)

	srvs, err := c.GetServerList()
	if err != nil {
		t.Fatalf("GetServerList: %v", err)
	}
	if !containsServer(srvs, ghost.ID) {
		t.Fatal("ghost was removed during the grace window; live-but-silent peers must be spared")
	}
}

// TestLocalPeerStatusReportsVotersOnEveryNode is the RT-13042 M1 regression
// test.
//
// NumVotersInConfig used to be counted from raft PeerObservation events, which
// raft emits only on the leader and only for non-self peers. So a follower
// reported 0 voters and even the leader of a 2-voter cluster reported 1 —
// the `NumVotersInConfig >= 2` arm of PeerStatus.inEstablishedCluster (the
// RT-12775 bootstrap gate for the leaderless case) could never fire on any
// node. The count is now re-derived from the raft configuration snapshot, so
// every node — leader or follower — must report the full voter count,
// self included.
func TestLocalPeerStatusReportsVotersOnEveryNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr1, tr1 := transports.NewInmemTransport("")
	addr2, tr2 := transports.NewInmemTransport("")
	tr1.Connect(addr2, tr2)
	tr2.Connect(addr1, tr1)

	servers := []raft.Server{
		{ID: "cache1", Address: addr1, Suffrage: raft.Voter},
		{ID: "cache2", Address: addr2, Suffrage: raft.Voter},
	}

	c1, err := NewKVCache(ctx,
		WithLocalID("cache1"),
		WithTransport(tr1),
		WithServers(servers),
	)
	if err != nil {
		t.Fatalf("NewKVCache(cache1): %v", err)
	}
	defer func() { _ = c1.Shutdown() }()

	c2, err := NewKVCache(ctx,
		WithLocalID("cache2"),
		WithTransport(tr2),
		WithServers(servers),
	)
	if err != nil {
		t.Fatalf("NewKVCache(cache2): %v", err)
	}
	defer func() { _ = c2.Shutdown() }()

	if err := c1.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady(cache1): %v", err)
	}
	if err := c2.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady(cache2): %v", err)
	}

	// The refresh runs in the observation goroutine, so poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for {
		n1 := c1.discovery.LocalPeerStatus().NumVotersInConfig
		n2 := c2.discovery.LocalPeerStatus().NumVotersInConfig
		if n1 == 2 && n2 == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected NumVotersInConfig == 2 on both nodes, got cache1=%d cache2=%d", n1, n2)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestDiscoveryRunDoesNotLeakPruningTickers is the RT-13042 M9 regression
// test. DiscoveryInjector.run is called on every raft (re)creation
// (newCache, reinitRaftAdoptingInstance, recoverQuorum) and used to start its
// pruning ticker on the cache-lifetime context: unlike the observer goroutine
// (cancelled via cancelPrevious), every rebuild leaked one more ticker
// goroutine running pruneExpiredServers concurrently for the life of the
// cache. Re-running discovery many times must not accumulate goroutines.
func TestDiscoveryRunDoesNotLeakPruningTickers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, tr := transports.NewInmemTransport("")
	c, err := NewKVCache(ctx,
		WithLocalID("node"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
		WithAutoPrune(time.Hour),
	)
	if err != nil {
		t.Fatalf("NewKVCache: %v", err)
	}
	defer func() { _ = c.Shutdown() }()

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	// Simulate 20 raft rebuilds re-registering discovery.
	const rebuilds = 20
	for i := 0; i < rebuilds; i++ {
		c.discovery.run(ctx, c.raft())
	}

	// Give superseded goroutines time to observe their cancelled contexts.
	time.Sleep(300 * time.Millisecond)
	got := runtime.NumGoroutine()

	// Only the latest observer + pruning ticker may remain; pre-fix this
	// leaks one pruning goroutine per rebuild.
	if diff := got - base; diff >= rebuilds/2 {
		t.Fatalf("discovery.run leaked %d goroutines across %d rebuilds", diff, rebuilds)
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
