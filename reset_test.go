package quasar_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/matryer/is"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/examples/generic/exampleFSM"
	"github.com/tehsphinx/quasar/transports"
)

// resetNode bundles a cache with its FSM for the reset tests.
type resetNode struct {
	id    string
	cache *quasar.Cache
	fsm   *exampleFSM.InMemoryFSM
}

// allCleared returns nil only when none of the named musicians are present
// in the local FSM.
func allCleared(fsm *exampleFSM.InMemoryFSM, names ...string) error {
	for _, name := range names {
		if _, err := fsm.GetMusicianLocal(name); err == nil {
			return fmt.Errorf("musician %s still present after reset", name)
		}
	}
	return nil
}

// hasAll returns nil only when all named musicians are present in the local FSM.
func hasAll(fsm *exampleFSM.InMemoryFSM, names ...string) error {
	for _, name := range names {
		if _, err := fsm.GetMusicianLocal(name); err != nil {
			return fmt.Errorf("musician %s not present yet", name)
		}
	}
	return nil
}

func writeMusician(ctx context.Context, fsm *exampleFSM.InMemoryFSM, name string) error {
	return fsm.SetMusician(ctx, exampleFSM.Musician{Name: name, Age: 30, Instruments: []string{"kazoo"}})
}

// TestResetReplicatesAndClearsLateJoiner exercises the RT-12994 redesign:
// Reset is now a replicated raft-log command rather than an out-of-band
// per-node RPC. As a committed entry it reaches every current member, and
// any node that joins afterwards is brought up in the post-reset state via
// normal replication — so the old getLastResetID re-send hack is no longer
// needed. The reset here is issued on a nonvoter to also exercise the
// forward-to-leader path.
func TestResetReplicatesAndClearsLateJoiner(t *testing.T) {
	const discoveryTick = 200 * time.Millisecond

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelMain()

	asrt := is.New(t)
	bus := newInmemBus()

	specs := []string{"v1", "n2", "n3"}
	addrs := map[string]raft.ServerAddress{}
	trs := map[string]*transports.InmemTransport{}
	for _, id := range specs {
		addr, tr := transports.NewInmemTransport("")
		addrs[id] = addr
		trs[id] = tr
	}
	// Full transport mesh up front; n3's cache is started later, so it stays
	// silent until then.
	for id, tr := range trs {
		for otherID, otherTr := range trs {
			if id == otherID {
				continue
			}
			tr.Connect(addrs[otherID], otherTr)
		}
	}

	startNode := func(id string, suffrage raft.ServerSuffrage) *resetNode {
		fsm := exampleFSM.NewInMemoryFSM()
		c, err := quasar.NewCache(ctxMain, fsm,
			quasar.WithLocalID(id),
			quasar.WithTransport(trs[id]),
			quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
			quasar.WithSuffrage(suffrage),
		)
		asrt.NoErr(err)
		return &resetNode{id: id, cache: c, fsm: fsm}
	}

	nodes := map[string]*resetNode{}
	nodes["v1"] = startNode("v1", raft.Voter)
	nodes["n2"] = startNode("n2", raft.Nonvoter)
	defer func() {
		for _, n := range nodes {
			_ = n.cache.Shutdown()
		}
	}()

	asrt.NoErr(nodes["v1"].cache.WaitReady(ctxMain))
	asrt.NoErr(nodes["n2"].cache.WaitReady(ctxMain))

	// Wait for n2 to be admitted to the cluster.
	settleCtx, cancelSettle := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelSettle()
	asrt.NoErr(waitForCondition(settleCtx, func() error {
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != 2 {
			return fmt.Errorf("v1 sees %d servers, want 2", len(srvs))
		}
		return nil
	}))

	// Write content via the leader and wait for it to replicate to the
	// follower.
	writeCtx, cancelWrite := context.WithTimeout(ctxMain, 10*time.Second)
	defer cancelWrite()
	for _, name := range []string{"alice", "bob", "carol"} {
		asrt.NoErr(writeMusician(writeCtx, nodes["v1"].fsm, name))
	}
	asrt.NoErr(waitForCondition(writeCtx, func() error {
		return hasAll(nodes["n2"].fsm, "alice", "bob", "carol")
	}))

	// Reset from the nonvoter — forwarded to the leader, committed through
	// the raft log, and replicated back to every member.
	asrt.NoErr(nodes["n2"].cache.Reset(ctxMain))

	clearCtx, cancelClear := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelClear()
	asrt.NoErr(waitForCondition(clearCtx, func() error {
		if e := allCleared(nodes["v1"].fsm, "alice", "bob", "carol"); e != nil {
			return fmt.Errorf("v1: %w", e)
		}
		if e := allCleared(nodes["n2"].fsm, "alice", "bob", "carol"); e != nil {
			return fmt.Errorf("n2: %w", e)
		}
		return nil
	}))

	// Bring up a late joiner *after* the reset. It must converge on the
	// post-reset state via normal replication and never observe the cleared
	// content.
	nodes["n3"] = startNode("n3", raft.Nonvoter)
	asrt.NoErr(nodes["n3"].cache.WaitReady(ctxMain))

	joinCtx, cancelJoin := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelJoin()
	asrt.NoErr(waitForCondition(joinCtx, func() error {
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != 3 {
			return fmt.Errorf("v1 sees %d servers, want 3", len(srvs))
		}
		return nil
	}))

	// A fresh post-reset write is the "caught up" marker: once n3 has it, n3
	// has replayed past the reset, so the absence of the pre-reset content is
	// conclusive.
	asrt.NoErr(writeMusician(joinCtx, nodes["v1"].fsm, "dave"))
	asrt.NoErr(waitForCondition(joinCtx, func() error {
		return hasAll(nodes["n3"].fsm, "dave")
	}))

	for _, id := range specs {
		asrt.NoErr(allCleared(nodes[id].fsm, "alice", "bob", "carol"))
		asrt.NoErr(hasAll(nodes[id].fsm, "dave"))
	}
}

// TestHardResetThenSoftResetClearsCluster exercises the RT-13034 recovery
// sequence. HardReset is an out-of-band fan-out (NOT a raft-log command): it
// sends a hard ResetCache RPC to every server, the leader only clears its FSM
// while every follower tears down and rebuilds its raft on fresh stores
// (dropping log/term/vote/snapshot) so a node that is ahead of a freshly
// (re)started leader stops being ahead and the cluster can form again. The hard
// reset clears FSMs out-of-band but does NOT remove entries from the leader's
// log, so it must be followed by a normal Reset once the cluster is stable: the
// soft reset anchors the clear in the log history / snapshots (RT-12994). This
// test runs the full sequence and asserts the cluster reconverges, the cleared
// content stays gone, and writes still flow.
func TestHardResetThenSoftResetClearsCluster(t *testing.T) {
	const discoveryTick = 200 * time.Millisecond

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelMain()

	asrt := is.New(t)
	bus := newInmemBus()

	specs := []string{"v1", "n2"}
	addrs := map[string]raft.ServerAddress{}
	trs := map[string]*transports.InmemTransport{}
	for _, id := range specs {
		addr, tr := transports.NewInmemTransport("")
		addrs[id] = addr
		trs[id] = tr
	}
	for id, tr := range trs {
		for otherID, otherTr := range trs {
			if id == otherID {
				continue
			}
			tr.Connect(addrs[otherID], otherTr)
		}
	}

	startNode := func(id string, suffrage raft.ServerSuffrage) *resetNode {
		fsm := exampleFSM.NewInMemoryFSM()
		c, err := quasar.NewCache(ctxMain, fsm,
			quasar.WithLocalID(id),
			quasar.WithTransport(trs[id]),
			quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
			quasar.WithSuffrage(suffrage),
		)
		asrt.NoErr(err)
		return &resetNode{id: id, cache: c, fsm: fsm}
	}

	nodes := map[string]*resetNode{}
	nodes["v1"] = startNode("v1", raft.Voter)
	nodes["n2"] = startNode("n2", raft.Nonvoter)
	defer func() {
		for _, n := range nodes {
			_ = n.cache.Shutdown()
		}
	}()

	asrt.NoErr(nodes["v1"].cache.WaitReady(ctxMain))
	asrt.NoErr(nodes["n2"].cache.WaitReady(ctxMain))

	settleCtx, cancelSettle := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelSettle()
	asrt.NoErr(waitForCondition(settleCtx, func() error {
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != 2 {
			return fmt.Errorf("v1 sees %d servers, want 2", len(srvs))
		}
		return nil
	}))

	// Populate and replicate to the follower.
	writeCtx, cancelWrite := context.WithTimeout(ctxMain, 10*time.Second)
	defer cancelWrite()
	for _, name := range []string{"alice", "bob", "carol"} {
		asrt.NoErr(writeMusician(writeCtx, nodes["v1"].fsm, name))
	}
	asrt.NoErr(waitForCondition(writeCtx, func() error {
		return hasAll(nodes["n2"].fsm, "alice", "bob", "carol")
	}))

	// Step 1 — HardReset from the leader: fan-out hard RPC to every server. This
	// tears down and rebuilds the follower's raft on fresh stores. The follower
	// must reconverge on the leader afterwards (its raft was replaced).
	asrt.NoErr(nodes["v1"].cache.HardReset(ctxMain))

	reconvCtx, cancelReconv := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelReconv()
	asrt.NoErr(waitForCondition(reconvCtx, func() error {
		if !nodes["n2"].cache.GetRaftStatus().HasLeader {
			return fmt.Errorf("n2 has no leader after hard reset")
		}
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != 2 {
			return fmt.Errorf("v1 sees %d servers, want 2 after hard reset", len(srvs))
		}
		return nil
	}))

	// Step 2 — the mandatory soft Reset once the cluster is stable again. It is a
	// committed log entry, so it clears every FSM and stays cleared across
	// replication / snapshots (RT-12994).
	asrt.NoErr(nodes["v1"].cache.Reset(ctxMain))

	clearCtx, cancelClear := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelClear()
	asrt.NoErr(waitForCondition(clearCtx, func() error {
		if e := allCleared(nodes["v1"].fsm, "alice", "bob", "carol"); e != nil {
			return fmt.Errorf("v1: %w", e)
		}
		if e := allCleared(nodes["n2"].fsm, "alice", "bob", "carol"); e != nil {
			return fmt.Errorf("n2: %w", e)
		}
		return nil
	}))

	// A post-reset write must replicate to the follower, and the pre-reset
	// content must stay gone on every node.
	asrt.NoErr(writeMusician(clearCtx, nodes["v1"].fsm, "dave"))
	asrt.NoErr(waitForCondition(clearCtx, func() error {
		return hasAll(nodes["n2"].fsm, "dave")
	}))

	for _, id := range specs {
		asrt.NoErr(allCleared(nodes[id].fsm, "alice", "bob", "carol"))
		asrt.NoErr(hasAll(nodes[id].fsm, "dave"))
	}
}

// TestResetSurvivesQuorumRecovery is the regression guard for RT-12994's
// secondary fault: a reset that cleared content used to be silently reverted
// by a later snapshot restore / quorum recovery, because the reset was an
// out-of-band FSM mutation that never entered the consensus history. Now that
// Reset is a committed raft-log command, raft.RecoverCluster replays it on top
// of any older snapshot, so the cleared state is preserved.
//
// Sequence: write content, force a snapshot that captures it, reset (a log
// entry *after* the snapshot), then kill the follower to trigger recoverQuorum
// on the survivor. The survivor must come back as a single-voter leader with
// the content still gone.
func TestResetSurvivesQuorumRecovery(t *testing.T) {
	const (
		recoverAfter  = 6 * time.Second
		recoverBudget = 30 * time.Second
		discoveryTick = 200 * time.Millisecond
	)

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelMain()

	asrt := is.New(t)
	bus := newInmemBus()

	specs := []string{"v1", "v2"}
	addrs := map[string]raft.ServerAddress{}
	trs := map[string]*transports.InmemTransport{}
	for _, id := range specs {
		addr, tr := transports.NewInmemTransport("")
		addrs[id] = addr
		trs[id] = tr
	}
	for id, tr := range trs {
		for otherID, otherTr := range trs {
			if id == otherID {
				continue
			}
			tr.Connect(addrs[otherID], otherTr)
		}
	}

	nodes := map[string]*resetNode{}
	for _, id := range specs {
		fsm := exampleFSM.NewInMemoryFSM()
		c, err := quasar.NewCache(ctxMain, fsm,
			quasar.WithLocalID(id),
			quasar.WithTransport(trs[id]),
			quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
			quasar.WithAutoPrune(recoverAfter),
			quasar.WithQuorumRecovery(recoverAfter),
			quasar.WithSuffrage(raft.Voter),
		)
		asrt.NoErr(err)
		nodes[id] = &resetNode{id: id, cache: c, fsm: fsm}
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.cache.Shutdown()
			}
		}
	}()

	for _, id := range specs {
		asrt.NoErr(nodes[id].cache.WaitReady(ctxMain))
	}

	settleCtx, cancelSettle := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelSettle()
	asrt.NoErr(waitForCondition(settleCtx, func() error {
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != 2 {
			return fmt.Errorf("v1 sees %d servers, want 2", len(srvs))
		}
		if nodes["v1"].cache.GetLeader().ID == "" {
			return fmt.Errorf("no leader yet")
		}
		return nil
	}))

	// Identify the leader (survivor) and the follower (to be killed).
	leaderID := string(nodes["v1"].cache.GetLeader().ID)
	var followerID string
	for _, id := range specs {
		if id != leaderID {
			followerID = id
		}
	}
	leader := nodes[leaderID]
	t.Logf("leader=%s follower=%s", leaderID, followerID)

	// Write content and force a snapshot that captures it. The snapshot
	// predates the reset, exactly the condition that used to resurrect the
	// cleared entry.
	writeCtx, cancelWrite := context.WithTimeout(ctxMain, 10*time.Second)
	defer cancelWrite()
	for _, name := range []string{"alice", "bob", "carol"} {
		asrt.NoErr(writeMusician(writeCtx, leader.fsm, name))
	}
	asrt.NoErr(leader.cache.ForceSnapshot())

	// Reset: a committed log entry *after* the snapshot.
	asrt.NoErr(leader.cache.Reset(ctxMain))
	asrt.NoErr(allCleared(leader.fsm, "alice", "bob", "carol"))

	// Kill the follower. The survivor loses quorum and must recover to a
	// single-voter cluster via raft.RecoverCluster, replaying the reset on top
	// of the pre-reset snapshot.
	asrt.NoErr(nodes[followerID].cache.Shutdown())
	nodes[followerID] = nil

	recoverCtx, cancelRecover := context.WithTimeout(ctxMain, recoverBudget)
	defer cancelRecover()
	asrt.NoErr(waitForCondition(recoverCtx, func() error {
		if !leader.cache.IsLeader() {
			return fmt.Errorf("survivor not yet leader of recovered cluster")
		}
		srvs, e := leader.cache.GetServerList()
		if e != nil {
			return e
		}
		voters := 0
		for _, s := range srvs {
			if s.Suffrage == raft.Voter {
				voters++
			}
		}
		if voters != 1 {
			return fmt.Errorf("survivor sees %d voters, want 1 after recovery", voters)
		}
		return nil
	}))

	// The cleared content must NOT have come back through recovery.
	asrt.NoErr(allCleared(leader.fsm, "alice", "bob", "carol"))

	// And the recovered cluster still accepts writes.
	postCtx, cancelPost := context.WithTimeout(ctxMain, 5*time.Second)
	defer cancelPost()
	asrt.NoErr(writeMusician(postCtx, leader.fsm, "dave"))
	asrt.NoErr(hasAll(leader.fsm, "dave"))
}

// TestHardResetWipesAheadPeerBeforeAddingOnRejoin is the RT-13067 regression
// test for the discovery layer.
//
// Production sequence (gatex's always-hard-reset-at-startup fix): a gate
// restarts with empty in-memory raft stores, bootstraps a single-voter cluster
// at a low term, and hard-resets — but at that instant its configuration is
// only itself, so the HardReset fan-out has no ahead peers to wipe. The
// surviving nonvoters, left on a high term by a previous leader's
// quorum-recovery fork, are only rediscovered and re-added AFTERWARDS, through
// discovery.addServer.
//
// The bug (RT-13067): addServer AddNonvoter'd the rejoining peer first and
// wiped it only after the config change committed. Adding an ahead nonvoter
// starts replication at once; its higher term demotes the fresh leader within a
// heartbeat — killing any in-flight commit (in gatex, the cache populate:
// "leadership lost while committing log") and re-triggering an election, so the
// cluster flaps and never stabilises. The post-add wipe never even runs: the
// AddNonvoter future resolves with "leadership lost" and addServer returns
// before reaching it.
//
// The fix wipes a rejoining peer BEFORE adding it whenever a reset is pending,
// so the ahead nonvoter is reset to fresh stores and cannot demote the leader.
//
// The test reproduces the exact ordering with the inmem bus's mute control:
// crash the first voter, let the second voter quorum-recover (forking the term
// the nonvoters follow up to), crash the second voter, then restart the first
// voter with discovery muted so it bootstraps and hard-resets on an empty
// config. Unmuting re-admits the ahead nonvoters through addServer. The leader
// must NOT be demoted by the rejoins — asserted deterministically by its raft
// term staying constant across the rejoin — and the cluster must converge with
// every nonvoter caught up.
func TestHardResetWipesAheadPeerBeforeAddingOnRejoin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping RT-13067 reproduction in -short mode")
	}

	const (
		discoveryTick = 200 * time.Millisecond
		recoverAfter  = 6 * time.Second
		settleBudget  = 20 * time.Second
		recoverBudget = 30 * time.Second
	)

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelMain()

	asrt := is.New(t)
	bus := newInmemBus()

	type spec struct {
		id       string
		suffrage raft.ServerSuffrage
	}
	specs := []spec{
		{"v1", raft.Voter},
		{"v2", raft.Voter},
		{"n1", raft.Nonvoter},
		{"n2", raft.Nonvoter},
	}
	nonvoters := []string{"n1", "n2"}

	addrs := map[string]raft.ServerAddress{}
	trs := map[string]*transports.InmemTransport{}
	for _, s := range specs {
		addr, tr := transports.NewInmemTransport("")
		addrs[s.id] = addr
		trs[s.id] = tr
	}
	// connect wires id's transport to every other live node, both directions.
	connect := func(id string) {
		for otherID, otherTr := range trs {
			if id == otherID {
				continue
			}
			trs[id].Connect(addrs[otherID], otherTr)
			otherTr.Connect(addrs[id], trs[id])
		}
	}
	for _, s := range specs {
		connect(s.id)
	}

	nodes := map[string]*resetNode{}
	startNode := func(id string, suffrage raft.ServerSuffrage) *resetNode {
		fsm := exampleFSM.NewInMemoryFSM()
		c, err := quasar.NewCache(ctxMain, fsm,
			quasar.WithLocalID(id),
			quasar.WithTransport(trs[id]),
			quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
			quasar.WithAutoPrune(recoverAfter),
			quasar.WithQuorumRecovery(recoverAfter),
			quasar.WithSuffrage(suffrage),
		)
		asrt.NoErr(err)
		return &resetNode{id: id, cache: c, fsm: fsm}
	}

	for _, s := range specs {
		nodes[s.id] = startNode(s.id, s.suffrage)
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.cache.Shutdown()
			}
		}
	}()

	termOf := func(n *resetNode) string {
		return n.cache.GetRaftStatus().Stats["term"]
	}

	asrt.NoErr(nodes["v1"].cache.WaitReady(ctxMain))
	asrt.NoErr(nodes["v2"].cache.WaitReady(ctxMain))

	// Settle on the full topology and a stable leader.
	settleCtx, cancelSettle := context.WithTimeout(ctxMain, settleBudget)
	defer cancelSettle()
	asrt.NoErr(waitForCondition(settleCtx, func() error {
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != len(specs) {
			return fmt.Errorf("v1 sees %d servers, want %d", len(srvs), len(specs))
		}
		if nodes["v1"].cache.GetLeader().ID == "" {
			return fmt.Errorf("no leader yet")
		}
		return nil
	}))

	// Populate the log through the current leader and replicate to a nonvoter.
	leaderFSM := func() *exampleFSM.InMemoryFSM {
		return nodes[string(nodes["v1"].cache.GetLeader().ID)].fsm
	}
	writeCtx, cancelWrite := context.WithTimeout(ctxMain, 10*time.Second)
	for _, name := range []string{"alice", "bob", "carol"} {
		asrt.NoErr(writeMusician(writeCtx, leaderFSM(), name))
	}
	asrt.NoErr(waitForCondition(writeCtx, func() error {
		return hasAll(nodes["n1"].fsm, "alice", "bob", "carol")
	}))
	cancelWrite()

	// Crash v1 — the node that will later restart fresh. (SIGKILL semantics:
	// plain Shutdown, no RemoveAndShutdown.)
	t.Logf("crashing v1")
	asrt.NoErr(nodes["v1"].cache.Shutdown())
	nodes["v1"] = nil

	// v2 runs quorum recovery: it forks to a high term, becomes the sole voter,
	// and the nonvoters follow it up to that term — leaving them ahead of
	// anything a freshly restarted v1 will reach.
	time.Sleep(recoverAfter + 4*time.Second)
	recCtx, cancelRec := context.WithTimeout(ctxMain, recoverBudget)
	asrt.NoErr(waitForCondition(recCtx, func() error {
		if !nodes["v2"].cache.IsLeader() {
			return fmt.Errorf("v2 not leader of recovered cluster yet")
		}
		for _, id := range nonvoters {
			if !nodes[id].cache.GetRaftStatus().HasLeader {
				return fmt.Errorf("%s has no leader yet", id)
			}
		}
		return nil
	}))
	cancelRec()
	t.Logf("nonvoters followed v2 to term %s (v1 will bootstrap far below this)", termOf(nodes["n1"]))

	// Crash v2 too. The nonvoters are now ahead of any fresh v1.
	t.Logf("crashing v2")
	asrt.NoErr(nodes["v2"].cache.Shutdown())
	nodes["v2"] = nil

	// Wait for the nonvoters to drop their stale belief in v2's leadership, so a
	// restarting v1 does not see an "established cluster" and skip bootstrap.
	noLeaderCtx, cancelNoLeader := context.WithTimeout(ctxMain, recoverBudget)
	asrt.NoErr(waitForCondition(noLeaderCtx, func() error {
		for _, id := range nonvoters {
			if nodes[id].cache.GetRaftStatus().HasLeader {
				return fmt.Errorf("%s still sees a leader", id)
			}
		}
		return nil
	}))
	cancelNoLeader()

	// Restart v1 fresh with discovery MUTED, on a new transport wired only to the
	// surviving nonvoters. Muted, it sees no peers, so it bootstraps a
	// single-voter cluster at a low term with a config of only itself —
	// reproducing the production restart.
	addr, tr := transports.NewInmemTransport("")
	addrs["v1"] = addr
	trs["v1"] = tr
	for _, id := range nonvoters {
		tr.Connect(addrs[id], trs[id])
		trs[id].Connect(addr, tr)
	}
	bus.mute("v1")
	nodes["v1"] = startNode("v1", raft.Voter)

	bootCtx, cancelBoot := context.WithTimeout(ctxMain, recoverBudget)
	asrt.NoErr(waitForCondition(bootCtx, func() error {
		if !nodes["v1"].cache.IsLeader() {
			return fmt.Errorf("fresh v1 not yet sole-voter leader")
		}
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != 1 {
			return fmt.Errorf("fresh v1 sees %d servers, want 1 (bootstrap not isolated)", len(srvs))
		}
		return nil
	}))
	cancelBoot()

	// Always-hard-reset-at-startup: v1 hard-resets while its config is only
	// itself, so the fan-out wipes no ahead peers — exactly the production gap.
	asrt.NoErr(nodes["v1"].cache.HardReset(ctxMain))
	termBefore := termOf(nodes["v1"])
	t.Logf("v1 bootstrapped + hard-reset at term %s", termBefore)

	// Unmute: the ahead nonvoters are now rediscovered and re-added through
	// addServer. With the fix they are wiped BEFORE being added, so they never
	// demote v1; with the bug the first one added demotes v1 and the cluster
	// flaps.
	bus.unmute("v1")

	convCtx, cancelConv := context.WithTimeout(ctxMain, recoverBudget)
	asrt.NoErr(waitForCondition(convCtx, func() error {
		if !nodes["v1"].cache.IsLeader() {
			return fmt.Errorf("v1 lost leadership while re-admitting ahead nonvoters (flap)")
		}
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != 3 {
			return fmt.Errorf("v1 sees %d servers, want 3 (self + 2 nonvoters)", len(srvs))
		}
		return nil
	}))
	cancelConv()

	// The discriminating assertion: re-admitting the ahead nonvoters caused NO
	// election on v1. A demotion (the bug) would have bumped the term.
	termAfter := termOf(nodes["v1"])
	if termAfter != termBefore {
		t.Fatalf("v1 term changed from %s to %s across rejoin: leader was demoted by an ahead nonvoter (RT-13067 flap)",
			termBefore, termAfter)
	}

	// A post-recovery write must replicate to both wiped nonvoters.
	postCtx, cancelPost := context.WithTimeout(ctxMain, recoverBudget)
	defer cancelPost()
	asrt.NoErr(writeMusician(postCtx, nodes["v1"].fsm, "dave"))
	asrt.NoErr(waitForCondition(postCtx, func() error {
		for _, id := range nonvoters {
			if e := hasAll(nodes[id].fsm, "dave"); e != nil {
				return fmt.Errorf("%s: %w", id, e)
			}
		}
		return nil
	}))
}
