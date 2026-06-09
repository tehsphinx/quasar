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
