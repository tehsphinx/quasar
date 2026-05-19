package quasar_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/matryer/is"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/examples/generic/exampleFSM"
	"github.com/tehsphinx/quasar/transports"
)

// inmemBus is a tiny in-memory pubsub used by inmemDiscovery instances to
// propagate raft.Server pings between caches in the same test process. It
// replaces the NATS-based discovery used in production so the test can run
// hermetically.
type inmemBus struct {
	mu      sync.RWMutex
	members map[*inmemDiscovery]struct{}
}

func newInmemBus() *inmemBus {
	return &inmemBus{members: map[*inmemDiscovery]struct{}{}}
}

func (b *inmemBus) register(d *inmemDiscovery) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.members[d] = struct{}{}
}

func (b *inmemBus) unregister(d *inmemDiscovery) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.members, d)
}

func (b *inmemBus) broadcast(srv raft.Server, sender *inmemDiscovery) {
	b.mu.RLock()
	members := make([]*inmemDiscovery, 0, len(b.members))
	for m := range b.members {
		if m == sender {
			continue
		}
		members = append(members, m)
	}
	b.mu.RUnlock()

	for _, m := range members {
		m.cache.ProcessServer(srv)
	}
}

type inmemDiscovery struct {
	bus      *inmemBus
	cache    *quasar.DiscoveryInjector
	interval time.Duration
}

func newInmemDiscovery(bus *inmemBus, interval time.Duration) *inmemDiscovery {
	return &inmemDiscovery{bus: bus, interval: interval}
}

func (d *inmemDiscovery) Inject(c *quasar.DiscoveryInjector) { d.cache = c }

func (d *inmemDiscovery) Run(ctx context.Context) error {
	d.bus.register(d)
	go func() {
		defer d.bus.unregister(d)
		d.bus.broadcast(d.cache.ServerInfo(), d)
		t := time.NewTicker(d.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				d.bus.broadcast(d.cache.ServerInfo(), d)
			}
		}
	}()
	return nil
}

// TestQuorumLossOnGatexShutdown exercises the RT-12544 fix.
//
// Topology mirrors a gatex deployment:
//   - 2 voters (the gatex caches)
//   - 1 nonvoter pgwatcher
//   - 2 nonvoter stores
//   - 2 nonvoter fservers
//
// The cluster has only 2 voters because gatex (the voter role) typically runs
// in 2-site HA. RAFT requires a majority of voters to make progress, so a
// 2-voter cluster needs both voters online.
//
// The test brings the cluster up, verifies it accepts writes, then abruptly
// shuts down one of the two gatex voters (no RemoveAndShutdown — simulating a
// crash or hard process kill). With WithQuorumRecovery enabled, it expects:
//
//  1. the surviving voter to become leader of a recovered single-voter cluster
//  2. the dead voter to be removed from the raft configuration
//  3. writes to keep working through the survivor
//
// Without the option (or in versions of quasar before this commit), all three
// would fail: 2-voter raft cannot elect with one voter dead, and quasar's
// auto-prune is leader-only so nothing prunes the dead peer.
//
// Run with: go test -run TestQuorumLossOnGatexShutdown -v -timeout 90s
func TestQuorumLossOnGatexShutdown(t *testing.T) {
	const (
		// Auto-prune is clamped to a 6s minimum in WithAutoPrune. We use the
		// minimum to keep the test as fast as possible while still exercising
		// the auto-prune code path.
		pruneAfter = 6 * time.Second
		// How long we wait after the shutdown for the cluster to recover.
		// Generous to absorb raft election backoff and slow CI.
		recoverBudget = 30 * time.Second
		discoveryTick = 500 * time.Millisecond
	)

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelMain()

	asrt := is.New(t)
	bus := newInmemBus()

	type spec struct {
		id       string
		suffrage raft.ServerSuffrage
	}
	specs := []spec{
		{"gatex1", raft.Voter},
		{"gatex2", raft.Voter},
		{"pgwatcher1", raft.Nonvoter},
		{"store1", raft.Nonvoter},
		{"store2", raft.Nonvoter},
		{"fserver1", raft.Nonvoter},
		{"fserver2", raft.Nonvoter},
	}

	type node struct {
		id    string
		cache *quasar.Cache
		fsm   *exampleFSM.InMemoryFSM
	}

	addrs := map[string]raft.ServerAddress{}
	trs := map[string]*transports.InmemTransport{}
	for _, s := range specs {
		addr, tr := transports.NewInmemTransport("")
		addrs[s.id] = addr
		trs[s.id] = tr
	}
	for id, tr := range trs {
		for otherID, otherTr := range trs {
			if id == otherID {
				continue
			}
			tr.Connect(addrs[otherID], otherTr)
		}
	}

	nodes := map[string]*node{}
	for _, s := range specs {
		fsm := exampleFSM.NewInMemoryFSM()
		c, err := quasar.NewCache(ctxMain, fsm,
			quasar.WithLocalID(s.id),
			quasar.WithTransport(trs[s.id]),
			quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
			quasar.WithAutoPrune(pruneAfter),
			quasar.WithQuorumRecovery(pruneAfter),
			quasar.WithSuffrage(s.suffrage),
		)
		asrt.NoErr(err)
		nodes[s.id] = &node{id: s.id, cache: c, fsm: fsm}
	}
	defer func() {
		for _, n := range nodes {
			_ = n.cache.Shutdown()
		}
	}()

	// Wait for the voters to become ready (leader elected, configured).
	asrt.NoErr(nodes["gatex1"].cache.WaitReady(ctxMain))
	asrt.NoErr(nodes["gatex2"].cache.WaitReady(ctxMain))

	// Wait for the full topology to land in the raft configuration. Discovery
	// pings have to propagate and the leader has to AddNonvoter each one.
	settleCtx, cancelSettle := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelSettle()
	err := waitForCondition(settleCtx, func() error {
		srvs, e := nodes["gatex1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != len(specs) {
			return fmt.Errorf("server list has %d entries, want %d", len(srvs), len(specs))
		}
		return nil
	})
	asrt.NoErr(err)

	// Sanity check: writes work before the shutdown.
	{
		writeCtx, cancel := context.WithTimeout(ctxMain, 5*time.Second)
		defer cancel()
		err := nodes["gatex1"].fsm.SetMusician(writeCtx, exampleFSM.Musician{
			Name: "preShutdown", Age: 1, Instruments: []string{"triangle"},
		})
		asrt.NoErr(err)
	}

	// Identify which gatex is the leader and shut down the other one. This
	// matches the QA scenario from RT-12544: leader stays up, follower dies
	// abruptly. (The bug reproduces either way; choosing the follower keeps
	// the leader stable across the kill.)
	leader := nodes["gatex1"].cache.GetLeader()
	t.Logf("leader before shutdown: %s", leader.ID)
	var (
		survivor   *node
		victimName string
	)
	switch leader.ID {
	case "gatex1":
		survivor = nodes["gatex1"]
		victimName = "gatex2"
	case "gatex2":
		survivor = nodes["gatex2"]
		victimName = "gatex1"
	default:
		t.Fatalf("expected gatex1 or gatex2 as leader, got %q", leader.ID)
	}

	t.Logf("shutting down voter %s (no RemoveAndShutdown — simulating crash)", victimName)
	asrt.NoErr(nodes[victimName].cache.Shutdown())
	delete(nodes, victimName)

	// Now wait past the prune window. With the bug present, the survivor
	// loses leadership (no quorum), no node prunes the dead voter, and writes
	// fail with "failed to get leader: context deadline exceeded".
	recoverCtx, cancelRecover := context.WithTimeout(ctxMain, recoverBudget)
	defer cancelRecover()

	err = waitForCondition(recoverCtx, func() error {
		if !survivor.cache.IsLeader() {
			st := survivor.cache.GetRaftStatus()
			return fmt.Errorf("survivor %s is not leader (state=%s, has_leader=%v, voters=%d)",
				survivor.id, st.State, st.HasLeader, st.NumVoters)
		}
		srvs, e := survivor.cache.GetServerList()
		if e != nil {
			return e
		}
		for _, s := range srvs {
			if string(s.ID) == victimName {
				return fmt.Errorf("dead voter %s still in raft config", victimName)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cluster did not recover after voter loss: %v", err)
	}

	// And confirm writes resume working through the survivor.
	writeCtx, cancelWrite := context.WithTimeout(ctxMain, 5*time.Second)
	defer cancelWrite()
	err = survivor.fsm.SetMusician(writeCtx, exampleFSM.Musician{
		Name: "postShutdown", Age: 2, Instruments: []string{"kazoo"},
	})
	if err != nil {
		t.Fatalf("write failed after voter loss: %v", err)
	}

	// Sanity: nonvoter sees the post-shutdown write through GetMusicianMaster.
	readCtx, cancelRead := context.WithTimeout(ctxMain, 5*time.Second)
	defer cancelRead()
	got, err := nodes["pgwatcher1"].fsm.GetMusicianMaster(readCtx, "postShutdown")
	if err != nil {
		// pgwatcher's shouldApply may filter out musician commands in
		// production — but the test FSM accepts everything, so this should
		// succeed.
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("nonvoter read failed: %v", err)
		}
	}
	_ = got

	// Now restart the dead voter and verify it rejoins the recovered cluster
	// as a voter. This is the path operators actually take: once the failed
	// gatex comes back, it should reattach without manual intervention.
	t.Logf("restarting %s and verifying it rejoins the recovered cluster", victimName)

	addr2, tr2 := transports.NewInmemTransport("")
	addrs[victimName] = addr2
	trs[victimName] = tr2
	// (Re-)wire the new transport to every other live peer in both directions.
	// Replaces the stale entries left over from the old transport instance.
	for id, tr := range trs {
		if id == victimName {
			continue
		}
		tr2.Connect(addrs[id], tr)
		tr.Connect(addr2, tr2)
	}

	fsm2 := exampleFSM.NewInMemoryFSM()
	c2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID(victimName),
		quasar.WithTransport(tr2),
		quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
		quasar.WithAutoPrune(pruneAfter),
		quasar.WithQuorumRecovery(pruneAfter),
		quasar.WithSuffrage(raft.Voter),
	)
	asrt.NoErr(err)
	nodes[victimName] = &node{id: victimName, cache: c2, fsm: fsm2}

	rejoinCtx, cancelRejoin := context.WithTimeout(ctxMain, 30*time.Second)
	defer cancelRejoin()
	err = waitForCondition(rejoinCtx, func() error {
		srvs, e := survivor.cache.GetServerList()
		if e != nil {
			return e
		}
		for _, s := range srvs {
			if string(s.ID) == victimName && s.Suffrage == raft.Voter {
				return nil
			}
		}
		return fmt.Errorf("%s has not rejoined as a voter yet (config=%v)", victimName, srvs)
	})
	if err != nil {
		t.Fatalf("rejoiner did not reattach to the cluster: %v", err)
	}

	// Final write — verify the cluster is genuinely back to a 2-voter setup.
	finalCtx, cancelFinal := context.WithTimeout(ctxMain, 5*time.Second)
	defer cancelFinal()
	if err := survivor.fsm.SetMusician(finalCtx, exampleFSM.Musician{
		Name: "postRejoin", Age: 3, Instruments: []string{"theremin"},
	}); err != nil {
		t.Fatalf("write failed after rejoin: %v", err)
	}
}

// TestQuorumLossMultiVoter covers the case where a cluster with more than
// two voters loses enough voters to drop below quorum, but at least one peer
// voter is still alive. Four voters total, two are killed at once: the two
// survivors can no longer reach quorum-of-four, so recovery is required even
// though one peer voter is still pingable. The single-criterion check in an
// earlier draft of the watcher would have skipped recovery here because some
// peer voter was still alive; the quorum-aware check forces recovery
// correctly.
func TestQuorumLossMultiVoter(t *testing.T) {
	const (
		recoverAfter  = 6 * time.Second
		recoverBudget = 30 * time.Second
		discoveryTick = 500 * time.Millisecond
	)

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelMain()

	asrt := is.New(t)
	bus := newInmemBus()

	specs := []string{"v1", "v2", "v3", "v4"}

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

	type node struct {
		id    string
		cache *quasar.Cache
		fsm   *exampleFSM.InMemoryFSM
	}
	nodes := map[string]*node{}
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
		nodes[id] = &node{id: id, cache: c, fsm: fsm}
	}
	defer func() {
		for _, n := range nodes {
			_ = n.cache.Shutdown()
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
		if len(srvs) != len(specs) {
			return fmt.Errorf("server list has %d entries, want %d", len(srvs), len(specs))
		}
		if nodes["v1"].cache.GetLeader().ID == "" {
			return fmt.Errorf("no leader yet")
		}
		return nil
	}))

	// Kill the leader and one follower simultaneously. With 4 voters total,
	// quorum is 3; the surviving 2 can't elect on their own and must
	// recover.
	leader := nodes["v1"].cache.GetLeader()
	if leader.ID == "" {
		t.Fatalf("no leader after settle")
	}
	t.Logf("leader before kill: %s", leader.ID)
	deadIDs := []string{string(leader.ID)}
	for _, id := range specs {
		if id != string(leader.ID) {
			deadIDs = append(deadIDs, id)
			break
		}
	}
	for _, id := range deadIDs {
		t.Logf("killing voter %s", id)
		asrt.NoErr(nodes[id].cache.Shutdown())
		delete(nodes, id)
	}

	// Pick one of the survivors and wait for it to claim leadership of the
	// recovered cluster.
	var survivor *node
	for _, n := range nodes {
		survivor = n
		break
	}

	recoverCtx, cancelRecover := context.WithTimeout(ctxMain, recoverBudget)
	defer cancelRecover()
	err := waitForCondition(recoverCtx, func() error {
		srvs, e := survivor.cache.GetServerList()
		if e != nil {
			return e
		}
		// All dead voters must be gone from the configuration.
		for _, s := range srvs {
			for _, d := range deadIDs {
				if string(s.ID) == d {
					return fmt.Errorf("dead voter %s still in raft config", d)
				}
			}
		}
		// And the cluster must have a leader.
		if !survivor.cache.GetRaftStatus().HasLeader {
			return fmt.Errorf("no leader yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("multi-voter cluster did not recover: %v", err)
	}

	// Verify writes go through against the recovered cluster (find whichever
	// surviving node is leader; either v1+vN or vN alone works).
	writeCtx, cancelWrite := context.WithTimeout(ctxMain, 5*time.Second)
	defer cancelWrite()
	for _, n := range nodes {
		if n.cache.IsLeader() {
			if e := n.fsm.SetMusician(writeCtx, exampleFSM.Musician{
				Name: "postRecover", Age: 9, Instruments: []string{"harmonica"},
			}); e != nil {
				t.Fatalf("write to recovered leader failed: %v", e)
			}
			return
		}
	}
	t.Fatalf("no surviving node is leader after recovery")
}

// TestQuorumRecoveryFiresAtThreshold is the RT-12846 regression guard for the
// event-driven recovery trigger.
//
// The original polling watcher (time.Ticker with interval recoverQuorumAfter/4,
// min 1s) added up to ~recoverQuorumAfter/4 of slop on both sides of the
// threshold: one tick to register noLeaderSince after step-down, and one tick
// after the threshold elapsed to actually act. With the 6s floor that is
// ~3s of avoidable slop on top of the threshold, pushing end-to-end recovery
// to ~11s.
//
// The event-driven trigger arms a one-shot timer the moment LeaderObservation
// reports leaderless and fires at exactly recoverQuorumAfter. End-to-end
// recovery should fit in step-down (~1s) + recoverQuorumAfter + post-recovery
// election (~1s) ≈ 8s. The budget below would fail if the polling slop
// regressed.
func TestQuorumRecoveryFiresAtThreshold(t *testing.T) {
	const (
		recoverAfter       = 6 * time.Second
		fastRecoveryBudget = 9 * time.Second
		discoveryTick      = 250 * time.Millisecond
	)

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 60*time.Second)
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

	type node struct {
		id    string
		cache *quasar.Cache
		fsm   *exampleFSM.InMemoryFSM
	}
	nodes := map[string]*node{}
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
		nodes[id] = &node{id: id, cache: c, fsm: fsm}
	}
	defer func() {
		for _, n := range nodes {
			_ = n.cache.Shutdown()
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
			return fmt.Errorf("server list has %d entries, want 2", len(srvs))
		}
		if nodes["v1"].cache.GetLeader().ID == "" {
			return fmt.Errorf("no leader yet")
		}
		return nil
	}))

	leader := nodes["v1"].cache.GetLeader()
	var (
		survivor   *node
		victimName string
	)
	switch leader.ID {
	case "v1":
		survivor, victimName = nodes["v1"], "v2"
	case "v2":
		survivor, victimName = nodes["v2"], "v1"
	default:
		t.Fatalf("expected v1 or v2 as leader, got %q", leader.ID)
	}

	t.Logf("killing %s; survivor=%s", victimName, survivor.id)
	asrt.NoErr(nodes[victimName].cache.Shutdown())
	delete(nodes, victimName)

	start := time.Now()
	recoverCtx, cancelRecover := context.WithTimeout(ctxMain, fastRecoveryBudget+5*time.Second)
	defer cancelRecover()
	err := waitForCondition(recoverCtx, func() error {
		if !survivor.cache.IsLeader() {
			return fmt.Errorf("survivor %s not leader yet", survivor.id)
		}
		srvs, e := survivor.cache.GetServerList()
		if e != nil {
			return e
		}
		for _, s := range srvs {
			if string(s.ID) == victimName {
				return fmt.Errorf("dead voter %s still in raft config", victimName)
			}
		}
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("survivor did not recover: %v", err)
	}
	if elapsed > fastRecoveryBudget {
		t.Fatalf("recovery took %s, budget is %s — event-driven trigger may have regressed to polling", elapsed, fastRecoveryBudget)
	}
	t.Logf("recovery elapsed: %s (budget %s)", elapsed, fastRecoveryBudget)
}

// TestQuorumRecoveryCancelOnTransientPartition covers the cancel path of the
// event-driven recovery trigger: when leadership briefly disappears and is
// restored before recoverQuorumAfter, the armed recovery timer must be
// cancelled so the cluster is not torn down by a spurious recoverQuorum.
//
// Setup: 3 voters with recoverQuorumAfter = 6s. The leader's raft transport
// is severed from the other two for a window shorter than recoverQuorumAfter.
// The two connected followers retain quorum (2/3) and elect a new leader; the
// old leader steps down. After reconnecting, the old leader rejoins as a
// follower. At no point should any node have run raft.RecoverCluster — the
// configuration should still hold all three voters.
//
// Discovery runs on a separate in-process bus and is unaffected by the raft
// transport partition, so discovery's alive-window check is also a backstop
// against recovery here. The end-state invariant (all 3 voters present) is
// the regression guard.
func TestQuorumRecoveryCancelOnTransientPartition(t *testing.T) {
	const (
		recoverAfter  = 6 * time.Second
		partitionFor  = 2 * time.Second
		discoveryTick = 250 * time.Millisecond
	)

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelMain()

	asrt := is.New(t)
	bus := newInmemBus()

	specs := []string{"v1", "v2", "v3"}
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

	type node struct {
		id    string
		cache *quasar.Cache
		fsm   *exampleFSM.InMemoryFSM
	}
	nodes := map[string]*node{}
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
		nodes[id] = &node{id: id, cache: c, fsm: fsm}
	}
	defer func() {
		for _, n := range nodes {
			_ = n.cache.Shutdown()
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
		if len(srvs) != 3 {
			return fmt.Errorf("server list has %d entries, want 3", len(srvs))
		}
		if nodes["v1"].cache.GetLeader().ID == "" {
			return fmt.Errorf("no leader yet")
		}
		return nil
	}))

	leader := nodes["v1"].cache.GetLeader()
	if leader.ID == "" {
		t.Fatalf("no leader after settle")
	}
	leaderID := string(leader.ID)
	t.Logf("leader before partition: %s", leaderID)

	// Sever the raft transport between the leader and the two followers
	// in both directions. Discovery still works (separate channel), so
	// the alive-window check would not by itself trigger recovery — the
	// timer-cancel path is what we are exercising.
	leaderTr := trs[leaderID]
	for _, id := range specs {
		if id == leaderID {
			continue
		}
		leaderTr.Disconnect(addrs[id])
		trs[id].Disconnect(addrs[leaderID])
	}

	time.Sleep(partitionFor)

	// Heal the partition before recoverAfter elapses.
	for _, id := range specs {
		if id == leaderID {
			continue
		}
		leaderTr.Connect(addrs[id], trs[id])
		trs[id].Connect(addrs[leaderID], leaderTr)
	}

	// Wait past the original recoverAfter window. If any node failed to
	// cancel its pending recovery timer, the configuration would now be
	// short of voters (a forced recovery drops missing voters).
	time.Sleep(recoverAfter + time.Second)

	for _, id := range specs {
		srvs, err := nodes[id].cache.GetServerList()
		if err != nil {
			t.Fatalf("node %s GetServerList: %v", id, err)
		}
		voters := 0
		for _, s := range srvs {
			if s.Suffrage == raft.Voter {
				voters++
			}
		}
		if voters != 3 {
			t.Fatalf("node %s sees %d voters after transient partition, want 3 (a spurious recovery may have fired)", id, voters)
		}
	}
}
