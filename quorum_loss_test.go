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
// propagate raft.Server pings — including each sender's PeerStatus, so
// RT-12862's InstanceID flows the same way the NATS discovery
// (peerStatusToPB) does in production — between caches in the same test
// process. It replaces the NATS-based discovery so the test can run
// hermetically.
//
// muted holds a set of sender localIDs whose pings should not be
// delivered to anyone, used by tests that need to partition discovery
// independently of transport.
type inmemBus struct {
	mu      sync.RWMutex
	members map[*inmemDiscovery]struct{}
	muted   map[string]struct{}
}

func newInmemBus() *inmemBus {
	return &inmemBus{
		members: map[*inmemDiscovery]struct{}{},
		muted:   map[string]struct{}{},
	}
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

// mute and unmute toggle whether a given localID's discovery pings are
// delivered. A muted sender's pings are dropped on broadcast, and other
// senders' pings are not delivered to the muted member either (so the
// muted node sees no peers and is seen by no peer).
func (b *inmemBus) mute(localID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.muted[localID] = struct{}{}
}

func (b *inmemBus) unmute(localID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.muted, localID)
}

func (b *inmemBus) broadcast(srv raft.Server, status quasar.PeerStatus, sender *inmemDiscovery) {
	b.mu.RLock()
	if _, muted := b.muted[string(srv.ID)]; muted {
		b.mu.RUnlock()
		return
	}
	members := make([]*inmemDiscovery, 0, len(b.members))
	for m := range b.members {
		if m == sender {
			continue
		}
		if _, muted := b.muted[m.localID]; muted {
			continue
		}
		members = append(members, m)
	}
	b.mu.RUnlock()

	for _, m := range members {
		m.cache.ProcessServerWithStatus(srv, status)
	}
}

type inmemDiscovery struct {
	bus      *inmemBus
	cache    *quasar.DiscoveryInjector
	interval time.Duration
	localID  string
}

func newInmemDiscovery(bus *inmemBus, interval time.Duration) *inmemDiscovery {
	return &inmemDiscovery{bus: bus, interval: interval}
}

func (d *inmemDiscovery) Inject(c *quasar.DiscoveryInjector) {
	d.cache = c
	d.localID = string(c.ServerInfo().ID)
}

func (d *inmemDiscovery) Run(ctx context.Context) error {
	d.bus.register(d)
	go func() {
		defer d.bus.unregister(d)
		d.bus.broadcast(d.cache.ServerInfo(), d.cache.LocalPeerStatus(), d)
		t := time.NewTicker(d.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				d.bus.broadcast(d.cache.ServerInfo(), d.cache.LocalPeerStatus(), d)
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

// TestInstanceIDForkAdoptionAfterRecoverQuorum is the RT-12862 regression
// guard for the instance-ID-driven fork-detection mechanism.
//
// What the bug looks like in production: a follower observes the leader
// shipping a snapshot whose Index sits below the follower's own cached
// lastLog, with a log gap below the snapshot index. The
// hashicorp/raft installSnapshot path does not reconcile the cached
// lastLog against the older snapshot it just installed, so the next
// AppendEntries(PrevLogEntry=snapshotIndex) hits ErrLogNotFound — the
// leader drops to SEND_SNAP, the follower re-installs the same
// snapshot, and the loop locks in indefinitely.
//
// Root condition: the leader's raft instance is a forked consensus
// history relative to the follower's in-memory state (e.g. the leader
// ran raft.RecoverCluster while the follower was transiently absent).
// Option I makes that fork explicit by tagging every newRaft with a
// fresh InstanceID and broadcasting the current raft leader's ID on
// every discovery ping. A follower whose local ID disagrees with the
// elected leader's for two consecutive pings runs reinitRaftAdoptingInstance
// to wipe its in-memory raft state and adopt the leader's ID — discarding the
// forked log/snapshot cache *before* the leader's older snapshot
// lands.
//
// This test reproduces the adoption mechanism end-to-end with a
// 2-voter cluster:
//
//  1. v1 bootstraps with instance ID `idA`; v2 joins and adopts `idA`
//     via the empty-local path in noteLeaderInstanceID (no reset).
//  2. v2 is hard-stopped. recoverQuorum on v1 mints a fresh `idB` and
//     reduces the cluster to a single-voter config.
//  3. v2 is restarted with empty stores; it observes v1's ping with
//     `idB` and adopts it via the same empty-local path, then rejoins
//     v1's cluster via AddVoter.
//
// A full reproduction of the snapshot-install loop end state requires
// keeping a survivor's in-memory state intact across a partition →
// recover-on-captain → reconnect cycle. With InmemTransport and a
// single test process this involves racing transport partitioning,
// discovery muting, raft leader stepdown, and the recoverQuorum timer
// — a setup too timing-sensitive to be a reliable CI signal. The
// production logs from the recurrence are the smoking gun for that
// path; the adoption mechanism this test guards is the cure.
func TestInstanceIDForkAdoptionAfterRecoverQuorum(t *testing.T) {
	const (
		recoverAfter  = 6 * time.Second
		recoverBudget = 30 * time.Second
		discoveryTick = 200 * time.Millisecond
		bootstrapWait = 2 * time.Second
	)

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 120*time.Second)
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
	startNode := func(id string) *node {
		fsm := exampleFSM.NewInMemoryFSM()
		c, err := quasar.NewCache(ctxMain, fsm,
			quasar.WithLocalID(id),
			quasar.WithTransport(trs[id]),
			quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
			quasar.WithAutoPrune(recoverAfter),
			quasar.WithQuorumRecovery(recoverAfter),
			quasar.WithSuffrage(raft.Voter),
			quasar.WithBootstrapWait(bootstrapWait),
		)
		asrt.NoErr(err)
		return &node{id: id, cache: c, fsm: fsm}
	}

	// Stagger startup so only v1 bootstraps; v2 joins via the
	// peer-in-cluster signal and adopts v1's InstanceID from the first
	// discovery ping.
	nodes := map[string]*node{}
	nodes["v1"] = startNode("v1")
	asrt.NoErr(nodes["v1"].cache.WaitReady(ctxMain))
	defer func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.cache.Shutdown()
			}
		}
	}()
	nodes["v2"] = startNode("v2")
	asrt.NoErr(nodes["v2"].cache.WaitReady(ctxMain))

	settleCtx, cancelSettle := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancelSettle()
	asrt.NoErr(waitForCondition(settleCtx, func() error {
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != 2 {
			return fmt.Errorf("v1 sees %d voters, want 2", len(srvs))
		}
		idA := nodes["v1"].cache.InstanceID()
		if idA == "" {
			return fmt.Errorf("v1 instance ID not minted yet")
		}
		if got := nodes["v2"].cache.InstanceID(); got != idA {
			return fmt.Errorf("v2 instance ID %q != v1 %q (empty-local adoption path)", got, idA)
		}
		return nil
	}))
	idA := nodes["v1"].cache.InstanceID()
	t.Logf("pre-fork cluster instance ID: %s", idA)

	// Hard-stop v2. With one of two voters dead and discovery pruning
	// the dead peer after recoverAfter, recoverQuorum on v1 mints a
	// fresh instance ID and reduces the cluster to a single-voter
	// configuration.
	asrt.NoErr(nodes["v2"].cache.Shutdown())
	nodes["v2"] = nil

	recoverCtx, cancelRecover := context.WithTimeout(ctxMain, recoverBudget)
	defer cancelRecover()
	asrt.NoErr(waitForCondition(recoverCtx, func() error {
		if !nodes["v1"].cache.IsLeader() {
			return fmt.Errorf("v1 not yet leader of recovered cluster")
		}
		if nodes["v1"].cache.InstanceID() == idA {
			return fmt.Errorf("v1 still on old instance ID; recoverQuorum has not minted a new one yet")
		}
		srvs, e := nodes["v1"].cache.GetServerList()
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
			return fmt.Errorf("v1 still sees %d voters, want 1 after recovery", voters)
		}
		return nil
	}))
	idB := nodes["v1"].cache.InstanceID()
	t.Logf("v1 recovered with new instance ID: %s", idB)
	if idB == idA {
		t.Fatalf("idB %q must differ from idA %q", idB, idA)
	}

	// Re-wire v2's transport (Shutdown closed the previous instance) and
	// bring v2 back with empty in-memory stores. v2 should start with an
	// empty InstanceID, observe v1's ping carrying idB, adopt idB
	// silently via the empty-local path (no reset), then rejoin v1's
	// cluster via the standard AddVoter path.
	addr2, tr2 := transports.NewInmemTransport("")
	addrs["v2"] = addr2
	trs["v2"] = tr2
	for id, tr := range trs {
		if id == "v2" {
			continue
		}
		tr2.Connect(addrs[id], tr)
		tr.Connect(addr2, tr2)
	}
	nodes["v2"] = startNode("v2")

	convergeCtx, cancelConverge := context.WithTimeout(ctxMain, recoverBudget)
	defer cancelConverge()
	asrt.NoErr(waitForCondition(convergeCtx, func() error {
		if got := nodes["v2"].cache.InstanceID(); got != idB {
			return fmt.Errorf("v2 instance ID %q != idB %q", got, idB)
		}
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		voters := 0
		for _, s := range srvs {
			if s.Suffrage == raft.Voter {
				voters++
			}
		}
		if voters != 2 {
			return fmt.Errorf("v1 sees %d voters, want 2 (config=%v)", voters, srvs)
		}
		return nil
	}))

	// Final write — verifies the reconverged cluster is functional.
	finalCtx, cancelFinal := context.WithTimeout(ctxMain, 5*time.Second)
	defer cancelFinal()
	asrt.NoErr(nodes["v1"].fsm.SetMusician(finalCtx, exampleFSM.Musician{
		Name: "postFork", Age: 99, Instruments: []string{"tuba"},
	}))
}

// TestRecoveredConfigReplicatesWithoutDiscovery_RT13067 is the regression
// guard for the post-quorum-recovery configuration stall (RT-13067 follow-up).
//
// raft.RecoverCluster persists the forced single-voter configuration only in
// a snapshot — it deletes the whole log. A nonvoter that is log-matched with
// the recovered captain therefore never receives an InstallSnapshot, and the
// captain's post-election noop carries no configuration, so without a fresh
// configuration entry the nonvoter keeps the stale pre-recovery configuration
// indefinitely. Its discovery pings then advertise the stale voter count,
// which a later restarting voter reads as "established cluster", skipping
// bootstrap into a configless wedge (RT-12775 gating).
//
// In production the instance-ID mismatch wipe usually masks this: the wiped
// nonvoter is caught up via InstallSnapshot, which carries the config. But
// that path depends on a discovery ping of the recovered leader arriving —
// and discovery delivery is best-effort. This test makes the dependency
// explicit by muting ALL discovery before the crash: the recovered
// configuration must reach the log-matched nonvoter through raft log
// replication alone (the recoverQuorum config re-assert), with no wipe — the
// nonvoter keeps its instance ID and its data.
func TestRecoveredConfigReplicatesWithoutDiscovery_RT13067(t *testing.T) {
	const (
		recoverAfter  = 6 * time.Second
		recoverBudget = 45 * time.Second
		discoveryTick = 200 * time.Millisecond
		bootstrapWait = 2 * time.Second
	)

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 150*time.Second)
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

	type node struct {
		id    string
		cache *quasar.Cache
		fsm   *exampleFSM.InMemoryFSM
	}
	startNode := func(id string, suffrage raft.ServerSuffrage) *node {
		fsm := exampleFSM.NewInMemoryFSM()
		// No WithAutoPrune: pruning is discovery-fed, and this test mutes
		// discovery — the muted (but alive and replicating) nonvoter must not
		// be pruned as dead. aliveCutoff falls back to recoverAfter.
		c, err := quasar.NewCache(ctxMain, fsm,
			quasar.WithLocalID(id),
			quasar.WithTransport(trs[id]),
			quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
			quasar.WithQuorumRecovery(recoverAfter),
			quasar.WithSuffrage(suffrage),
			quasar.WithBootstrapWait(bootstrapWait),
		)
		asrt.NoErr(err)
		return &node{id: id, cache: c, fsm: fsm}
	}

	// Stagger startup so only v1 bootstraps and everyone adopts its
	// instance ID via leader pings.
	nodes := map[string]*node{}
	nodes["v1"] = startNode("v1", raft.Voter)
	asrt.NoErr(nodes["v1"].cache.WaitReady(ctxMain))
	defer func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.cache.Shutdown()
			}
		}
	}()
	nodes["v2"] = startNode("v2", raft.Voter)
	asrt.NoErr(nodes["v2"].cache.WaitReady(ctxMain))
	nodes["n1"] = startNode("n1", raft.Nonvoter)

	settleCtx, cancelSettle := context.WithTimeout(ctxMain, 20*time.Second)
	defer cancelSettle()
	asrt.NoErr(waitForCondition(settleCtx, func() error {
		srvs, e := nodes["v1"].cache.GetServerList()
		if e != nil {
			return e
		}
		if len(srvs) != len(specs) {
			return fmt.Errorf("v1 sees %d servers, want %d", len(srvs), len(specs))
		}
		idA := nodes["v1"].cache.InstanceID()
		if idA == "" {
			return fmt.Errorf("v1 instance ID not minted yet")
		}
		if got := nodes["n1"].cache.InstanceID(); got != idA {
			return fmt.Errorf("n1 instance ID %q != v1 %q", got, idA)
		}
		return nil
	}))
	idA := nodes["v1"].cache.InstanceID()

	// Write through the cluster and wait until n1 is fully caught up
	// (log-matched with the voters).
	{
		writeCtx, cancel := context.WithTimeout(ctxMain, 5*time.Second)
		defer cancel()
		asrt.NoErr(nodes["v1"].fsm.SetMusician(writeCtx, exampleFSM.Musician{
			Name: "preCrash", Age: 1, Instruments: []string{"theremin"},
		}))
	}
	catchupCtx, cancelCatchup := context.WithTimeout(ctxMain, 10*time.Second)
	defer cancelCatchup()
	asrt.NoErr(waitForCondition(catchupCtx, func() error {
		if _, e := nodes["n1"].fsm.GetMusicianLocal("preCrash"); e != nil {
			return fmt.Errorf("n1 not caught up: %w", e)
		}
		return nil
	}))

	// Mute ALL discovery. From here on no instance-ID adoption (and thus no
	// mismatch wipe + InstallSnapshot) can deliver the recovered config to
	// n1 — it must arrive through raft log replication.
	for _, s := range specs {
		bus.mute(s.id)
	}

	// Hard-stop v2 — the recovery captain precondition.
	asrt.NoErr(nodes["v2"].cache.Shutdown())
	nodes["v2"] = nil

	recoverCtx, cancelRecover := context.WithTimeout(ctxMain, recoverBudget)
	defer cancelRecover()
	asrt.NoErr(waitForCondition(recoverCtx, func() error {
		if !nodes["v1"].cache.IsLeader() {
			return fmt.Errorf("v1 not yet leader of recovered cluster")
		}
		if nodes["v1"].cache.InstanceID() == idA {
			return fmt.Errorf("v1 still on pre-recovery instance ID")
		}
		return nil
	}))

	// The regression assertion: the log-matched, discovery-deaf nonvoter
	// receives the recovered single-voter configuration.
	configCtx, cancelConfig := context.WithTimeout(ctxMain, recoverBudget)
	defer cancelConfig()
	asrt.NoErr(waitForCondition(configCtx, func() error {
		srvs, e := nodes["n1"].cache.GetServerList()
		if e != nil {
			return e
		}
		voters := 0
		for _, s := range srvs {
			if s.ID == "v2" {
				return fmt.Errorf("n1 config still contains the dropped voter v2")
			}
			if s.Suffrage == raft.Voter {
				voters++
			}
		}
		if voters != 1 {
			return fmt.Errorf("n1 config has %d voters, want 1", voters)
		}
		return nil
	}))

	// Prove the config came via log replication, not via a wipe: n1 kept its
	// pre-recovery instance ID and its data.
	if got := nodes["n1"].cache.InstanceID(); got != idA {
		t.Fatalf("n1 instance ID changed to %q (was %q) — config arrived via a wipe, not log replication", got, idA)
	}
	if _, err := nodes["n1"].fsm.GetMusicianLocal("preCrash"); err != nil {
		t.Fatalf("n1 lost its data across the recovery: %v", err)
	}
}
