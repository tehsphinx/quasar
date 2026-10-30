package quasar

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// pingingDiscovery keeps announcing one peer, the way a live peer's discovery
// pings keep arriving. It pings with the status last set (none while nil), or
// without any status when plain is set.
type pingingDiscovery struct {
	inj    *DiscoveryInjector
	peer   raft.Server
	plain  bool
	status atomic.Pointer[PeerStatus]
}

func (d *pingingDiscovery) Inject(inj *DiscoveryInjector) { d.inj = inj }

func (d *pingingDiscovery) Run(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			if d.plain {
				d.inj.ProcessServer(d.peer)
			} else if status := d.status.Load(); status != nil {
				d.inj.ProcessServerWithStatus(d.peer, *status)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

// TestVoterAhead is the RT-15590 unit test of the fallback's order: an alive
// voter peer outside an established cluster goes first when it has a lower ID
// or has already bootstrapped.
func TestVoterAhead(t *testing.T) {
	voter := func(id raft.ServerID) raft.Server {
		return raft.Server{ID: id, Address: raft.ServerAddress(id), Suffrage: raft.Voter}
	}

	tests := []struct {
		name   string
		peer   raft.Server
		status PeerStatus
		stale  bool
		ahead  bool
	}{
		{name: "lower ID, configless", peer: voter("sitea"), ahead: true},
		{name: "lower ID, bootstrapped", peer: voter("sitea"), status: PeerStatus{NumVotersInConfig: 1}, ahead: true},
		{name: "higher ID, bootstrapped", peer: voter("sited"), status: PeerStatus{NumVotersInConfig: 1}, ahead: true},
		{name: "higher ID, configless", peer: voter("sited")},
		{name: "lower ID, multi-voter configuration", peer: voter("sitea"), status: PeerStatus{NumVotersInConfig: 2}},
		{name: "lower ID, following a leader", peer: voter("sitea"), status: PeerStatus{HasLeader: true}},
		{name: "lower ID, leading", peer: voter("sitea"), status: PeerStatus{HasLeader: true, NumVotersInConfig: 1}},
		{name: "lower ID, stale", peer: voter("sitea"), stale: true},
		{name: "lower ID, nonvoter", peer: raft.Server{ID: "fserver", Suffrage: raft.Nonvoter}},
		{name: "self, bootstrapped", peer: voter("sitec"), status: PeerStatus{NumVotersInConfig: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDiscoveryInjector(&Cache{localID: "sitec"})
			d.setServerWithStatus(tt.peer, tt.status)
			if tt.stale {
				d.lastSeen[tt.peer.ID] = time.Now().Add(-time.Minute)
			}

			if got := d.voterAhead(time.Second); got != tt.ahead {
				t.Fatalf("expected voterAhead %v, got %v", tt.ahead, got)
			}
		})
	}
}

// TestBootstrapFallbackAfterReset is the RT-15590 regression test for G1: a
// voter whose raft was rebuilt by a history reset has an empty configuration
// and cannot campaign. Pre-fix nothing ever bootstrapped it again (Sites A and
// B logged "no known peers, aborting election" for 38 minutes); the reset must
// arm the bootstrap fallback. The fallback holds off for as long as a
// lower-ID voter pings configless too, and is not armed at all for a
// discovery that never reports status: it could not tell a live leader.
func TestBootstrapFallbackAfterReset(t *testing.T) {
	const window = time.Second

	voter := func(id raft.ServerID) raft.Server {
		return raft.Server{ID: id, Address: raft.ServerAddress(id), Suffrage: raft.Voter}
	}

	tests := []struct {
		name       string
		peer       raft.Server
		plain      bool
		peerStatus PeerStatus
		// heldOff is whether the fallback must wait while the peer pings.
		heldOff bool
	}{
		{
			name:       "behind a configless lower-ID voter",
			peer:       voter("sitea"),
			peerStatus: PeerStatus{InstanceID: "hist-a"},
			heldOff:    true,
		},
		{
			name:       "not behind a lower-ID voter in an established cluster",
			peer:       voter("sitea"),
			peerStatus: PeerStatus{NumVotersInConfig: 2, InstanceID: "hist-a"},
		},
		{
			// A higher ID, so only the missing status holds the fallback off.
			name:    "discovery without status",
			peer:    voter("sited"),
			plain:   true,
			heldOff: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			out := &syncBuffer{}
			disc := &pingingDiscovery{peer: tt.peer, plain: tt.plain}
			_, tr := transports.NewInmemTransport("")
			c, err := NewCache(ctx, newSnapFSM(),
				WithLocalID("siteb"),
				WithTransport(tr),
				WithDiscovery(disc),
				WithBootstrapWait(0),
				WithBootstrapFallbackWait(200*time.Millisecond),
				WithNoLeaderTimeout(window),
				WithHclogLogger(hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})),
			)
			if err != nil {
				t.Fatalf("NewCache: %v", err)
			}
			t.Cleanup(func() { _ = c.Shutdown() })
			if err := c.WaitReady(ctx); err != nil {
				t.Fatalf("WaitReady: %v", err)
			}

			if !tt.plain {
				// sitea was in an established cluster before it was reset.
				disc.status.Store(&PeerStatus{NumVotersInConfig: 2})
				select {
				case <-c.discovery.PeerInCluster():
				case <-ctx.Done():
					t.Fatal("the established-cluster ping never arrived")
				}
				disc.status.Store(&tt.peerStatus)
			}

			if err := c.ReinitForTest("hist-c"); err != nil {
				t.Fatalf("reinit: %v", err)
			}
			if !c.hasEmptyConfig() {
				t.Fatal("expected an empty configuration right after the reset")
			}

			if tt.heldOff {
				ctxHold, cancelHold := context.WithTimeout(ctx, 3*window)
				defer cancelHold()
				if eventually(ctxHold, func() bool { return !c.hasEmptyConfig() }) {
					t.Fatalf("the fallback bootstrapped while it had to hold off:\n%s", out.String())
				}
				if tt.plain {
					return
				}
				// sitea goes away without bootstrapping: once its pings age
				// out, nobody is ahead any more.
				disc.status.Store(nil)
			}

			ctxLead, cancelLead := context.WithTimeout(ctx, window+5*time.Second)
			defer cancelLead()
			if !eventually(ctxLead, c.IsLeader) {
				t.Fatalf("the reset voter never led again:\n%s", out.String())
			}
			if !strings.Contains(out.String(), "bootstrap fallback") {
				t.Fatalf("expected a bootstrap-fallback log, got:\n%s", out.String())
			}
		})
	}
}

// TestBootstrapFallbackIgnoresReplacedRaft guards the RT-15590 binding: a
// fallback armed for one raft instance must not bootstrap the instance that
// replaced it. Pre-fix the startup fallback bootstrapped whatever s.raft()
// happened to be.
func TestBootstrapFallbackIgnoresReplacedRaft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out := &syncBuffer{}
	disc := &announcingDiscovery{
		peerID: "stale-peer",
		status: PeerStatus{HasLeader: false, NumVotersInConfig: 2},
	}
	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, newSnapFSM(),
		WithLocalID("voter"),
		WithTransport(tr),
		WithDiscovery(disc),
		WithBootstrapFallbackWait(time.Hour),
		WithHclogLogger(hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	old := c.raft()
	if err := c.ReinitForTest("hist"); err != nil {
		t.Fatalf("reinit: %v", err)
	}

	c.runBootstrapFallback(context.Background(), old, 0, tr)

	if !c.hasEmptyConfig() {
		t.Fatal("a fallback armed for the replaced raft bootstrapped its replacement")
	}
	if strings.Contains(out.String(), "bootstrap fallback") {
		t.Fatalf("a fallback armed for the replaced raft fired:\n%s", out.String())
	}
}

// newConfiglessVoter is a voter that skipped its bootstrap on a stale
// established-cluster ping and sits configless, its own startup fallback held
// off. window is its leader-ping window.
func newConfiglessVoter(ctx context.Context, t *testing.T, window time.Duration) (*Cache, *syncBuffer) {
	t.Helper()

	out := &syncBuffer{}
	disc := &announcingDiscovery{
		peerID: "stale-peer",
		status: PeerStatus{NumVotersInConfig: 2},
	}
	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, newSnapFSM(),
		WithLocalID("voter"),
		WithTransport(tr),
		WithDiscovery(disc),
		WithBootstrapFallbackWait(time.Hour),
		WithNoLeaderTimeout(window),
		WithHclogLogger(hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })
	return c, out
}

// TestBootstrapFallbackRechecksUnderLock: the fallback takes recoveryMutex
// before it bootstraps, which can mean waiting out a rebuild. A leader that
// pinged meanwhile must hold the bootstrap off without ending the fallback,
// and a raft shut down meanwhile must be neither bootstrapped nor given a
// fresh instance ID.
func TestBootstrapFallbackRechecksUnderLock(t *testing.T) {
	const window = time.Second

	t.Run("a leader pinged meanwhile", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		c, out := newConfiglessVoter(ctx, t, window)

		c.recoveryMutex.Lock()
		go c.runBootstrapFallback(ctx, c.raft(), 0, c.transport)
		time.Sleep(200 * time.Millisecond) // the fallback is due and waits for the lock
		c.discovery.sawLeaderPingAt.Store(time.Now().UnixNano())
		c.recoveryMutex.Unlock()

		time.Sleep(window / 2)
		if !c.hasEmptyConfig() || strings.Contains(out.String(), "bootstrap fallback") {
			t.Fatalf("the fallback bootstrapped despite a fresh leader ping:\n%s", out.String())
		}
		// Once that ping is stale, the fallback, still armed, breaks the wedge.
		ctxLead, cancelLead := context.WithTimeout(ctx, 5*time.Second)
		defer cancelLead()
		if !eventually(ctxLead, c.IsLeader) {
			t.Fatalf("the fallback never bootstrapped:\n%s", out.String())
		}
	})

	t.Run("the raft shut down meanwhile", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		c, out := newConfiglessVoter(ctx, t, window)
		rft, instanceID := c.raft(), c.getInstanceID()

		c.recoveryMutex.Lock()
		done := make(chan struct{})
		go func() {
			c.runBootstrapFallback(ctx, rft, 0, c.transport)
			close(done)
		}()
		time.Sleep(200 * time.Millisecond) // the fallback is due and waits for the lock
		_ = rft.Shutdown().Error()
		c.recoveryMutex.Unlock()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the fallback kept waiting on a shut-down raft")
		}
		if got := c.getInstanceID(); got != instanceID || strings.Contains(out.String(), "bootstrap fallback") {
			t.Fatalf("the fallback bootstrapped a shut-down raft (instance ID %q, now %q):\n%s", instanceID, got, out.String())
		}
	})
}

// TestBootstrapFallbackOutlivesALeaderThatWentAway: raft learns a leader from
// its first AppendEntries, before any configuration arrives, and that RPC also
// raises its term, after which raft refuses to bootstrap it. A leader that
// reached a configless voter and went away must neither end the fallback nor
// leave the voter unable to bootstrap (RT-15590).
func TestBootstrapFallbackOutlivesALeaderThatWentAway(t *testing.T) {
	const window = time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, out := newConfiglessVoter(ctx, t, window)
	voterTr, ok := c.transport.(*transports.InmemTransport)
	if !ok {
		t.Fatalf("unexpected transport %T", c.transport)
	}

	// One AppendEntries from a leader the voter has no log in common with:
	// rejected, but the leader and its term are taken.
	leaderAddr, leaderTr := transports.NewInmemTransport("")
	leaderTr.Connect(voterTr.LocalAddr(), voterTr)
	req := &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: raft.ProtocolVersionMax,
			ID:              []byte("gone-leader"),
			Addr:            leaderTr.EncodePeer("gone-leader", leaderAddr),
		},
		Term:         7,
		PrevLogEntry: 5,
		PrevLogTerm:  7,
	}
	var resp raft.AppendEntriesResponse
	if err := leaderTr.AppendEntries("voter", voterTr.LocalAddr(), req, &resp); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !c.hasLeader() || !c.hasEmptyConfig() {
		t.Fatal("precondition: expected a known leader and an empty configuration")
	}

	go c.runBootstrapFallback(ctx, c.raft(), 0, c.transport)

	ctxLead, cancelLead := context.WithTimeout(ctx, 10*time.Second)
	defer cancelLead()
	if !eventually(ctxLead, c.IsLeader) {
		t.Fatalf("the voter never bootstrapped after the leader went away:\n%s", out.String())
	}
}

// blockingSnapFSM is a blockingFSM that can snapshot, which
// raft.RecoverCluster needs.
type blockingSnapFSM struct {
	*blockingFSM
}

func (blockingSnapFSM) Snapshot() (raft.FSMSnapshot, error) { return snapFSMSnapshot(nil), nil }

// addGhostVoter adds a second voter, "ghost", that does not exist to c's
// configuration for quorum recovery to drop. The change cannot commit without
// it, but the latest configuration already lists it, and c, unable to reach
// it, steps down.
func addGhostVoter(ctx context.Context, t *testing.T, c *Cache) {
	t.Helper()

	_ = c.raft().AddVoter("ghost", "ghost", 0, 0)
	ctxWait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !eventually(ctxWait, func() bool { return c.raftHasServer("ghost") && !c.IsLeader() }) {
		t.Fatal("expected the ghost voter in the configuration and the node stepped down")
	}
}

// TestRebuildWaitsOutAWedgedFSM is the RT-15590 regression test for G2: on
// Site C quorum recovery gave up awaiting the old raft after 10s, while its
// FSM was stuck, and left the node on the raft it had already shut down. The
// watcher's retry needed shouldRecoverQuorum to agree again, which it did not
// for 32 minutes. A rebuild that shut the raft down must instead wait for it
// to exit and then complete, on both paths that rebuild on a live cluster.
func TestRebuildWaitsOutAWedgedFSM(t *testing.T) {
	tests := []struct {
		name    string
		rebuild func(c *Cache) error
		// rebuilt reports whether the rebuild did its work; instanceID is the
		// one before it.
		rebuilt func(c *Cache, instanceID string) bool
	}{
		{
			name:    "quorum recovery",
			rebuild: (*Cache).recoverQuorum,
			rebuilt: func(c *Cache, instanceID string) bool {
				return c.IsLeader() && !c.raftHasServer("ghost") && c.getInstanceID() != instanceID
			},
		},
		{
			name:    "hard reset",
			rebuild: func(c *Cache) error { return c.localHardReset("reset-1", "initiator-hist", false) },
			rebuilt: func(c *Cache, _ string) bool {
				return c.hasEmptyConfig() && c.getInstanceID() == "initiator-hist" && c.getLastResetID() == "reset-1"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			fsm := newBlockingFSM()
			c := newLeaderCache(ctx, t, blockingSnapFSM{fsm})
			release := wedgeApply(ctx, t, c, fsm)
			addGhostVoter(ctx, t, c)
			old, instanceID := c.raft(), c.getInstanceID()

			done := make(chan error, 1)
			go func() { done <- tt.rebuild(c) }()
			select {
			case err := <-done:
				t.Fatalf("the rebuild returned while the old raft was still running: %v", err)
			case <-time.After(time.Second):
			}
			if c.raft() != old {
				t.Fatal("the raft was replaced while the old one was still running")
			}

			release()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("rebuild: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("the rebuild never completed after the FSM returned")
			}
			ctxDone, cancelDone := context.WithTimeout(ctx, 10*time.Second)
			defer cancelDone()
			if !eventually(ctxDone, func() bool { return tt.rebuilt(c, instanceID) }) {
				t.Fatalf("the rebuild did not do its work (instance ID %q, configuration %v)",
					c.getInstanceID(), c.raft().GetConfiguration().Configuration().Servers)
			}
		})
	}
}

// TestRebuildWaitEndsWithTheCache: the wait for a wedged FSM is not bounded,
// but closing the cache must still end it rather than leak the rebuild.
func TestRebuildWaitEndsWithTheCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ctxCache, closeCache := context.WithCancel(ctx)
	defer closeCache()

	fsm := newBlockingFSM()
	c := newLeaderCache(ctxCache, t, blockingSnapFSM{fsm})
	wedgeApply(ctx, t, c, fsm)
	addGhostVoter(ctx, t, c)

	done := make(chan error, 1)
	go func() { done <- c.recoverQuorum() }()
	time.Sleep(200 * time.Millisecond)
	closeCache()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected the closed cache to end the wait, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wait outlived the cache")
	}
}

// TestRecoveredRaftDoesNotOutliveTheCache: Shutdown does not take
// recoveryMutex. One that runs while quorum recovery builds the recovered raft
// shuts the old raft down and returns, and recovery then installed the new one
// on the closed transport, where it ran for the life of the process. The
// recovered raft must be shut down by its installer.
func TestRecoveredRaftDoesNotOutliveTheCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ctxCache, closeCache := context.WithCancel(ctx)
	defer closeCache()

	c := newLeaderCache(ctxCache, t, newSnapFSM())
	addGhostVoter(ctx, t, c)

	c.newRaftFn = func(cfg options, fsm raft.FSM, logs raft.LogStore, stable raft.StableStore,
		snaps raft.SnapshotStore, tr transports.Transport,
	) (*raft.Raft, error) {
		closeCache()
		time.Sleep(200 * time.Millisecond) // the cache's shutdown runs on the old raft
		return newRaft(cfg, fsm, logs, stable, snaps, tr)
	}

	if err := c.recoverQuorum(); err != nil {
		t.Fatalf("recoverQuorum: %v", err)
	}
	if state := c.raft().State(); state != raft.Shutdown {
		t.Fatalf("the recovered raft outlived the cache: it is %v", state)
	}
}

// TestRecoveryRetriedUntilTheRaftRuns: by the time quorum recovery builds the
// recovered raft, RecoverCluster has rewritten the stores, so the history is
// forked whether the build succeeds or not. A failed build used to return
// before the instance ID was minted and the dropped voters were forgotten, and
// the node came back on the forked history under its old ID, where survivors
// would never be wiped (RT-15590). The recovery must be retried until the
// recovered raft runs, and finish every step after it.
func TestRecoveryRetriedUntilTheRaftRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	out := &syncBuffer{}
	c := newLeaderCache(ctx, t, newSnapFSM(),
		WithHclogLogger(hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Error})))
	addGhostVoter(ctx, t, c)
	c.discovery.setServer(raft.Server{ID: "ghost", Address: "ghost", Suffrage: raft.Voter})

	var calls atomic.Int32
	c.newRaftFn = func(cfg options, fsm raft.FSM, logs raft.LogStore, stable raft.StableStore,
		snaps raft.SnapshotStore, tr transports.Transport,
	) (*raft.Raft, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("failed to load any existing snapshots")
		}
		return newRaft(cfg, fsm, logs, stable, snaps, tr)
	}

	instanceID := c.getInstanceID()
	if err := c.recoverQuorum(); err != nil {
		t.Fatalf("recoverQuorum: %v", err)
	}
	if got := strings.Count(out.String(), "node is inert"); got != 1 {
		t.Fatalf("expected the failed build logged once, got %d:\n%s", got, out.String())
	}
	if c.getInstanceID() == instanceID {
		t.Fatal("the recovered raft runs under the old instance ID")
	}
	c.discovery.serversM.RLock()
	_, known := c.discovery.servers["ghost"]
	c.discovery.serversM.RUnlock()
	if known {
		t.Fatal("the dropped voter was not forgotten")
	}
	ctxLead, cancelLead := context.WithTimeout(ctx, 10*time.Second)
	defer cancelLead()
	if !eventually(ctxLead, func() bool { return c.IsLeader() && !c.raftHasServer("ghost") }) {
		t.Fatal("expected a recovered single-voter leader")
	}
}

// raftHasServer reports whether the latest raft configuration lists id.
func (s *Cache) raftHasServer(id raft.ServerID) bool {
	for _, srv := range s.raft().GetConfiguration().Configuration().Servers {
		if srv.ID == id {
			return true
		}
	}
	return false
}
