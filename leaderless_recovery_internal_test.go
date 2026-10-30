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

// quickRaftShutdown makes rebuilds give up on a wedged old raft quickly.
func quickRaftShutdown() Option {
	return func(o *options) { o.awaitRaftShutdownTimeout = 200 * time.Millisecond }
}

// newWedgedRecoverableCache is a node whose FSM is wedged inside ApplyCmd and
// whose configuration lists a second voter, "ghost", for quorum recovery to
// drop. The change cannot commit without it, but the latest configuration
// already lists it, and the node, unable to reach it, steps down.
func newWedgedRecoverableCache(ctx context.Context, t *testing.T) (*Cache, *blockingFSM, func()) {
	t.Helper()

	fsm := newBlockingFSM()
	c := newLeaderCache(ctx, t, blockingSnapFSM{fsm}, quickRaftShutdown())
	release := wedgeApply(ctx, t, c, fsm)

	_ = c.raft().AddVoter("ghost", "ghost", 0, 0)
	ctxWait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !eventually(ctxWait, func() bool { return c.raftHasServer("ghost") && !c.IsLeader() }) {
		t.Fatal("expected the ghost voter in the configuration and the node stepped down")
	}
	return c, fsm, release
}

// awaitRestart waits until the raft old was replaced by a live one.
func awaitRestart(ctx context.Context, t *testing.T, c *Cache, old *raft.Raft) {
	t.Helper()

	ctxWait, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if !eventually(ctxWait, func() bool {
		rft := c.raft()
		return rft != old && rft.State() != raft.Shutdown
	}) {
		t.Fatal("the node stayed on the shut-down raft")
	}
}

// TestRecoveryTimeoutRestartsRaft is the RT-15590 regression test for G2 on
// Site C: quorum recovery timed out awaiting the old raft, which was already
// shut down, and the node sat on the dead raft for 32 minutes because the
// watcher's retry needed shouldRecoverQuorum to agree again. Once the old raft
// has exited the node must run a live raft on its untouched stores, on which
// recovery can be decided again.
func TestRecoveryTimeoutRestartsRaft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, _, release := newWedgedRecoverableCache(ctx, t)

	old, instanceID := c.raft(), c.getInstanceID()
	if err := c.recoverQuorum(); !errors.Is(err, errAwaitRaftShutdown) {
		t.Fatalf("expected recovery to time out awaiting the wedged raft, got %v", err)
	}
	if old.State() != raft.Shutdown {
		t.Fatalf("expected the old raft shut down, it is %v", old.State())
	}

	release()
	awaitRestart(ctx, t, c, old)
	if !c.raftHasServer("ghost") {
		t.Fatal("the restart lost the configuration the recovery had not changed")
	}
	if got := c.getInstanceID(); got != instanceID {
		t.Fatalf("the restart changed the instance ID from %q to %q", instanceID, got)
	}

	if err := c.recoverQuorum(); err != nil {
		t.Fatalf("recovery on the restarted raft: %v", err)
	}
	ctxLead, cancelLead := context.WithTimeout(ctx, 10*time.Second)
	defer cancelLead()
	if !eventually(ctxLead, func() bool { return c.IsLeader() && !c.raftHasServer("ghost") }) {
		t.Fatal("expected a recovered single-voter leader")
	}
}

// TestHardResetRetryAppliesAfterTimeout is the reset half of G2: a hard reset
// that timed out awaiting the old raft must leave the node on its old history
// and must not be recorded, so the initiator's retry, once the old raft has
// exited, applies it. Recorded, the retry was deduplicated away and the leader
// went on to add a node that never reset.
func TestHardResetRetryAppliesAfterTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, fsm, release := newWedgedRecoverableCache(ctx, t)

	old, instanceID := c.raft(), c.getInstanceID()
	if err := c.localHardReset("reset-1", "initiator-hist", false); !errors.Is(err, errAwaitRaftShutdown) {
		t.Fatalf("expected the reset to time out awaiting the wedged raft, got %v", err)
	}
	if got := c.getLastResetID(); got == "reset-1" {
		t.Fatal("the reset was recorded although it was not applied")
	}
	if got := c.getInstanceID(); got != instanceID || fsm.resets.Load() != 0 {
		t.Fatalf("the reset changed the node before the old raft exited (instance ID %q, FSM resets %d)", got, fsm.resets.Load())
	}

	release()
	awaitRestart(ctx, t, c, old)
	if got := c.getInstanceID(); got != instanceID {
		t.Fatalf("the restart changed the instance ID from %q to %q", instanceID, got)
	}

	if err := c.localHardReset("reset-1", "initiator-hist", false); err != nil {
		t.Fatalf("retried reset: %v", err)
	}
	if got := c.getInstanceID(); got != "initiator-hist" || fsm.resets.Load() != 1 || !c.hasEmptyConfig() {
		t.Fatalf("expected the retry applied (instance ID %q, FSM resets %d, empty configuration %v)",
			got, fsm.resets.Load(), c.hasEmptyConfig())
	}
	if got := c.getLastResetID(); got != "reset-1" {
		t.Fatalf("expected the applied reset recorded, got %q", got)
	}
}

// TestRestartRaftSkipsReplacedRaft: the restart after a timed-out rebuild must
// not run when another rebuild replaced the raft before the old one exited.
func TestRestartRaftSkipsReplacedRaft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fsm := newBlockingFSM()
	c := newLeaderCache(ctx, t, fsm, quickRaftShutdown())
	release := wedgeApply(ctx, t, c, fsm)

	if err := c.ReinitForTest("adopted"); !errors.Is(err, errAwaitRaftShutdown) {
		t.Fatalf("expected the reinit to time out awaiting the wedged raft, got %v", err)
	}

	_, tr := transports.NewInmemTransport("")
	c.newStores()
	other, err := newRaft(c.cfg, c.fsm, c.logStore, c.stableStore, c.snapshotStore, tr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	c.recoveryMutex.Lock()
	c.setRaft(other)
	c.recoveryMutex.Unlock()

	release()
	time.Sleep(500 * time.Millisecond)

	if c.raft() != other {
		t.Fatal("the restart replaced a raft it was not for")
	}
	if fsm.resets.Load() != 0 {
		t.Fatalf("the FSM was reset %d times", fsm.resets.Load())
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
