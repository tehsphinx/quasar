package quasar

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// pingingDiscovery keeps announcing one peer with a fixed status, the way a
// live peer's discovery pings keep arriving.
type pingingDiscovery struct {
	inj    *DiscoveryInjector
	peer   raft.Server
	status PeerStatus
}

func (d *pingingDiscovery) Inject(inj *DiscoveryInjector) { d.inj = inj }

func (d *pingingDiscovery) Run(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			d.inj.ProcessServerWithStatus(d.peer, d.status)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

// waitUntil polls cond until it holds or the timeout expires.
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLowerFormingVoters is the RT-15590 unit test of the fallback's rank:
// only alive voter peers with a lower ID whose last ping put them outside an
// established cluster count.
func TestLowerFormingVoters(t *testing.T) {
	d := newDiscoveryInjector(&Cache{localID: "sitec"})
	voter := func(id string) raft.Server {
		return raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(id), Suffrage: raft.Voter}
	}

	d.setServerWithStatus(voter("sitea"), PeerStatus{})                                      // configless: counts
	d.setServerWithStatus(voter("site1"), PeerStatus{NumVotersInConfig: 1})                  // bootstrapped, not elected: counts
	d.setServerWithStatus(voter("siteb"), PeerStatus{NumVotersInConfig: 2})                  // established
	d.setServerWithStatus(voter("site0"), PeerStatus{HasLeader: true})                       // follows a leader
	d.setServerWithStatus(voter("sited"), PeerStatus{})                                      // higher ID
	d.setServerWithStatus(raft.Server{ID: "fserver", Suffrage: raft.Nonvoter}, PeerStatus{}) // nonvoter
	d.setServerWithStatus(voter("site-gone"), PeerStatus{})                                  // stale
	d.lastSeen["site-gone"] = time.Now().Add(-time.Minute)

	if got := d.lowerFormingVoters(time.Second); got != 2 {
		t.Fatalf("expected 2 lower forming voters, got %d", got)
	}

	// Both have since elected themselves: they no longer count.
	d.setServerWithStatus(voter("sitea"), PeerStatus{HasLeader: true, NumVotersInConfig: 1})
	d.setServerWithStatus(voter("site1"), PeerStatus{HasLeader: true, NumVotersInConfig: 1})
	if got := d.lowerFormingVoters(time.Second); got != 0 {
		t.Fatalf("expected 0 lower forming voters once both lead, got %d", got)
	}
}

// TestBootstrapFallbackAfterReset is the RT-15590 regression test for G1: a
// voter whose raft was rebuilt by a history reset has an empty configuration
// and cannot campaign. Pre-fix nothing ever bootstrapped it again (Sites A and
// B logged "no known peers, aborting election" for 38 minutes); the reset must
// arm the bootstrap fallback, which waits one extra window behind a lower-ID
// voter that is configless too, and not behind one in an established cluster.
func TestBootstrapFallbackAfterReset(t *testing.T) {
	// Wide enough that a single-voter election after the bootstrap (raft's
	// heartbeat timeout, 1-2s here) fits under it: one extra window or none
	// must stay tellable apart.
	const window = 4 * time.Second

	tests := []struct {
		name       string
		peerStatus PeerStatus
		minWait    time.Duration
		maxWait    time.Duration
	}{
		{
			name:       "behind a configless lower-ID voter",
			peerStatus: PeerStatus{InstanceID: "hist-a"},
			minWait:    window,
			maxWait:    window + 3*time.Second,
		},
		{
			name:       "not behind a configured lower-ID voter",
			peerStatus: PeerStatus{NumVotersInConfig: 2, InstanceID: "hist-a"},
			minWait:    0,
			maxWait:    window - time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			out := &syncBuffer{}
			disc := &pingingDiscovery{
				peer:   raft.Server{ID: "sitea", Address: "sitea", Suffrage: raft.Voter},
				status: tt.peerStatus,
			}
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

			start := time.Now()
			if err := c.ReinitForTest("hist-c"); err != nil {
				t.Fatalf("reinit: %v", err)
			}
			if !c.hasEmptyConfig() {
				t.Fatal("expected an empty configuration right after the reset")
			}

			waitUntil(t, tt.maxWait+time.Second, "the reset voter to lead again", c.IsLeader)
			elapsed := time.Since(start)
			if elapsed < tt.minWait || elapsed > tt.maxWait {
				t.Fatalf("expected leadership between %s and %s after the reset, got %s", tt.minWait, tt.maxWait, elapsed)
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

// newWedgedRecoverableCache is a leader whose FSM is wedged inside ApplyCmd
// and whose configuration lists a second voter, "ghost", for quorum recovery
// to drop. The change cannot commit without it, but the latest configuration
// already lists it.
func newWedgedRecoverableCache(ctx context.Context, t *testing.T) (*Cache, *blockingFSM) {
	t.Helper()

	fsm := newBlockingFSM()
	c := newLeaderCache(ctx, t, blockingSnapFSM{fsm}, quickRaftShutdown())
	wedgeApply(ctx, t, c, fsm)

	_ = c.raft().AddVoter("ghost", "ghost", 0, 0)
	waitUntil(t, 5*time.Second, "the ghost voter in the configuration", func() bool {
		return c.raftHasServer("ghost")
	})
	return c, fsm
}

// TestRecoverQuorumFinishesAfterOldRaftExits is the RT-15590 regression test
// for G2 on Site C: quorum recovery timed out awaiting the old raft, which was
// already shut down, and the node sat on the dead raft for 32 minutes because
// the watcher's retry needed shouldRecoverQuorum to agree again. The recovery
// must be finished once the old raft has exited.
func TestRecoverQuorumFinishesAfterOldRaftExits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, fsm := newWedgedRecoverableCache(ctx, t)

	old := c.raft()
	if err := c.recoverQuorum(); !errors.Is(err, errAwaitRaftShutdown) {
		close(fsm.release)
		t.Fatalf("expected recovery to time out awaiting the wedged raft, got %v", err)
	}
	if old.State() != raft.Shutdown {
		close(fsm.release)
		t.Fatalf("expected the old raft shut down, it is %v", old.State())
	}

	close(fsm.release)

	waitUntil(t, 10*time.Second, "the recovered single-voter leader", func() bool {
		return c.raft() != old && c.IsLeader() && !c.raftHasServer("ghost")
	})
}

// TestRecoverQuorumRedoRejoinsLiveLeader: a recovery finished after the old
// raft exited must not fork a fresh history when a leader formed meanwhile;
// the two leaders would admit, and hard-reset, each other's members
// (RT-14149). It rejoins on fresh stores, keeping its instance ID.
func TestRecoverQuorumRedoRejoinsLiveLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, fsm := newWedgedRecoverableCache(ctx, t)

	old, instanceID := c.raft(), c.getInstanceID()
	if err := c.recoverQuorum(); !errors.Is(err, errAwaitRaftShutdown) {
		close(fsm.release)
		t.Fatalf("expected recovery to time out awaiting the wedged raft, got %v", err)
	}

	// A peer reports a live leader while the old raft is still exiting.
	c.discovery.sawLeaderPingAt.Store(time.Now().UnixNano())
	close(fsm.release)

	waitUntil(t, 10*time.Second, "the rejoin on fresh stores", func() bool {
		return c.raft() != old && c.hasEmptyConfig()
	})
	if got := c.getInstanceID(); got != instanceID {
		t.Fatalf("expected the instance ID %q kept, got %q", instanceID, got)
	}
}

// TestQueuedResetWinsOverRecovery: a reset and a quorum recovery that both
// timed out on the same old raft must leave the reset queued, whichever came
// first. The reset's resetID is already marked, so a retry of it would be
// deduplicated away and the reset lost.
func TestQueuedResetWinsOverRecovery(t *testing.T) {
	tests := []struct {
		name       string
		resetFirst bool
	}{
		{name: "reset after recovery"},
		{name: "reset before recovery", resetFirst: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			c, fsm := newWedgedRecoverableCache(ctx, t)

			reset := func() {
				if err := c.ReinitForTest("adopted"); !errors.Is(err, errAwaitRaftShutdown) {
					close(fsm.release)
					t.Fatalf("expected the reinit to time out awaiting the wedged raft, got %v", err)
				}
			}
			if tt.resetFirst {
				reset()
			}
			if err := c.recoverQuorum(); !errors.Is(err, errAwaitRaftShutdown) {
				close(fsm.release)
				t.Fatalf("expected recovery to time out awaiting the wedged raft, got %v", err)
			}
			if !tt.resetFirst {
				reset()
			}

			close(fsm.release)

			waitUntil(t, 10*time.Second, "the queued reset", func() bool {
				return c.getInstanceID() == "adopted" && c.hasEmptyConfig()
			})
		})
	}
}

// TestQueuedRebuildWinsOverCallerAfterExit: a rebuild that takes
// recoveryMutex after the old raft exited, ahead of the queued one, must defer
// to it. The quorum watcher's retry is such a caller: run in its place, it
// forked a fresh history and the queued reset was dropped as replaced.
func TestQueuedRebuildWinsOverCallerAfterExit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, fsm := newWedgedRecoverableCache(ctx, t)

	if err := c.ReinitForTest("adopted"); !errors.Is(err, errAwaitRaftShutdown) {
		close(fsm.release)
		t.Fatalf("expected the reinit to time out awaiting the wedged raft, got %v", err)
	}

	// The watcher's retry holds recoveryMutex when the old raft exits.
	c.recoveryMutex.Lock()
	exit := c.raftExit
	close(fsm.release)
	<-exit.done
	_, awaitErr := c.awaitRaftShutdown(exit.rft, time.Minute)
	recoverErr := c.recoverQuorumLocked()
	c.recoveryMutex.Unlock()
	if !errors.Is(awaitErr, errAwaitRaftShutdown) || !errors.Is(recoverErr, errAwaitRaftShutdown) {
		t.Fatalf("expected both to defer to the queued reset, got await %v, recovery %v", awaitErr, recoverErr)
	}

	waitUntil(t, 10*time.Second, "the queued reset", func() bool {
		return c.getInstanceID() == "adopted" && c.hasEmptyConfig()
	})
}

// TestReinitFinishesAfterOldRaftExits is the reset half of G2: a reinit that
// timed out awaiting the old raft must rebuild onto fresh stores, adopting the
// requested instance ID, once the old raft has exited.
func TestReinitFinishesAfterOldRaftExits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fsm := newBlockingFSM()
	c := newLeaderCache(ctx, t, fsm, quickRaftShutdown())
	wedgeApply(ctx, t, c, fsm)

	old := c.raft()
	if err := c.ReinitForTest("adopted"); !errors.Is(err, errAwaitRaftShutdown) {
		close(fsm.release)
		t.Fatalf("expected the reinit to time out awaiting the wedged raft, got %v", err)
	}
	if fsm.resets.Load() != 0 {
		close(fsm.release)
		t.Fatal("the FSM was reset before the old raft exited")
	}

	// A retry behind the queued rebuild must not hold recoveryMutex for
	// another full wait.
	start := time.Now()
	c.recoveryMutex.Lock()
	_, err := c.awaitRaftShutdown(old, time.Minute)
	c.recoveryMutex.Unlock()
	if !errors.Is(err, errAwaitRaftShutdown) || time.Since(start) > time.Second {
		close(fsm.release)
		t.Fatalf("expected a retry behind a queued rebuild to return at once, got %v after %s", err, time.Since(start))
	}

	close(fsm.release)

	waitUntil(t, 10*time.Second, "the reinit to complete", func() bool {
		rft := c.raft()
		return rft != old && rft.State() != raft.Shutdown &&
			c.getInstanceID() == "adopted" && fsm.resets.Load() == 1
	})
}

// TestRebuildAfterExitSkipsReplacedRaft: a pending rebuild must not run when
// another rebuild replaced the raft before the old one exited.
func TestRebuildAfterExitSkipsReplacedRaft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fsm := newBlockingFSM()
	c := newLeaderCache(ctx, t, fsm, quickRaftShutdown())
	wedgeApply(ctx, t, c, fsm)

	if err := c.ReinitForTest("adopted"); !errors.Is(err, errAwaitRaftShutdown) {
		close(fsm.release)
		t.Fatalf("expected the reinit to time out awaiting the wedged raft, got %v", err)
	}

	_, tr := transports.NewInmemTransport("")
	c.newStores()
	other, err := newRaft(c.cfg, c.fsm, c.logStore, c.stableStore, c.snapshotStore, tr)
	if err != nil {
		close(fsm.release)
		t.Fatalf("newRaft: %v", err)
	}
	c.recoveryMutex.Lock()
	c.setRaft(other)
	c.recoveryMutex.Unlock()

	close(fsm.release)
	time.Sleep(500 * time.Millisecond)

	if c.raft() != other {
		t.Fatal("the pending rebuild replaced a raft it was not queued for")
	}
	if fsm.resets.Load() != 0 {
		t.Fatalf("the pending rebuild reset the FSM %d times", fsm.resets.Load())
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
