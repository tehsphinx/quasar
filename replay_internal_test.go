package quasar

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
	"google.golang.org/protobuf/proto"
)

// replayFSM counts what reaches it as news and what as a replay.
type replayFSM struct {
	snapStubFSM
	applied  atomic.Int32
	replayed atomic.Int32
}

func (f *replayFSM) ApplyCmd([]byte) error {
	f.applied.Add(1)
	return nil
}

func (f *replayFSM) ReplayCmd([]byte) error {
	f.replayed.Add(1)
	return nil
}

func (f *replayFSM) expect(t *testing.T, step string, applied, replayed int32) {
	t.Helper()
	if got := f.applied.Load(); got != applied {
		t.Errorf("%s: %d commands applied as news, expected %d", step, got, applied)
	}
	if got := f.replayed.Load(); got != replayed {
		t.Errorf("%s: %d commands applied as replays, expected %d", step, got, replayed)
	}
}

func storeLog(t *testing.T, index, term uint64) *raft.Log {
	t.Helper()

	bts, err := proto.Marshal(cmdStore("", []byte("command")))
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: bts}
}

// TestReplayerGetsThePastAsReplays walks the wrapper through the life of a
// process: what it applies while it is catching up at start, what raft hands it
// again after a rebuild on the same history, and what comes after adopting
// another history, where the same indexes are other entries (RT-15906).
func TestReplayerGetsThePastAsReplays(t *testing.T) {
	fsm := &replayFSM{}
	w := wrapFSM(fsm)
	apply := func(term, from, to uint64) {
		for i := from; i <= to; i++ {
			w.Apply(storeLog(t, i, term))
		}
	}

	apply(1, 1, 2)
	fsm.expect(t, "catching up at start", 0, 2)

	// The cluster stood at 3, in term 1, when the process learned it.
	w.endStartup(3, 1, w.startupEpoch())
	apply(1, 3, 4)
	fsm.expect(t, "after the start", 1, 3)

	w.replayUpTo(w.replayedSoFar()) // raft rebuilt on the same history after index 4
	apply(1, 1, 5)
	fsm.expect(t, "rebuilt on the same history", 2, 7)

	// A node that was behind won the election after the rebuild and writes
	// under indexes this process has applied, in its new term.
	w.replayUpTo(w.replayedSoFar())
	apply(1, 1, 3)
	apply(2, 4, 5)
	fsm.expect(t, "rebuilt on a log a later leader went on writing", 4, 10)

	w.replayUpTo(0, 0) // raft rebuilt on another history
	apply(1, 1, 2)
	fsm.expect(t, "rebuilt on another history", 6, 10)

	if wrapFSM(&stubFSM{}).starting.Load() {
		t.Error("an FSM that is no Replayer has no replay to start in")
	}
}

// TestReplayBoundaryIsWhatTheProcessKnows covers what a rebuild on the same
// history keeps as the boundary.
func TestReplayBoundaryIsWhatTheProcessKnows(t *testing.T) {
	// A process still catching up is rebuilt: the past it was told of reaches
	// further than what it has applied, and stays the past.
	w := wrapFSM(&replayFSM{})
	w.endStartup(9, 2, w.startupEpoch())
	w.Apply(storeLog(t, 1, 1))
	if index, term := w.replayedSoFar(); index != 9 || term != 2 {
		t.Errorf("catching up: boundary %d in term %d, expected 9 in term 2", index, term)
	}

	// The index of a noop counts as applied when it is stored. A later leader
	// can still put a command there, which this process has not applied.
	w = wrapFSM(&replayFSM{})
	w.Apply(storeLog(t, 4, 1))
	w.regSystemUID(5)
	if got := w.getLastApplied(); got != 5 {
		t.Fatalf("precondition: lastApplied is %d, expected the noop's 5", got)
	}
	if index, term := w.replayedSoFar(); index != 4 || term != 1 {
		t.Errorf("behind a stored noop: boundary %d in term %d, expected 4 in term 1", index, term)
	}

	// The leader answers with its last index while raft is rebuilt on a log
	// that starts over: as a boundary there, that index would mute the news.
	w = wrapFSM(&replayFSM{})
	epoch := w.startupEpoch()
	w.replayUpTo(0, 0)
	if w.endStartup(9, 1, epoch) || !w.starting.Load() || w.replayUntil.Load() != 0 {
		t.Error("an index of the log before the rebuild ended the startup replay")
	}

	// The indexes start over with the log, and so does what was applied.
	w = wrapFSM(&replayFSM{})
	w.Apply(storeLog(t, 7, 3))
	w.applyRaftReset()
	if index, term := w.replayedSoFar(); index != 0 || term != 0 {
		t.Errorf("after the raft reset: boundary %d in term %d, expected none", index, term)
	}
}

// TestFreshFollowerReplaysTheClustersPast covers the process that joins a
// running cluster: what the cluster had is a replay to it, what is written once
// it has caught up is news. The follower never calls WaitReady before the
// startup replay ends.
func TestFreshFollowerReplaysTheClustersPast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := newLeaderCache(ctx, t, &replayFSM{})
	leaderTr, ok := leader.transport.(*transports.InmemTransport)
	if !ok {
		t.Fatalf("unexpected transport %T", leader.transport)
	}

	const past = 3
	for range past {
		if _, err := leader.store(ctx, "key", []byte("value")); err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	fsm := &replayFSM{}
	addr, tr := transports.NewInmemTransport("")
	tr.Connect(leaderTr.LocalAddr(), leaderTr)
	leaderTr.Connect(addr, tr)
	follower, err := NewCache(ctx, fsm,
		WithLocalID("follower"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = follower.Shutdown() })

	if err := leader.raft().AddNonvoter("follower", addr, 0, 0).Error(); err != nil {
		t.Fatalf("AddNonvoter: %v", err)
	}
	if !eventually(ctx, func() bool { return fsm.replayed.Load() == past && !follower.fsm.starting.Load() }) {
		t.Fatalf("the follower did not replay the past and leave the startup replay: %d replayed, starting %t",
			fsm.replayed.Load(), follower.fsm.starting.Load())
	}
	fsm.expect(t, "caught up", 0, past)

	if _, err := leader.store(ctx, "key", []byte("value")); err != nil {
		t.Fatalf("store: %v", err)
	}
	if !eventually(ctx, func() bool { return fsm.applied.Load() == 1 }) {
		t.Fatal("the follower was not told of a write after it had caught up")
	}
	fsm.expect(t, "a write after the catch-up", 1, past)
}

// TestRecoverQuorumReplaysWhatWasApplied covers the quorum recovery. raft
// re-applies the log behind the newest snapshot, here the whole log, on a
// process that has applied it once already.
func TestRecoverQuorumReplaysWhatWasApplied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fsm := &replayFSM{}
	c := newLeaderCache(ctx, t, fsm)
	if c.fsm.starting.Load() {
		t.Fatal("WaitReady returned in the startup replay")
	}

	const writes = 3
	for range writes {
		if _, err := c.store(ctx, "key", []byte("value")); err != nil {
			t.Fatalf("store: %v", err)
		}
	}
	fsm.expect(t, "writes", writes, 0)

	addGhostVoter(ctx, t, c)
	if err := c.recoverQuorum(); err != nil {
		t.Fatalf("recoverQuorum: %v", err)
	}
	fsm.expect(t, "quorum recovery", writes, writes)

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady after the recovery: %v", err)
	}
	if _, err := c.store(ctx, "key", []byte("value")); err != nil {
		t.Fatalf("store after the recovery: %v", err)
	}
	fsm.expect(t, "a write after the recovery", writes+1, writes)
}

// newReplayFollower returns a lone nonvoter on history-a that has applied a
// command at index 7 in term 2.
func newReplayFollower(ctx context.Context, t *testing.T) *Cache {
	t.Helper()

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, &replayFSM{},
		WithLocalID("follower"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	c.setInstanceID("history-a")
	c.fsm.Apply(storeLog(t, 7, 2))
	return c
}

func expectBoundary(t *testing.T, c *Cache, step string, index, term uint64) {
	t.Helper()
	if got, gotTerm := c.fsm.replayUntil.Load(), c.fsm.replayTerm.Load(); got != index || gotTerm != term {
		t.Errorf("%s: replays up to %d in term %d, expected %d in term %d", step, got, gotTerm, index, term)
	}
}

// TestRebuildReplaysOnlyOnTheSameHistory covers the follower rebuild. A hard
// reset from the history the follower is on makes it fetch the log it has
// applied; one that makes it adopt another history brings entries it has not
// seen under the same indexes.
func TestRebuildReplaysOnlyOnTheSameHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c := newReplayFollower(ctx, t)
	if err := c.localHardReset("reset-1", "history-a", false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}
	expectBoundary(t, c, "rebuilt on the same history", 7, 2)

	// A second reset before anything came back: the boundary stays.
	if err := c.localHardReset("reset-2", "history-a", false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}
	expectBoundary(t, c, "rebuilt again before catching up", 7, 2)

	if err := c.localHardReset("reset-3", "history-b", false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}
	expectBoundary(t, c, "rebuilt on another history", 0, 0)
}

// TestBootstrapFallbackStartsWithoutReplays covers the history that starts on
// the node itself. A follower rebuilt on its leader's history keeps the boundary
// while it waits to be added again. When that leader never comes back, the
// bootstrap fallback starts a history on a log that starts over, and its first
// entries must not pass for the ones under the same indexes before.
func TestBootstrapFallbackStartsWithoutReplays(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c := newReplayFollower(ctx, t)
	if err := c.localHardReset("reset-1", "history-a", false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}
	if got := c.fsm.replayUntil.Load(); got != 7 {
		t.Fatalf("rebuilt on the same history: replays up to %d, expected 7", got)
	}

	due := func() bool { return true }
	if !c.tryFallbackBootstrap(c.raft(), due, time.Second, time.Second, c.transport) {
		t.Fatal("the bootstrap fallback did not bootstrap")
	}
	expectBoundary(t, c, "a history started here", 0, 0)
}

// flakySnapshotFSM is a replayFSM whose first snapshots fail.
type flakySnapshotFSM struct {
	replayFSM
	failSnapshots atomic.Int32
}

func (f *flakySnapshotFSM) Snapshot() (raft.FSMSnapshot, error) {
	if f.failSnapshots.Add(-1) >= 0 {
		return nil, errors.New("flakySnapshotFSM: snapshot failed")
	}
	return f.replayFSM.Snapshot()
}

// TestRetriedRecoveryReplaysWhatTheFailedAttemptApplied covers a quorum
// recovery that is retried. RecoverCluster applies the log before it takes its
// snapshot, so an attempt that fails there has applied the stored tail this
// process had not applied before. The retry applies it again, and by then it
// is no news.
func TestRetriedRecoveryReplaysWhatTheFailedAttemptApplied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fsm := &flakySnapshotFSM{}
	c := newLeaderCache(ctx, t, fsm)

	const writes = 3
	for range writes {
		if _, err := c.store(ctx, "key", []byte("value")); err != nil {
			t.Fatalf("store: %v", err)
		}
	}
	addGhostVoter(ctx, t, c)

	// A command that was stored and never applied.
	last, err := c.logStore.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if err := c.logStore.StoreLog(storeLog(t, last+1, c.raft().CurrentTerm())); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}

	fsm.failSnapshots.Store(1)
	if err := c.recoverQuorum(); err != nil {
		t.Fatalf("recoverQuorum: %v", err)
	}
	// The first attempt replays the writes and applies the tail as news, the
	// second replays all four.
	fsm.expect(t, "a recovery that needed two attempts", writes+1, 2*writes+1)
}
