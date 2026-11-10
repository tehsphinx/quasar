package quasar

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/transports"
)

// stubFSM is a minimal FSM for internal tests; it counts Reset calls so a
// test can assert how often a hard reset actually wiped the node.
type stubFSM struct {
	resets atomic.Int32
}

func (s *stubFSM) Inject(*FSMInjector)         {}
func (s *stubFSM) ApplyCmd([]byte) error       { return nil }
func (s *stubFSM) Restore(io.ReadCloser) error { return nil }

func (s *stubFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, errors.New("stubFSM: no snapshots")
}

func (s *stubFSM) Reset() error {
	s.resets.Add(1)
	return nil
}

// newFollowerCache spins up a lone nonvoter — it never campaigns, so
// IsLeader() is deterministically false and localHardReset takes the
// follower path that tears down and rebuilds raft.
func newFollowerCache(ctx context.Context, t *testing.T) (*Cache, *stubFSM) {
	t.Helper()

	fsm := &stubFSM{}
	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx,
		fsm,
		WithLocalID("follower"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if c.IsLeader() {
		t.Fatal("lone nonvoter unexpectedly reports IsLeader() == true")
	}
	return c, fsm
}

// TestLocalHardResetSerializesWithRecovery is the RT-13042 C3 regression test.
//
// adoptLeaderInstance and recoverQuorum serialize raft teardown/rebuild on
// recoveryMutex; localHardReset did not. A hard-reset RPC racing a concurrent
// quorum recovery (or fork adoption — plausible exactly in the wedged states
// hard reset is for) could interleave two Shutdown/newStores/setRaft
// sequences: both create new raft instances, one overwrites the other in
// setRaft, and the loser keeps running, bound to the same shared transport
// consumer — two live rafts consuming one transport.
//
// This test holds recoveryMutex the way a recovery in flight would and
// asserts localHardReset waits for it instead of rebuilding raft concurrently.
// Pre-fix, localHardReset completes while the lock is held.
func TestLocalHardResetSerializesWithRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, _ := newFollowerCache(ctx, t)

	c.recoveryMutex.Lock()

	done := make(chan error, 1)
	go func() { done <- c.localHardReset("reset-1", "", false, "", 0) }()

	select {
	case <-done:
		c.recoveryMutex.Unlock()
		t.Fatal("localHardReset completed while a recovery held recoveryMutex")
	case <-time.After(300 * time.Millisecond):
	}

	c.recoveryMutex.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("localHardReset: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("localHardReset did not complete after recovery released recoveryMutex")
	}
}

// TestReinitRaftRebuildFailureRetriesAndLogs is the RT-13042 m5 regression
// test. After reinitRaftAdoptingInstance wipes the FSM and shuts the old raft
// down, a failing rebuild used to return immediately, leaving the node inert
// with no retry and no prominent log. rebuildRaft must log every failed
// attempt loudly and keep retrying until a raft runs: giving up after a few
// attempts left the node inert for good (RT-15590).
func TestReinitRaftRebuildFailureRetriesAndLogs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &syncBuffer{}
	logger := hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Error})

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, &stubFSM{},
		WithLocalID("follower"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
		WithHclogLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if c.IsLeader() {
		t.Fatal("lone nonvoter unexpectedly reports IsLeader() == true")
	}

	// More failures than the three attempts rebuildRaft used to give up after.
	const failures = 4
	var calls atomic.Int32
	c.newRaftFn = func(cfg options, fsm raft.FSM, logs raft.LogStore, stable raft.StableStore,
		snaps raft.SnapshotStore, tr transports.Transport,
	) (*raft.Raft, error) {
		if calls.Add(1) <= failures {
			return nil, errors.New("rebuild boom")
		}
		return newRaft(cfg, fsm, logs, stable, snaps, tr)
	}

	old := c.raft()
	if err := c.localHardReset("reset-1", "", false, "", 0); err != nil {
		t.Fatalf("expected the rebuild to be retried until it succeeds, got %v", err)
	}
	if got := strings.Count(out.String(), "node is inert"); got != failures {
		t.Fatalf("expected %d loud rebuild-failure logs, got %d:\n%s", failures, got, out.String())
	}
	if rft := c.raft(); rft == old || rft.State() == raft.Shutdown {
		t.Fatal("the node stayed on the shut-down raft")
	}
}

// TestHardResetReusesResetIDOnRetry is the RT-13042 m9 regression test.
// HardReset used to mint a fresh UUID on every call, so retrying after a
// partial fan-out failure re-wiped peers already reset by the prior attempt.
// The resetID must be reused while the last attempt has not fully succeeded,
// and a fresh one minted only after success.
func TestHardResetReusesResetIDOnRetry(t *testing.T) {
	c := &Cache{}

	first := c.hardResetID()
	if first == "" {
		t.Fatal("expected a resetID")
	}

	// Partial failure: the next attempt must reuse the same ID.
	c.recordHardResetResult(first, false)
	if got := c.hardResetID(); got != first {
		t.Fatalf("expected the failed attempt's resetID %q reused on retry, got %q", first, got)
	}

	// Success: the next attempt must mint a fresh ID.
	c.recordHardResetResult(first, true)
	if got := c.hardResetID(); got == first {
		t.Fatalf("expected a fresh resetID after a successful reset, got the old %q", got)
	}
}

// TestLocalHardResetAdoptsInitiatorInstanceID is the RT-13067 follow-up
// regression test. A follower hard reset used to clear its instance ID and
// rely on a later discovery ping of the leader to re-seed it (RT-13042 m4).
// Discovery delivery is best-effort, so an initiator crash inside that window
// left the follower without an instance ID — a recovered survivor's freshly
// minted ID was then silently adopted without the fork wipe, and the stale
// configuration never reconciled. The follower must adopt the initiator's
// instance ID directly at the wipe.
func TestLocalHardResetAdoptsInitiatorInstanceID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, _ := newFollowerCache(ctx, t)

	c.setInstanceID("stale-instance-id")

	if err := c.localHardReset("reset-1", "initiator-instance-id", false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}

	if got := c.getInstanceID(); got != "initiator-instance-id" {
		t.Fatalf("expected the initiator's instance ID adopted after follower hard reset, got %q", got)
	}
}

// TestLocalHardResetClearsInstanceID is the RT-13042 m4 regression test,
// scoped since RT-13067 to the fallback case of an initiator without an
// instance ID: the follower must not keep its stale ID (the next leader ping
// would mismatch and trigger a second, redundant full wipe via
// adoptLeaderInstance) but clear it, so it silently adopts the leader's ID
// from a later ping instead.
func TestLocalHardResetClearsInstanceID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, _ := newFollowerCache(ctx, t)

	c.setInstanceID("stale-instance-id")

	if err := c.localHardReset("reset-1", "", false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}

	if got := c.getInstanceID(); got != "" {
		t.Fatalf("expected instance ID cleared after follower hard reset, got %q", got)
	}
}

// TestLocalHardResetDeduplicatesConcurrentResets covers the second half of the
// RT-13042 C3 fix: the resetID dedup used to be a get-then-set across two
// separate lock acquisitions, so two concurrent identical resets could both
// pass the check and wipe the node twice (and, without recoveryMutex, rebuild
// raft twice concurrently). With the check-and-set folded into one critical
// section inside recoveryMutex, exactly one of N concurrent identical resets
// performs the wipe.
func TestLocalHardResetDeduplicatesConcurrentResets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, fsm := newFollowerCache(ctx, t)

	const concurrent = 8
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.localHardReset("reset-1", "", false, "", 0); err != nil {
				t.Errorf("localHardReset: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := fsm.resets.Load(); got != 1 {
		t.Fatalf("expected exactly 1 FSM reset for %d concurrent identical hard resets, got %d", concurrent, got)
	}
}

// resetProbeFSM records the state of the raft instance that was live at the
// time Reset ran.
type resetProbeFSM struct {
	stubFSM

	raftState    func() raft.RaftState
	probed       bool
	stateAtReset raft.RaftState
}

func (s *resetProbeFSM) Reset() error {
	// Only the first reset: the rebuild paths reset once, but a later
	// reset must not overwrite what the first one observed.
	if !s.probed {
		s.probed = true
		s.stateAtReset = s.raftState()
	}
	return s.stubFSM.Reset()
}

// TestFollowerHardResetAwaitsOldRaftShutdown is the RT-14147 regression test.
//
// reinitRaftAdoptingInstance used to wipe the FSM and swap the stores while the
// old raft's run/runFSM/runSnapshots goroutines were still running: a batch
// applied by the old runFSM landed in the freshly wiped FSM, and its deferred
// uidApplied overwrote lastApplied = 0 with a stale high index, so WaitFor
// returned early — stale reads until the new raft caught up. The teardown must
// be awaited (via the shutdown future, which is only safe because raft holds a
// transport whose Close() is hidden — see hideClose) before the FSM is touched.
//
// Pre-fix the FSM observes a live raft; post-fix it observes raft.Shutdown.
func TestFollowerHardResetAwaitsOldRaftShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := &resetProbeFSM{}
	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, fsm,
		WithLocalID("follower"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if c.IsLeader() {
		t.Fatal("lone nonvoter unexpectedly reports IsLeader() == true")
	}

	fsm.raftState = c.raft().State

	if err := c.localHardReset("reset-1", "", false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}

	if fsm.stateAtReset != raft.Shutdown {
		t.Fatalf("expected the old raft shut down before the FSM reset, it was %v", fsm.stateAtReset)
	}
}

// wedgeApply parks the raft's FSM goroutine inside a blockingFSM.ApplyCmd, so
// shutting that raft down cannot complete until the returned release runs.
// Cleanup releases it too, ahead of the cache's Shutdown, which would
// otherwise wait on the parked FSM goroutine forever when the test fails
// early. Call it after newLeaderCache: cleanups run last-registered first.
func wedgeApply(ctx context.Context, t *testing.T, c *Cache, fsm *blockingFSM) (release func()) {
	t.Helper()

	var once sync.Once
	release = func() { once.Do(func() { close(fsm.release) }) }
	t.Cleanup(release)

	go func() { _, _ = c.store(ctx, "wedge", []byte("me")) }()
	select {
	case <-fsm.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("FSM.ApplyCmd never ran")
	}
	return release
}

// resetFailFSM fails every Reset, i.e. the FSM clear a hard reset performs
// after the old raft is already down and unrecoverable.
type resetFailFSM struct {
	stubFSM
}

func (s *resetFailFSM) Reset() error {
	_ = s.stubFSM.Reset()
	return errors.New("resetFailFSM: reset failed")
}

// TestFollowerHardResetRebuildsRaftWhenFSMResetFails guards the fall-through:
// past the awaited shutdown there is no way back to the old raft, so returning
// on a reset error would leave the node inert with nothing to retry it (m5).
// It must rebuild instead and let the leader's heartbeats catch it up.
func TestFollowerHardResetRebuildsRaftWhenFSMResetFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := &resetFailFSM{}
	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, fsm,
		WithLocalID("follower"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.localHardReset("reset-1", "", false, "", 0); err != nil {
		t.Fatalf("localHardReset must not surface the reset error, got %v", err)
	}
	if fsm.resets.Load() != 1 {
		t.Fatalf("expected exactly one FSM reset attempt, got %d", fsm.resets.Load())
	}
	if state := c.raft().State(); state == raft.Shutdown {
		t.Fatal("raft was not rebuilt after the failed FSM reset; the node is inert")
	}
}

// TestAdmissionWipeRanksLeaders is the RT-14149 regression test. Two leaders
// from different histories that meet wipe each other through the discovery
// admission gate. A leader receiving that wipe used to clear its FSM and keep
// its log, snapshot and lastApplied, so the surviving leader served an empty
// FSM its log claimed was applied. Now the ranks decide: more voters wins, a
// tie goes to the lower server ID; the winner refuses and leaves the resetID
// unrecorded, the loser steps down through the follower rebuild.
func TestAdmissionWipeRanksLeaders(t *testing.T) {
	tests := []struct {
		name            string
		localID         string
		initiatorID     string
		initiatorVoters uint32
		refuse          bool
	}{
		{name: "tie, lower ID refuses", localID: "a", initiatorID: "b", initiatorVoters: 1, refuse: true},
		{name: "tie, higher ID steps down", localID: "b", initiatorID: "a", initiatorVoters: 1},
		{name: "more voters refuses despite higher ID", localID: "z", initiatorID: "a", initiatorVoters: 0, refuse: true},
		{name: "fewer voters steps down despite lower ID", localID: "a", initiatorID: "z", initiatorVoters: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fsm := &stubFSM{}
			c := newLeaderCache(ctx, t, fsm, WithLocalID(tt.localID))
			ownInst := c.getInstanceID()
			old := c.raft()

			err := c.localHardReset("initiator-inst", "initiator-inst", false, tt.initiatorID, tt.initiatorVoters)

			if tt.refuse {
				if !errors.Is(err, errOutranksInitiator) {
					t.Fatalf("expected errOutranksInitiator, got %v", err)
				}
				if got := fsm.resets.Load(); got != 0 {
					t.Fatalf("the outranking leader cleared its FSM %d times", got)
				}
				if c.raft() != old || !c.IsLeader() || c.getInstanceID() != ownInst {
					t.Fatal("the outranking leader lost its raft, leadership or instance ID")
				}
				if c.getLastResetID() == "initiator-inst" {
					t.Fatal("a refused wipe recorded its resetID; a later wipe with it would be skipped")
				}
				return
			}

			if err != nil {
				t.Fatalf("localHardReset: %v", err)
			}
			if c.raft() == old {
				t.Fatal("the outranked leader kept its raft instead of stepping down")
			}
			if got := c.getInstanceID(); got != "initiator-inst" {
				t.Fatalf("expected the initiator's instance ID adopted, got %q", got)
			}
			if got := fsm.resets.Load(); got != 1 {
				t.Fatalf("expected one FSM reset on the step-down, got %d", got)
			}
		})
	}
}

// TestOperatorHardResetKeepsLeaderRaft guards the operator path RT-14149 left
// alone: a hard reset without an initiator server ID (an operator HardReset,
// or a sender older than RT-14149) still clears only the leader's FSM and
// keeps its raft, whatever the initiator's instance ID.
func TestOperatorHardResetKeepsLeaderRaft(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := &stubFSM{}
	c := newLeaderCache(ctx, t, fsm)
	ownInst := c.getInstanceID()
	old := c.raft()

	if err := c.localHardReset("reset-1", "other-inst", false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}
	if got := fsm.resets.Load(); got != 1 {
		t.Fatalf("expected one FSM reset, got %d", got)
	}
	if c.raft() != old || !c.IsLeader() || c.getInstanceID() != ownInst {
		t.Fatal("the leader lost its raft, leadership or instance ID on an operator hard reset")
	}
}

// stringErrResetTransport answers every ResetCache with the given error
// flattened to a string, as the NATS transport delivers a receiver's error.
type stringErrResetTransport struct {
	transports.Transport
	err error
}

func (t *stringErrResetTransport) ResetCache(context.Context, raft.ServerID, raft.ServerAddress,
	*pb.ResetCache,
) (*pb.ResetCacheResponse, error) {
	return nil, errors.New(t.err.Error())
}

// TestSendHardResetRestoresRefusalSentinel guards the sender side of RT-14149:
// a refused admission wipe arrives over NATS as a plain string, and
// sendHardReset must turn it back into errOutranksInitiator so discovery logs
// the routine refusal at Info instead of as a failure.
func TestSendHardResetRestoresRefusalSentinel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, inmem := transports.NewInmemTransport("")
	tr := &stringErrResetTransport{Transport: inmem, err: errOutranksInitiator}
	c := newLeaderCache(ctx, t, &stubFSM{}, WithTransport(tr))

	err := c.sendHardReset(ctx, raft.Server{ID: "peer", Address: "peer"}, "reset-1", false, true)
	if !errors.Is(err, errOutranksInitiator) {
		t.Fatalf("sendHardReset returned %v, want errOutranksInitiator", err)
	}
}

// The instance IDs of two raft histories.
const (
	historyA = "history-a"
	historyB = "history-b"
)

// answeringStoreTransport answers every persisted Store with resp, the way a
// leader answers with an index of its log and its instance ID.
type answeringStoreTransport struct {
	transports.Transport
	resp *pb.StoreResponse
}

func (t *answeringStoreTransport) SupportsPersisted() bool { return true }

func (t *answeringStoreTransport) StorePersisted(context.Context, *pb.Store, transports.PersistedStoreOpts,
) (*pb.StoreResponse, error) {
	return t.resp, nil
}

// TestWaitTokenAcrossRaftRebuild covers a wait index that is kept across a
// rebuild of raft on a fresh log. Store answers with the index its write got in
// the leader's log, and an embedder waits for that index before it reads. After
// a fork adoption the node is in another history, whose log starts over: on
// rt74qa it went from index 151 763 to about 3 100, and every read that waited
// for the old index timed out for almost three hours (RT-15900). The token
// names the history, so such an index is refused instead of waited for. A hard
// reset inside one history leaves the leader's log as it is, and there the
// wiped node still has to reach the index before it reads.
func TestWaitTokenAcrossRaftRebuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, inmem := transports.NewInmemTransport("")
	tr := &answeringStoreTransport{
		Transport: inmem,
		resp:      &pb.StoreResponse{Uid: 151763, InstanceId: historyA},
	}
	c, err := NewCache(ctx, &stubFSM{},
		WithLocalID("follower"),
		WithTransport(tr),
		WithSuffrage(raft.Nonvoter),
		WithDiscovery(noopDiscovery{}),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })
	c.setInstanceID(historyA)

	token, err := c.store(ctx, "key", []byte("value"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if expected := (WaitToken{InstanceID: historyA, Index: 151763}); token != expected {
		t.Fatalf("store returned %+v, expected %+v", token, expected)
	}

	// A hard reset by the leader of the same history.
	if err := c.localHardReset("reset-1", historyA, false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancelWait()
	if err := c.fsm.waitFor(waitCtx, token); !errors.Is(err, ErrWaitFor) {
		t.Fatalf("wait on the wiped node returned %v, expected it to wait for the index", err)
	}
	c.fsm.uidApplied(token.Index)
	if err := c.fsm.waitFor(ctx, token); err != nil {
		t.Fatalf("wait with the index applied: %v", err)
	}

	// A fork adoption. The new log is behind the index at first and past it
	// later, and the index is one of this history in neither case.
	if err := c.localHardReset("reset-2", historyB, false, "", 0); err != nil {
		t.Fatalf("localHardReset: %v", err)
	}
	for _, applied := range []uint64{3100, token.Index} {
		c.fsm.uidApplied(applied)
		if err := c.fsm.waitFor(ctx, token); !errors.Is(err, errOtherHistory) {
			t.Fatalf("wait in another history at index %d returned %v, expected errOtherHistory", applied, err)
		}
	}
}

// TestWaitForTokenOfAnotherHistoryWaitsForLeaderLatest covers what a caller
// gets for a token of another history: a wait for the leader's latest index,
// as WaitForMasterLatest does, instead of a wait for an index this log is far
// from.
func TestWaitForTokenOfAnotherHistoryWaitsForLeaderLatest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, &stubFSM{},
		WithLocalID("leader"),
		WithTransport(tr),
		WithDiscovery(noopDiscovery{}),
		WithBootstrapWait(0),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	// The leader answers its own write with its instance ID.
	token, err := c.store(ctx, "key", []byte("value"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if token.InstanceID == "" || token.InstanceID != c.getInstanceID() || token.Index == 0 {
		t.Fatalf("store returned %+v, expected an index of instance %q", token, c.getInstanceID())
	}

	inj := &FSMInjector{cache: c}
	waitCtx, cancelWait := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancelWait()

	if err := inj.WaitFor(waitCtx, WaitToken{InstanceID: historyA, Index: 151763}); err != nil {
		t.Fatalf("WaitFor a token of another history: %v", err)
	}
	// The same index in this history is still waited for.
	err = inj.WaitFor(waitCtx, WaitToken{InstanceID: token.InstanceID, Index: 151763})
	if !errors.Is(err, ErrWaitFor) {
		t.Fatalf("WaitFor an index ahead of the log returned %v, expected it to wait for the index", err)
	}
}

// TestWaitTokenWithoutDiscovery covers a cluster without discovery. Nothing
// reconciles the instance ID there: every node keeps the one it minted at
// bootstrap, so the leader's differs from a follower's although both are in
// one history. A follower has to take the leader's index as it is.
func TestWaitTokenWithoutDiscovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	addr1, t1 := transports.NewInmemTransport("")
	addr2, t2 := transports.NewInmemTransport("")
	t1.Connect(addr2, t2)
	t2.Connect(addr1, t1)

	servers := []raft.Server{
		{ID: "cache1", Address: addr1, Suffrage: raft.Voter},
		{ID: "cache2", Address: addr2, Suffrage: raft.Voter},
	}
	var caches []*Cache
	for i, tr := range []*transports.InmemTransport{t1, t2} {
		c, err := NewCache(ctx, &stubFSM{},
			WithLocalID(string(servers[i].ID)),
			WithTransport(tr),
			WithServers(servers),
		)
		if err != nil {
			t.Fatalf("NewCache: %v", err)
		}
		t.Cleanup(func() { _ = c.Shutdown() })
		caches = append(caches, c)
	}
	for _, c := range caches {
		if err := c.WaitReady(ctx); err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
	}
	leader, follower := caches[0], caches[1]
	if follower.IsLeader() {
		leader, follower = follower, leader
	}

	// The reply of the leader carries its instance ID to the follower.
	token, err := follower.store(ctx, "key", []byte("value"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if token.InstanceID == "" || token.InstanceID != leader.getInstanceID() || token.Index == 0 {
		t.Fatalf("store returned %+v, expected an index of the leader's instance %q", token, leader.getInstanceID())
	}

	inj := &FSMInjector{cache: follower}
	if err := inj.WaitFor(ctx, token); err != nil {
		t.Fatalf("WaitFor the write: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancelWait()
	err = inj.WaitFor(waitCtx, WaitToken{InstanceID: token.InstanceID, Index: token.Index + 1000})
	if !errors.Is(err, ErrWaitFor) {
		t.Fatalf("WaitFor an index ahead of the log returned %v, expected it to wait for the index", err)
	}
}
