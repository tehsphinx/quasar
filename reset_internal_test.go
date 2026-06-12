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
	go func() { done <- c.localHardReset("reset-1", "") }()

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
// with no retry and no prominent log. rebuildRaft must retry a bounded number
// of times and log loudly before giving up.
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

	var calls atomic.Int32
	wantErr := errors.New("rebuild boom")
	c.newRaftFn = func(options, raft.FSM, raft.LogStore, raft.StableStore,
		raft.SnapshotStore, transports.Transport,
	) (*raft.Raft, error) {
		calls.Add(1)
		return nil, wantErr
	}

	if err := c.localHardReset("reset-1", ""); !errors.Is(err, wantErr) {
		t.Fatalf("expected the rebuild error to propagate, got %v", err)
	}
	if got := calls.Load(); got != rebuildRaftAttempts {
		t.Fatalf("expected %d rebuild attempts, got %d", rebuildRaftAttempts, got)
	}
	if !strings.Contains(out.String(), "node is inert") {
		t.Fatalf("expected a loud rebuild-failure log, got:\n%s", out.String())
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

	if err := c.localHardReset("reset-1", "initiator-instance-id"); err != nil {
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

	if err := c.localHardReset("reset-1", ""); err != nil {
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
			if err := c.localHardReset("reset-1", ""); err != nil {
				t.Errorf("localHardReset: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := fsm.resets.Load(); got != 1 {
		t.Fatalf("expected exactly 1 FSM reset for %d concurrent identical hard resets, got %d", concurrent, got)
	}
}
