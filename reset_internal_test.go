package quasar

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	go func() { done <- c.localHardReset("reset-1") }()

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

// TestLocalHardResetClearsInstanceID is the RT-13042 m4 regression test.
// A follower hard reset used to call reinitRaftAdoptingInstance("") while
// leaving its stale instance ID in place, so the next leader ping mismatched
// and triggered a second, redundant full wipe via adoptLeaderInstance.
// localHardReset must clear the instance ID so the follower silently adopts
// the leader's ID instead.
func TestLocalHardResetClearsInstanceID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, _ := newFollowerCache(ctx, t)

	c.setInstanceID("stale-instance-id")

	if err := c.localHardReset("reset-1"); err != nil {
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
			if err := c.localHardReset("reset-1"); err != nil {
				t.Errorf("localHardReset: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := fsm.resets.Load(); got != 1 {
		t.Fatalf("expected exactly 1 FSM reset for %d concurrent identical hard resets, got %d", concurrent, got)
	}
}
