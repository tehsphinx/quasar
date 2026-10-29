package quasar_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/examples/generic/exampleFSM"
	"github.com/tehsphinx/quasar/transports"
)

// lockedBuffer is a goroutine-safe log sink shared by several caches.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestVotersResetIntoEmptyConfigsElectALeader is the RT-15590 end-to-end
// regression test, shaped like the 2026-10-28 cloud incident: of three voters
// one is gone (Site C, whose raft was dead) and the other two were reset onto
// fresh rafts with empty configurations by the fork-adoption chain (Sites A
// and B). Neither can campaign ("no known peers, aborting election") and
// nobody is left to replicate a configuration to them. Pre-fix that lasted
// until a process restart; now exactly one of them must bootstrap and admit
// the other, without a restart.
func TestVotersResetIntoEmptyConfigsElectALeader(t *testing.T) {
	const (
		// WithAutoPrune's minimum; it is also the liveness window the
		// bootstrap fallback ages leader pings out over.
		pruneAfter    = 6 * time.Second
		discoveryTick = 500 * time.Millisecond
		// Fallback grace, a stale leader ping, the lower-ID voter's head start,
		// an election and the leader's re-add sweep, with room for slow CI.
		recoverBudget = 40 * time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	bus := newInmemBus()
	out := &lockedBuffer{}
	ids := []string{"sitea", "siteb", "sitec"}

	addrs := map[string]raft.ServerAddress{}
	trs := map[string]*transports.InmemTransport{}
	for _, id := range ids {
		addrs[id], trs[id] = transports.NewInmemTransport("")
	}
	for id, tr := range trs {
		for otherID, otherTr := range trs {
			if id != otherID {
				tr.Connect(addrs[otherID], otherTr)
			}
		}
	}

	caches := map[string]*quasar.Cache{}
	for _, id := range ids {
		c, err := quasar.NewCache(ctx, exampleFSM.NewInMemoryFSM(),
			quasar.WithLocalID(id),
			quasar.WithTransport(trs[id]),
			quasar.WithDiscovery(newInmemDiscovery(bus, discoveryTick)),
			quasar.WithAutoPrune(pruneAfter),
			quasar.WithQuorumRecovery(pruneAfter),
			quasar.WithBootstrapFallbackWait(time.Second),
			quasar.WithHclogLogger(hclog.New(&hclog.LoggerOptions{Name: id, Output: out, Level: hclog.Warn})),
		)
		if err != nil {
			t.Fatalf("NewCache %s: %v", id, err)
		}
		caches[id] = c
	}
	defer func() {
		for _, c := range caches {
			_ = c.Shutdown()
		}
	}()

	for _, id := range ids {
		if err := caches[id].WaitReady(ctx); err != nil {
			t.Fatalf("WaitReady %s: %v", id, err)
		}
	}
	waitFor(t, 30*time.Second, "all three voters in the configuration", func() error {
		return hasVoters(caches["sitea"], "sitea", "siteb", "sitec")
	})

	// Site C's raft is gone.
	if err := caches["sitec"].Shutdown(); err != nil {
		t.Fatalf("Shutdown sitec: %v", err)
	}
	delete(caches, "sitec")

	// Reset the leader first, so the follower has no leader left to re-learn
	// a configuration from, then the follower: both now have empty
	// configurations on different histories.
	leader, follower := "sitea", "siteb"
	if caches["siteb"].IsLeader() {
		leader, follower = "siteb", "sitea"
	}
	mark := len(out.String())
	if err := caches[leader].ReinitForTest("history-c"); err != nil {
		t.Fatalf("reset %s: %v", leader, err)
	}
	if err := caches[follower].ReinitForTest("history-a"); err != nil {
		t.Fatalf("reset %s: %v", follower, err)
	}
	for _, id := range []string{"sitea", "siteb"} {
		if srvs, err := caches[id].GetServerList(); err != nil || len(srvs) != 0 {
			t.Fatalf("precondition: expected %s to have an empty configuration after the reset, got %v (%v)", id, srvs, err)
		}
	}

	waitFor(t, recoverBudget, "a leader with both reset voters", func() error {
		var lead *quasar.Cache
		for _, c := range caches {
			if c.IsLeader() {
				lead = c
			}
		}
		if lead == nil {
			return fmt.Errorf("no leader")
		}
		if err := hasVoters(lead, "sitea", "siteb"); err != nil {
			return err
		}
		if a, b := caches["sitea"].InstanceID(), caches["siteb"].InstanceID(); a != b {
			return fmt.Errorf("histories differ: sitea %q, siteb %q", a, b)
		}
		return nil
	})

	if n := strings.Count(out.String()[mark:], "bootstrap fallback"); n != 1 {
		t.Fatalf("expected exactly one fallback bootstrap after the reset, got %d:\n%s", n, out.String()[mark:])
	}
}

// hasVoters reports whether c's configuration lists every id as a voter.
func hasVoters(c *quasar.Cache, ids ...raft.ServerID) error {
	srvs, err := c.GetServerList()
	if err != nil {
		return err
	}
	voters := map[raft.ServerID]bool{}
	for _, s := range srvs {
		voters[s.ID] = s.Suffrage == raft.Voter
	}
	for _, id := range ids {
		if !voters[id] {
			return fmt.Errorf("%s is not a voter in %v", id, srvs)
		}
	}
	return nil
}

// waitFor polls cond until it returns nil or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() error) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s: %v", timeout, what, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
