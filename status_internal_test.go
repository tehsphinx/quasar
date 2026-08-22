package quasar

import (
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestDetermineClusterHealthNoUnderflow is the L7 regression test. CommitIndex
// and LastApplied are uint64; if LastApplied exceeds CommitIndex the lag
// subtraction wraps to a huge number and falsely reports the cluster
// unhealthy. A healthy node with LastApplied > CommitIndex must still report
// healthy.
func TestDetermineClusterHealthNoUnderflow(t *testing.T) {
	c := &Cache{}
	status := RaftStatus{
		HasLeader:     true,
		LeaderHealthy: true,
		NumVoters:     1,
		CommitIndex:   5,
		LastApplied:   6, // ahead of commit — pre-fix this underflows to ~2^64
	}

	if !c.determineClusterHealth(status) {
		t.Fatal("expected healthy when LastApplied > CommitIndex (lag underflow)")
	}
}

// TestVerifyLeaderBoundedTimesOut covers RT-13900: on a leader whose raft main
// goroutine is wedged, neither the verifyCh send nor the verify future is ever
// answered. The status path must answer anyway, reporting the leader — and with
// it the cluster — unhealthy, instead of hanging and taking the leader's
// monitoring series with it.
func TestVerifyLeaderBoundedTimesOut(t *testing.T) {
	c := &Cache{cfg: options{raftConfig: &raft.Config{HeartbeatTimeout: 20 * time.Millisecond}}}

	release := make(chan struct{})
	defer close(release)

	start := time.Now()
	if c.verifyLeaderBounded(func() error { <-release; return nil }) {
		t.Fatal("expected LeaderHealthy=false when the verify never answers")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("verify probe blocked for %s, want the 20ms heartbeat-timeout bound", elapsed)
	}

	// An unverified leader is what makes the cluster report unhealthy.
	if c.determineClusterHealth(RaftStatus{HasLeader: true, IsLeader: true, NumVoters: 1}) {
		t.Fatal("expected Healthy=false while the leader cannot be verified")
	}
}

// TestVerifyLeaderBoundedSingleFlight covers RT-13900: a probe that never
// answers must not be joined by one more parked goroutine per status request —
// over a multi-hour wedge that is hundreds of goroutines and as many entries in
// raft's leaderState.notify.
func TestVerifyLeaderBoundedSingleFlight(t *testing.T) {
	c := &Cache{cfg: options{raftConfig: &raft.Config{HeartbeatTimeout: 20 * time.Millisecond}}}

	release := make(chan struct{})
	defer close(release)

	const callers = 10
	started := make(chan struct{}, callers)
	verify := func() error {
		started <- struct{}{}
		<-release

		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if c.verifyLeaderBounded(verify) {
				t.Error("expected LeaderHealthy=false while the verify is stalled")
			}
		}()
	}
	wg.Wait()

	<-started // the one probe that was allowed to run
	if extra := len(started); extra != 0 {
		t.Fatalf("expected exactly one outstanding verify, got %d more", extra)
	}
}

// TestRaftStatsBeforeStart covers RT-13896: the metrics scrape calls RaftStats
// on whatever cache exists, including one whose raft instance is not up yet.
func TestRaftStatsBeforeStart(t *testing.T) {
	c := &Cache{}

	if stats := c.RaftStats(); stats != nil {
		t.Fatalf("expected nil stats before raft is started, got %v", stats)
	}
}
