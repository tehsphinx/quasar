package quasar

import "testing"

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
