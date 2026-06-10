package quasar

import (
	"context"
	"testing"
	"time"
)

// TestRunPersistedConsumerNilRaftWaitsAndExits guards the RT-13042 m7 fix.
// The rft == nil branch of runPersistedConsumer used to `continue` immediately
// (a tight CPU spin); it now waits ~100ms between retries, mirroring
// runQuorumRecoveryWatch. The spin itself is a CPU property without a directly
// assertable failure (the path is unreachable in production), so this guards
// the control flow instead: while raft stays nil and the context is live the
// loop keeps running, and it returns promptly once the context is canceled.
func TestRunPersistedConsumerNilRaftWaitsAndExits(t *testing.T) {
	c := &Cache{ctxRaft: context.Background()} // raftLocker nil → rft == nil branch

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		c.runPersistedConsumer(ctx)
		close(done)
	}()

	select {
	case <-done:
		cancel()
		t.Fatal("runPersistedConsumer returned while raft was nil and ctx live")
	case <-time.After(300 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runPersistedConsumer did not return after context cancel")
	}
}
