package quasar

import (
	"context"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/transports"
)

// fakePersistedItem is a minimal transports.PersistedItem stub for exercising
// the apply-path decision helpers without a live raft.
type fakePersistedItem struct {
	retry       bool
	deadline    time.Time
	hasDeadline bool
}

func (f fakePersistedItem) Command() *pb.Store                                  { return &pb.Store{} }
func (f fakePersistedItem) Retry() bool                                         { return f.retry }
func (f fakePersistedItem) Deadline() (time.Time, bool)                         { return f.deadline, f.hasDeadline }
func (fakePersistedItem) ReplySuccess(context.Context, *pb.StoreResponse) error { return nil }
func (fakePersistedItem) ReplyError(context.Context, error) error               { return nil }
func (fakePersistedItem) Nack(context.Context) error                            { return nil }
func (fakePersistedItem) NackWithDelay(context.Context) error                   { return nil }

var _ transports.PersistedItem = fakePersistedItem{}

// TestPersistedExpired covers the RT-12964 time-protection decision: a
// non-retry write whose publisher deadline has passed must be dropped, while
// retry writes and writes that are still within (or have no) deadline must
// proceed to apply.
func TestPersistedExpired(t *testing.T) {
	now := time.Unix(1_000, 0)
	past := now.Add(-time.Second)
	future := now.Add(time.Second)

	cases := []struct {
		name string
		item fakePersistedItem
		want bool
	}{
		{"non-retry past deadline drops", fakePersistedItem{retry: false, deadline: past, hasDeadline: true}, true},
		{"non-retry future deadline applies", fakePersistedItem{retry: false, deadline: future, hasDeadline: true}, false},
		{"non-retry no deadline applies", fakePersistedItem{retry: false, hasDeadline: false}, false},
		{"retry past deadline applies", fakePersistedItem{retry: true, deadline: past, hasDeadline: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := persistedExpired(tc.item, now); got != tc.want {
				t.Fatalf("persistedExpired = %v, want %v", got, tc.want)
			}
		})
	}
}

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
