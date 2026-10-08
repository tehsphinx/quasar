// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

// TestPersistedConsumerStartWaitsForTheAppliedTail is the RT-14687 regression:
// a new leader must not start the persisted consumer while its FSM still lacks
// entries the previous leader committed. The consumer's apply path persists
// before it proposes, and a persister reading the FSM's state would otherwise
// merge onto a record that misses those writes.
//
// The follower's FSM parks on a committed write, then the follower is made
// leader. raft reports the new leadership while that write is still unapplied;
// the consumer must start only once it is.
func TestPersistedConsumerStartWaitsForTheAppliedTail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := newParkedTailCluster(ctx, t, true)
	tr := c.trs[c.follower]

	starts := tr.calls.Load()
	c.transferToFollower(ctx, t)

	time.Sleep(300 * time.Millisecond)
	if got := tr.calls.Load(); got != starts {
		t.Fatalf("consumer started over an unapplied tail (%d starts, want %d)", got, starts)
	}

	c.release()
	if !eventually(ctx, func() bool { return tr.calls.Load() > starts }) {
		t.Fatal("consumer never started once the tail was applied")
	}
}

// TestDirectStorePersistWaitsForTheAppliedTail: without persisted-FIFO a
// Store on the leader persists in applyLocal right away, with no consumer
// start to hold back, so persist itself must wait for the applied tail
// (RT-14687).
func TestDirectStorePersistWaitsForTheAppliedTail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	persists := &countingPersistStore{}
	c := newParkedTailCluster(ctx, t, false, WithPersistentStore(persists))
	c.transferToFollower(ctx, t)

	before := persists.calls.Load()
	stored := make(chan error, 1)
	go func() {
		_, err := c.caches[c.follower].store(ctx, "key", []byte("next"))
		stored <- err
	}()

	time.Sleep(300 * time.Millisecond)
	if got := persists.calls.Load(); got != before {
		t.Fatalf("persisted over an unapplied tail (%d persists, want %d)", got, before)
	}

	c.release()
	select {
	case err := <-stored:
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("store never completed once the tail was applied")
	}
	if got := persists.calls.Load(); got != before+1 {
		t.Fatalf("%d persists, want %d", got, before+1)
	}
}

// TestPersistedConsumerBarrierWaitEndsWithTheRaft: Barrier's timeout bounds
// only the enqueue, and a raft shut down while the barrier is still queued for
// its FSM never answers it. The barrier wait must end with the raft instance
// instead, or the leadership watcher never re-registers on the rebuilt raft.
// A watcher leaving its raft must not log that wait as a failed start either.
func TestPersistedConsumerBarrierWaitEndsWithTheRaft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	out := &syncBuffer{}
	logger := hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Error})

	fsm := newBlockingFSM()
	c := newLeaderCache(ctx, t, fsm, WithHclogLogger(logger))
	defer close(fsm.release)

	// Park the FSM so the barrier queues behind the write and is never answered.
	go func() {
		_, _ = c.store(ctx, "key", []byte("parked"))
	}()
	select {
	case <-fsm.entered:
	case <-ctx.Done():
		t.Fatal("write never reached the FSM")
	}

	rft, _ := c.getRaftWithCtx()
	ctxRaft, cancelRaft := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		c.watchLeadershipOnRaft(ctx, ctxRaft, rft)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancelRaft()

	select {
	case <-done:
	case <-time.After(applyTimeout / 2):
		t.Fatal("barrier wait outlived its raft")
	}
	if strings.Contains(out.String(), "failed to start persisted consumer") {
		t.Fatalf("watcher logged a failed start for the raft it left:\n%s", out.String())
	}
}

// TestApplyLocalWaitEndsWithTheRaft: an Apply future still queued for the FSM
// when its raft shuts down is never answered either. A Store waiting on it
// must end with the raft instance, or it holds its goroutine and its in-flight
// apply slot for good (RT-14687).
func TestApplyLocalWaitEndsWithTheRaft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fsm := newBlockingFSM()
	c := newLeaderCache(ctx, t, fsm)
	defer close(fsm.release)

	stored := make(chan error, 1)
	go func() {
		_, err := c.store(ctx, "key", []byte("parked"))
		stored <- err
	}()
	select {
	case <-fsm.entered:
	case <-ctx.Done():
		t.Fatal("write never reached the FSM")
	}

	// Installing the same raft again ends the instance's context the way a
	// replacement does, without a second raft to wait for.
	c.setRaft(c.raft())

	select {
	case err := <-stored:
		if !errors.Is(err, raft.ErrRaftShutdown) {
			t.Fatalf("store ended with %v, want raft.ErrRaftShutdown", err)
		}
	case <-time.After(applyTimeout / 2):
		t.Fatal("apply wait outlived its raft")
	}
}

// parkedTailCluster is a 3-node inmem cluster in which one follower's FSM is
// parked on a write the leader has committed. Every other FSM applies.
type parkedTailCluster struct {
	servers  []raft.Server
	trs      []*failingStartTransport
	caches   []*Cache
	release  func() // unparks the follower's FSM
	leader   int
	follower int
}

// newParkedTailCluster builds the cluster, with the persisted-FIFO queue when
// persisted is set, and parks the follower's FSM on a committed write.
func newParkedTailCluster(ctx context.Context, t *testing.T, persisted bool, opts ...Option) *parkedTailCluster {
	t.Helper()

	const n = 3
	addrs := make([]raft.ServerAddress, n)
	inners := make([]*transports.InmemTransport, n)
	servers := make([]raft.Server, n)
	for i := range n {
		addrs[i], inners[i] = transports.NewInmemTransport("")
		servers[i] = raft.Server{ID: raft.ServerID(fmt.Sprintf("node%d", i)), Address: addrs[i], Suffrage: raft.Voter}
	}
	for i := range n {
		for j := range n {
			if i != j {
				inners[i].Connect(addrs[j], inners[j])
			}
		}
	}
	if persisted {
		transports.ConnectInmemQueueHub(transports.NewInmemQueueHub(), inners...)
	}

	trs := make([]*failingStartTransport, n)
	fsms := make([]*blockingFSM, n)
	caches := make([]*Cache, n)
	releases := make([]func(), n)
	for i := range n {
		trs[i] = &failingStartTransport{InmemTransport: inners[i]}
		fsms[i] = newBlockingFSM()
		releases[i] = sync.OnceFunc(func() { close(fsms[i].release) })

		c, err := NewCache(ctx, fsms[i], append([]Option{
			WithLocalID(string(servers[i].ID)),
			WithTransport(trs[i]),
			WithServers(servers),
		}, opts...)...)
		if err != nil {
			t.Fatalf("NewCache %d: %v", i, err)
		}
		t.Cleanup(func() { _ = c.Shutdown() })
		caches[i] = c
	}
	// Registered last so it runs first: a parked FSM would hold Shutdown.
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
	})
	for i, c := range caches {
		if err := c.WaitReady(ctx); err != nil {
			t.Fatalf("WaitReady %d: %v", i, err)
		}
	}

	leader, follower := -1, -1
	for i, c := range caches {
		switch {
		case c.IsLeader():
			leader = i
		case follower < 0:
			follower = i
		}
	}
	if leader < 0 || follower < 0 {
		t.Fatalf("no leader/follower pair (leader %d, follower %d)", leader, follower)
	}

	// Every FSM but the follower's applies; the follower's parks on the write.
	for i := range n {
		if i != follower {
			releases[i]()
		}
	}
	if _, err := caches[leader].store(ctx, "key", []byte("tail")); err != nil {
		t.Fatalf("store: %v", err)
	}
	select {
	case <-fsms[follower].entered:
	case <-ctx.Done():
		t.Fatal("follower never reached the write")
	}

	return &parkedTailCluster{
		servers:  servers,
		trs:      trs,
		caches:   caches,
		release:  releases[follower],
		leader:   leader,
		follower: follower,
	}
}

// transferToFollower makes the parked follower leader. raft reports the new
// leadership while the parked write is still unapplied.
func (c *parkedTailCluster) transferToFollower(ctx context.Context, t *testing.T) {
	t.Helper()

	srv := c.servers[c.follower]
	if err := c.caches[c.leader].raft().LeadershipTransferToServer(srv.ID, srv.Address).Error(); err != nil {
		t.Fatalf("leadership transfer: %v", err)
	}
	if !eventually(ctx, c.caches[c.follower].IsLeader) {
		t.Fatal("follower never became leader")
	}
}

// countingPersistStore counts the persist calls of every node it is given to.
type countingPersistStore struct {
	calls atomic.Int32
}

func (s *countingPersistStore) Store(stores.StoreData) error {
	s.calls.Add(1)
	return nil
}

// eventually polls cond until it holds or ctx is done.
func eventually(ctx context.Context, cond func() bool) bool {
	for !cond() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
	return true
}
