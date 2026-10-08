// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
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

	const n = 3
	hub := transports.NewInmemQueueHub()
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
	transports.ConnectInmemQueueHub(hub, inners...)

	trs := make([]*failingStartTransport, n)
	fsms := make([]*blockingFSM, n)
	caches := make([]*Cache, n)
	releases := make([]func(), n)
	for i := range n {
		trs[i] = &failingStartTransport{InmemTransport: inners[i]}
		fsms[i] = newBlockingFSM()
		releases[i] = sync.OnceFunc(func() { close(fsms[i].release) })
		// Runs before the caches' Shutdown cleanups: a parked FSM would hold it.
		defer releases[i]()

		c, err := NewCache(ctx, fsms[i],
			WithLocalID(string(servers[i].ID)),
			WithTransport(trs[i]),
			WithServers(servers),
		)
		if err != nil {
			t.Fatalf("NewCache %d: %v", i, err)
		}
		t.Cleanup(func() { _ = c.Shutdown() })
		caches[i] = c
	}
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

	starts := trs[follower].calls.Load()
	fut := caches[leader].raft().LeadershipTransferToServer(servers[follower].ID, servers[follower].Address)
	if err := fut.Error(); err != nil {
		t.Fatalf("leadership transfer: %v", err)
	}
	if !eventually(ctx, caches[follower].IsLeader) {
		t.Fatal("follower never became leader")
	}

	time.Sleep(300 * time.Millisecond)
	if got := trs[follower].calls.Load(); got != starts {
		t.Fatalf("consumer started over an unapplied tail (%d starts, want %d)", got, starts)
	}

	releases[follower]()
	if !eventually(ctx, func() bool { return trs[follower].calls.Load() > starts }) {
		t.Fatal("consumer never started once the tail was applied")
	}
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
