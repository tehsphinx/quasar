// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

// gatingPersistStore is a stores.PersistentStorage that parks inside Store
// until released, recording the highest number of calls that were inside it at
// the same time. The persist hook runs on the leader before the raft proposal
// (see Cache.applyLocal), so it is where the per-shard apply concurrency
// becomes observable.
type gatingPersistStore struct {
	entered chan struct{}
	release chan struct{}

	inflight atomic.Int64
	peak     atomic.Int64
}

func newGatingPersistStore(capacity int) *gatingPersistStore {
	return &gatingPersistStore{
		entered: make(chan struct{}, capacity),
		release: make(chan struct{}),
	}
}

func (g *gatingPersistStore) Store(stores.StoreData) error {
	n := g.inflight.Add(1)
	for {
		peak := g.peak.Load()
		if n <= peak || g.peak.CompareAndSwap(peak, n) {
			break
		}
	}

	// Non-blocking: a full buffer means more calls arrived than the test asked
	// about, and dropping the surplus signal keeps Store from parking here
	// instead of on release (which would outlive the test's cleanup).
	select {
	case g.entered <- struct{}{}:
	default:
	}

	<-g.release
	g.inflight.Add(-1)
	return nil
}

// waitEntered blocks until n calls have entered Store.
func (g *gatingPersistStore) waitEntered(t *testing.T, n int, within time.Duration) {
	t.Helper()

	deadline := time.After(within)
	for i := 0; i < n; i++ {
		select {
		case <-g.entered:
		case <-deadline:
			t.Fatalf("only %d of %d persist hooks entered within %v (peak in flight %d)",
				i, n, within, g.peak.Load())
		}
	}
}

// newPersistedLeaderCache boots a lone leader whose transport is backed by a
// sharded in-memory persisted-FIFO hub, so a write takes the real
// publish -> partition -> per-partition apply path in-process.
func newPersistedLeaderCache(ctx context.Context, t *testing.T, shards int, opts ...Option) *Cache {
	t.Helper()

	_, tr := transports.NewInmemTransport("")
	transports.ConnectInmemQueueHub(transports.NewShardedInmemQueueHub(shards), tr)
	if !tr.SupportsPersisted() {
		t.Fatal("inmem transport is not in persisted-FIFO mode")
	}

	c, err := NewCache(ctx, &stubFSM{}, append([]Option{
		WithLocalID("solo"),
		WithTransport(tr),
		WithBootstrap(true),
	}, opts...)...)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if !c.IsLeader() {
		t.Fatal("lone bootstrapped voter is not leader")
	}
	return c
}

// TestPersistedApplyOverlapsAcrossShards is the RT-14337 throughput
// assertion: items from different FIFO partitions are applied concurrently.
// Before the change every shard's puller fed one shared channel drained by a
// single apply loop, so exactly one write cluster-wide was ever inside the
// persist hook or the raft commit round trip, and this peaked at 1.
func TestPersistedApplyOverlapsAcrossShards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		shards = 4
		// More distinct keys than partitions, so they span more than one.
		writes = 16
	)

	gate := newGatingPersistStore(writes)
	c := newPersistedLeaderCache(ctx, t, shards, WithPersistentStore(gate))

	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = c.store(ctx, "", []byte("v"), WithShardKey(fmt.Sprintf("key-%d", i)))
		}(i)
	}
	defer func() {
		close(gate.release)
		wg.Wait()
	}()

	gate.waitEntered(t, 2, 10*time.Second)
	if peak := gate.peak.Load(); peak < 2 {
		t.Fatalf("persist hooks never overlapped: peak in flight %d, want >= 2", peak)
	}
}

// TestPersistedApplyStaysSerialWithinAShard is the ordering half of the same
// change: everything published under one shard key lands in one partition, and
// a partition is applied by exactly one goroutine, so its items are never
// applied concurrently no matter how many are queued.
func TestPersistedApplyStaysSerialWithinAShard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		shards = 4
		writes = 8
	)

	gate := newGatingPersistStore(writes)
	c := newPersistedLeaderCache(ctx, t, shards, WithPersistentStore(gate))

	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.store(ctx, "", []byte("v"), WithShardKey("one-and-only"))
		}()
	}
	defer func() {
		close(gate.release)
		wg.Wait()
	}()

	gate.waitEntered(t, 1, 10*time.Second)
	// Give the other seven writes every chance to slip into the hook next to
	// the parked one before concluding they cannot.
	time.Sleep(500 * time.Millisecond)

	if peak := gate.peak.Load(); peak != 1 {
		t.Fatalf("same-shard writes overlapped in the persist hook: peak in flight %d, want 1", peak)
	}
}
