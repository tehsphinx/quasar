// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"context"
	"fmt"
	"strings"
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
func newPersistedLeaderCache(ctx context.Context, t *testing.T, shards int, fsm FSM, opts ...Option) *Cache {
	t.Helper()

	_, tr := transports.NewInmemTransport("")
	transports.ConnectInmemQueueHub(transports.NewShardedInmemQueueHub(shards), tr)
	if !tr.SupportsPersisted() {
		t.Fatal("inmem transport is not in persisted-FIFO mode")
	}

	c, err := NewCache(ctx, fsm, append([]Option{
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
	c := newPersistedLeaderCache(ctx, t, shards, &stubFSM{}, WithPersistentStore(gate))

	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			_, _ = c.store(ctx, key, []byte("v"), WithShardKey(key))
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
	c := newPersistedLeaderCache(ctx, t, shards, &stubFSM{}, WithPersistentStore(gate))

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

// orderRecorder is one totally-ordered log of when a payload entered the
// leader's persist hook and when the FSM applied it. Both sides observe the
// same bytes: Cache.persist hands stores.PersistentStorage the pb.Store
// payload and fsmWrapper.store hands FSM.ApplyCmd the same field, so the two
// events can be matched without the test threading an id of its own.
type orderRecorder struct {
	m      sync.Mutex
	events []string

	inflight atomic.Int64
	peak     atomic.Int64
}

func (r *orderRecorder) record(phase, payload string) {
	r.m.Lock()
	r.events = append(r.events, phase+" "+payload)
	r.m.Unlock()
}

func (r *orderRecorder) snapshot() []string {
	r.m.Lock()
	defer r.m.Unlock()
	return append([]string(nil), r.events...)
}

// recordingPersistStore is a concurrency-safe stores.PersistentStorage that
// logs its calls and tracks how many were inside Store at once. The short park
// is what makes partitions actually overlap in the hook rather than merely
// being able to.
type recordingPersistStore struct {
	rec *orderRecorder
}

func (s *recordingPersistStore) Store(data stores.StoreData) error {
	n := s.rec.inflight.Add(1)
	for {
		peak := s.rec.peak.Load()
		if n <= peak || s.rec.peak.CompareAndSwap(peak, n) {
			break
		}
	}

	s.rec.record("persist", string(data.Data()))
	time.Sleep(time.Millisecond)

	s.rec.inflight.Add(-1)
	return nil
}

// recordingFSM logs every applied command into the same log as the persist
// hook. Everything else is stubFSM's behaviour.
type recordingFSM struct {
	stubFSM

	rec *orderRecorder
}

func (s *recordingFSM) ApplyCmd(cmd []byte) error {
	s.rec.record("apply", string(cmd))
	return nil
}

// TestPersistedPersistAndRaftStayInterleavedPerShardKey is the contract half of
// stores.PersistentStorage.Store: it MUST be safe for concurrent use, and calls
// that share a shard key are still mutually ordered.
//
// Concurrency is proved by the peak assertion plus -race: several partitions
// are genuinely inside Store at once, on different goroutines, hitting the
// leader's persist path together.
//
// Ordering is proved per shard key by the event log. Every write is issued
// from its own goroutine, so within one shard key the writers race each other
// all the way to the queue; the order they end up in is not defined, but the
// pairing is. One partition is applied by one goroutine, so its log must read
// persist X, apply X, persist Y, apply Y — a write's persist hook and its raft
// apply are never separated by another write's. That strict pairing is the
// only thing keeping the persistent store and the FSM from disagreeing about
// an entity, and it is exactly what a shard key that varies per call site
// throws away: publish the same writes under WithShardKey(payload) instead and
// they spread over partitions, overlap, and this fails.
func TestPersistedPersistAndRaftStayInterleavedPerShardKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		shards = 4
		// More keys than partitions, so several share one and several do not.
		keys = 8
		// Concurrent writers per key: enough that a partition applied by more
		// than one goroutine would interleave rather than pair.
		perKey = 6
	)

	rec := &orderRecorder{}
	c := newPersistedLeaderCache(ctx, t, shards, &recordingFSM{rec: rec},
		WithPersistentStore(&recordingPersistStore{rec: rec}))

	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		for i := 0; i < perKey; i++ {
			wg.Add(1)
			go func(k, i int) {
				defer wg.Done()
				key := fmt.Sprintf("key-%d", k)
				payload := fmt.Sprintf("%s#%d", key, i)
				if _, err := c.store(ctx, key, []byte(payload), WithShardKey(key)); err != nil {
					t.Errorf("store %s: %v", payload, err)
				}
			}(k, i)
		}
	}
	wg.Wait()

	if peak := rec.peak.Load(); peak < 2 {
		t.Fatalf("persist hooks never overlapped: peak in flight %d — the ordering assertions below would be vacuous", peak)
	}

	events := rec.snapshot()
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%d", k)

		var got []string
		for _, e := range events {
			if strings.Contains(e, " "+key+"#") {
				got = append(got, e)
			}
		}

		if len(got) != 2*perKey {
			t.Errorf("%s: recorded %d events, want %d: %v", key, len(got), 2*perKey, got)
			continue
		}
		for i := 0; i < len(got); i += 2 {
			persisted, ok := strings.CutPrefix(got[i], "persist ")
			if !ok || got[i+1] != "apply "+persisted {
				t.Errorf("%s: two writes to one shard key overlapped — %q is not followed by its own apply\nfull: %v",
					key, got[i], got)
				break
			}
		}
	}
}
