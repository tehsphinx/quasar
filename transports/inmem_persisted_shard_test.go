// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/pb/v1"
)

// publishKeys fires one publish per key and returns without waiting for any
// reply — the items stay in flight so the test can inspect delivery.
func publishKeys(ctx context.Context, tr *InmemTransport, keys []string) {
	for _, key := range keys {
		go func(key string) {
			_, _ = tr.StorePersisted(ctx, &pb.Store{Key: key, Data: []byte(key)},
				PersistedStoreOpts{ShardKey: key})
		}(key)
	}
}

// collectItems reads up to want items off the sink channel, giving up after
// within. It never settles them, so each one occupies its partition's
// in-flight slot for the duration of the test.
func collectItems(t *testing.T, ch <-chan PersistedItem, want int, within time.Duration) []PersistedItem {
	t.Helper()

	items := make([]PersistedItem, 0, want)
	deadline := time.After(within)
	for len(items) < want {
		select {
		case item, ok := <-ch:
			if !ok {
				return items
			}
			items = append(items, item)
		case <-deadline:
			return items
		}
	}
	return items
}

// itemShard reads the partition an in-memory item was routed to. The public
// interface deliberately exposes no shard id — a consumer applies each
// partition on the goroutine the transport hands it and never needs one
// (RT-14337) — so a white-box read is how the partitioning is asserted.
func itemShard(t *testing.T, item PersistedItem) int {
	t.Helper()

	inmem, ok := item.(*inmemPersistedItem)
	if !ok {
		t.Fatalf("item is %T, want *inmemPersistedItem", item)
	}
	return inmem.shard
}

// TestShardedInmemQueueHubDeliversPartitionsIndependently is what makes the
// per-shard apply concurrency testable without a JetStream server: a sharded
// hub keeps one in-flight item PER PARTITION, so an unsettled item only blocks
// its own partition (RT-14337).
func TestShardedInmemQueueHubDeliversPartitionsIndependently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const shards = 4

	_, tr := NewInmemTransport("")
	ConnectInmemQueueHub(NewShardedInmemQueueHub(shards), tr)

	apply, sink := itemSink(shards)
	if _, err := tr.StartPersistedConsumer(ctx, apply); err != nil {
		t.Fatalf("StartPersistedConsumer: %v", err)
	}
	t.Cleanup(func() { _ = tr.StopPersistedConsumer() })

	// More distinct keys than partitions, so they span more than one.
	keys := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
	publishKeys(ctx, tr, keys)

	items := collectItems(t, sink, shards, 5*time.Second)
	if len(items) < 2 {
		t.Fatalf("hub delivered %d unsettled items, want at least 2 — partitions are not independent", len(items))
	}

	// Every delivered item must come from a distinct partition: a partition
	// with an unsettled item must not deliver a second one.
	seen := make(map[int]string, len(items))
	for _, item := range items {
		shard := itemShard(t, item)
		if prev, dup := seen[shard]; dup {
			t.Fatalf("shard %d delivered %q while %q was still unsettled",
				shard, item.Command().GetKey(), prev)
		}
		seen[shard] = item.Command().GetKey()
	}
}

// TestShardedInmemQueueHubOrdersWithinAPartition is the ordering half: writes
// that share a shard key land in one partition, and that partition's apply
// goroutine runs them one at a time in arrival order. Ordering within a shard
// is the delivery loop's own sequencing — nothing downstream has to preserve
// it (RT-14337).
func TestShardedInmemQueueHubOrdersWithinAPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const (
		shards = 4
		writes = 6
	)

	_, tr := NewInmemTransport("")
	ConnectInmemQueueHub(NewShardedInmemQueueHub(shards), tr)

	var (
		mu    sync.Mutex
		seen  []string
		peak  int
		busy  int
		apply = func(ctx context.Context, item PersistedItem) {
			mu.Lock()
			busy++
			if busy > peak {
				peak = busy
			}
			seen = append(seen, item.Command().GetKey())
			mu.Unlock()

			// Park briefly so a second concurrent apply on this partition
			// would be caught by the peak counter rather than missed.
			time.Sleep(10 * time.Millisecond)

			mu.Lock()
			busy--
			mu.Unlock()

			_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 1})
		}
	)

	if _, err := tr.StartPersistedConsumer(ctx, apply); err != nil {
		t.Fatalf("StartPersistedConsumer: %v", err)
	}
	t.Cleanup(func() { _ = tr.StopPersistedConsumer() })

	// Publish sequentially so arrival order is the test's own, all under one
	// shard key so they share a partition.
	want := make([]string, 0, writes)
	for i := 0; i < writes; i++ {
		key := fmt.Sprintf("ordered-%d", i)
		want = append(want, key)
		if _, err := tr.StorePersisted(ctx, &pb.Store{Key: key},
			PersistedStoreOpts{ShardKey: "one-and-only"}); err != nil {
			t.Fatalf("StorePersisted %s: %v", key, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if peak != 1 {
		t.Fatalf("same-partition items applied concurrently: peak %d, want 1", peak)
	}
	if len(seen) != writes {
		t.Fatalf("applied %d items, want %d: %v", len(seen), writes, seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("partition applied out of order: got %v, want %v", seen, want)
		}
	}
}

// TestInmemQueueHubStaysSingleShard confirms the default hub is unchanged:
// one partition, one item in flight hub-wide, whatever the shard keys say.
func TestInmemQueueHubStaysSingleShard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, tr := NewInmemTransport("")
	ConnectInmemQueueHub(NewInmemQueueHub(), tr)

	apply, sink := itemSink(2)
	if _, err := tr.StartPersistedConsumer(ctx, apply); err != nil {
		t.Fatalf("StartPersistedConsumer: %v", err)
	}
	t.Cleanup(func() { _ = tr.StopPersistedConsumer() })

	publishKeys(ctx, tr, []string{"a", "b", "c", "d"})

	items := collectItems(t, sink, 2, 2*time.Second)
	if len(items) != 1 {
		t.Fatalf("single-shard hub delivered %d unsettled items, want 1", len(items))
	}
	if got := itemShard(t, items[0]); got != 0 {
		t.Fatalf("single-shard hub routed to shard %d, want 0", got)
	}
}
