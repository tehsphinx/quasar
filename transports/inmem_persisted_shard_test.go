// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"fmt"
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

// collectItems reads up to want items off the consumer channel, giving up
// after within. It never settles them, so each one occupies its partition's
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

// TestShardedInmemQueueHubDeliversPartitionsIndependently is what makes the
// cache's per-shard apply workers testable without a JetStream server: a
// sharded hub keeps one in-flight item PER PARTITION, so an unsettled item
// only blocks its own partition (RT-14337).
func TestShardedInmemQueueHubDeliversPartitionsIndependently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const shards = 4

	_, tr := NewInmemTransport("")
	ConnectInmemQueueHub(NewShardedInmemQueueHub(shards), tr)

	ch, err := tr.StartPersistedConsumer(ctx)
	if err != nil {
		t.Fatalf("StartPersistedConsumer: %v", err)
	}
	t.Cleanup(func() { _ = tr.StopPersistedConsumer() })

	// More distinct keys than partitions, so they span more than one.
	keys := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
	publishKeys(ctx, tr, keys)

	items := collectItems(t, ch, shards, 5*time.Second)
	if len(items) < 2 {
		t.Fatalf("hub delivered %d unsettled items, want at least 2 — partitions are not independent", len(items))
	}

	// Every delivered item must come from a distinct partition: a partition
	// with an unsettled item must not deliver a second one.
	seen := make(map[int]string, len(items))
	for _, item := range items {
		if prev, dup := seen[item.Shard()]; dup {
			t.Fatalf("shard %d delivered %q while %q was still unsettled",
				item.Shard(), item.Command().GetKey(), prev)
		}
		seen[item.Shard()] = item.Command().GetKey()
	}
}

// TestInmemQueueHubStaysSingleShard confirms the default hub is unchanged:
// one partition, one item in flight hub-wide, whatever the shard keys say.
func TestInmemQueueHubStaysSingleShard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, tr := NewInmemTransport("")
	ConnectInmemQueueHub(NewInmemQueueHub(), tr)

	ch, err := tr.StartPersistedConsumer(ctx)
	if err != nil {
		t.Fatalf("StartPersistedConsumer: %v", err)
	}
	t.Cleanup(func() { _ = tr.StopPersistedConsumer() })

	publishKeys(ctx, tr, []string{"a", "b", "c", "d"})

	items := collectItems(t, ch, 2, 2*time.Second)
	if len(items) != 1 {
		t.Fatalf("single-shard hub delivered %d unsettled items, want 1", len(items))
	}
	if got := items[0].Shard(); got != 0 {
		t.Fatalf("single-shard hub reported shard %d, want 0", got)
	}
}
