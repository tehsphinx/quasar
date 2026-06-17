// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tehsphinx/quasar/pb/v1"
)

// TestShardRouting_Deterministic covers the pure routing helpers (no NATS):
// a key always maps to the same shard, every shard is in range, and the
// subject/filter/durable names follow the per-shard convention (RT-12964).
func TestShardRouting_Deterministic(t *testing.T) {
	asrt := is.New(t)

	const shards = 20
	q := &natsPersistedQueue{
		subjectBase: "quasar.x.queue",
		subject:     "quasar.x.queue.msg",
		shards:      shards,
	}

	for _, key := range []string{"", "device:123", "chat:9", "member:42", "device:123"} {
		s := q.shardOf(key)
		asrt.True(s >= 0 && s < shards)            // in range
		asrt.Equal(s, q.shardOf(key))              // deterministic
		asrt.Equal(q.shardSubject(key),            // publish subject matches shard
			fmt.Sprintf("quasar.x.queue.s%d.msg", s))
	}

	asrt.Equal(q.shardOf(""), 0)                       // empty key → shard 0
	asrt.Equal(q.shardFilter(7), "quasar.x.queue.s7.>")
	asrt.Equal(q.shardDurable(3), "quasar-cache-consumer-s3")
}

// TestShardRouting_SingleShardBackCompat confirms an unsharded queue keeps the
// original subject, durable and shard-0 routing unchanged.
func TestShardRouting_SingleShardBackCompat(t *testing.T) {
	asrt := is.New(t)

	q := &natsPersistedQueue{
		subjectBase: "quasar.x.queue",
		subject:     "quasar.x.queue.msg",
		shards:      1,
	}

	asrt.Equal(q.shardOf("anything"), 0)
	asrt.Equal(q.shardSubject("anything"), "quasar.x.queue.msg")
	asrt.Equal(q.shardDurable(0), persistedConsumerNamePrefix)
}

// TestNATSPersistedQueue_ShardsDrainIndependently is the head-of-line-blocking
// guard (RT-12964): an item in flight on one shard that is never settled must
// not stop a write on a different shard from being delivered and applied.
// Skips when no JetStream server is available.
func TestNATSPersistedQueue_ShardsDrainIndependently(t *testing.T) {
	asrt := is.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const shards = 4
	tr := makeNATSPersistedTransport(ctx, t, "shardtest", "node-a", "quasar_shardtest_queue",
		WithPersistedShards(shards))

	// Find two keys that hash to different shards so the test exercises the
	// cross-shard path regardless of the hash.
	q := tr.queue
	keyStuck, keyFree := "k0", ""
	for i := 0; ; i++ {
		keyFree = fmt.Sprintf("k%d", i)
		if q.shardOf(keyFree) != q.shardOf(keyStuck) {
			break
		}
	}

	ch, err := tr.StartPersistedConsumer(ctx)
	asrt.NoErr(err)
	t.Cleanup(func() { _ = tr.StopPersistedConsumer() })

	// The apply loop keeps reading the merged channel. It deliberately never
	// settles the "stuck" shard's item, but settles everything else — so the
	// free shard can only succeed if it is not blocked behind the stuck one.
	freeDone := make(chan struct{})
	go func() {
		for item := range ch {
			if string(item.Command().Key) == keyStuck {
				continue // hold it in flight, never settle
			}
			_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 1})
			if string(item.Command().Key) == keyFree {
				close(freeDone)
			}
		}
	}()

	// Publish the stuck write (don't wait for its reply — it never comes).
	go func() {
		_, _ = tr.StorePersisted(ctx, &pb.Store{Key: keyStuck, Data: []byte("stuck")},
			PersistedStoreOpts{ShardKey: keyStuck})
	}()

	// The free-shard write must complete despite the stuck shard.
	freeCtx, freeCancel := context.WithTimeout(ctx, 8*time.Second)
	defer freeCancel()
	resp, err := tr.StorePersisted(freeCtx, &pb.Store{Key: keyFree, Data: []byte("free")},
		PersistedStoreOpts{ShardKey: keyFree})
	asrt.NoErr(err)
	asrt.Equal(resp.Uid, uint64(1))

	select {
	case <-freeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("free shard item was not applied; head-of-line blocked by the stuck shard")
	}
}

// TestNATSPersistedQueue_ShardCrashDoesNotStopOthers is the crash-isolation
// guard (RT-12964): when one shard's JetStream consumer dies server-side, that
// shard must rebuild itself in place while the OTHER shards keep delivering
// uninterrupted — a single crashed consumer no longer stops the whole queue.
// Skips when no JetStream server is available.
func TestNATSPersistedQueue_ShardCrashDoesNotStopOthers(t *testing.T) {
	asrt := is.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const shards = 4
	tr := makeNATSPersistedTransport(ctx, t, "crashtest", "node-a", "quasar_crashtest_queue",
		WithPersistedShards(shards))

	// Two keys on different shards: one whose consumer we crash, one that must
	// keep working throughout.
	q := tr.queue
	keyCrash, keySurvive := "k0", ""
	for i := 0; ; i++ {
		keySurvive = fmt.Sprintf("k%d", i)
		if q.shardOf(keySurvive) != q.shardOf(keyCrash) {
			break
		}
	}

	ch, err := tr.StartPersistedConsumer(ctx)
	asrt.NoErr(err)
	t.Cleanup(func() { _ = tr.StopPersistedConsumer() })

	go func() {
		for item := range ch {
			_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 1})
		}
	}()

	// Crash one shard's consumer the way an operator or stream rebuild would.
	js, err := jetstream.New(tr.conn)
	asrt.NoErr(err)
	asrt.NoErr(js.DeleteConsumer(ctx, q.streamName, q.shardDurable(q.shardOf(keyCrash))))

	// The surviving shard must deliver immediately — it is untouched by the
	// sibling crash, so this can't wait on any reconnect window.
	surviveCtx, surviveCancel := context.WithTimeout(ctx, 5*time.Second)
	defer surviveCancel()
	resp, err := tr.StorePersisted(surviveCtx, &pb.Store{Key: keySurvive, Data: []byte("survive")},
		PersistedStoreOpts{ShardKey: keySurvive})
	asrt.NoErr(err)
	asrt.Equal(resp.Uid, uint64(1))

	// The crashed shard rebuilds itself and resumes delivering too.
	resp, err = tr.StorePersisted(ctx, &pb.Store{Key: keyCrash, Data: []byte("crash")},
		PersistedStoreOpts{ShardKey: keyCrash})
	asrt.NoErr(err)
	asrt.Equal(resp.Uid, uint64(1))
}
