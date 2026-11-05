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

// TestNATSPersistedQueue_StartWithUnavailableShard is the RT-15781 guard: a new
// leader whose start finds one shard's consumer impossible to create must still
// drain every other shard, and the blocked shard must start draining on its own
// once its consumer can be created. Skips when no JetStream server is available.
func TestNATSPersistedQueue_StartWithUnavailableShard(t *testing.T) {
	asrt := is.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tr := makeNATSPersistedTransport(ctx, t, "starttest", "node-a",
		fmt.Sprintf("quasar_starttest_%d", time.Now().UnixNano()), WithPersistedShards(4))
	keyBlocked, keyFree := keysOnTwoShards(tr.queue)
	unblock := blockShardConsumer(ctx, t, tr.queue, tr.queue.shardOf(keyBlocked))

	_, err := tr.StartPersistedConsumer(ctx, func(ctx context.Context, item PersistedItem) {
		_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 1})
	})
	asrt.NoErr(err) // one blocked shard must not fail the start
	t.Cleanup(func() { _ = tr.StopPersistedConsumer() })

	freeCtx, freeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer freeCancel()
	resp, err := tr.StorePersisted(freeCtx, &pb.Store{Key: keyFree, Data: []byte("free")},
		PersistedStoreOpts{ShardKey: keyFree})
	asrt.NoErr(err) // the other shards drain right away
	asrt.Equal(resp.Uid, uint64(1))

	unblock()
	resp, err = tr.StorePersisted(ctx, &pb.Store{Key: keyBlocked, Data: []byte("blocked")},
		PersistedStoreOpts{ShardKey: keyBlocked})
	asrt.NoErr(err) // the blocked shard recovers without a restart
	asrt.Equal(resp.Uid, uint64(1))
}

// TestNATSPersistedQueue_StopWhileShardRetrying covers losing leadership while a
// shard that could not start is still retrying: every puller exits and no pull
// subscription is left behind (RT-15781). Skips when no JetStream server is
// available.
func TestNATSPersistedQueue_StopWhileShardRetrying(t *testing.T) {
	asrt := is.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tr := makeNATSPersistedTransport(ctx, t, "stoptest", "node-a",
		fmt.Sprintf("quasar_stoptest_%d", time.Now().UnixNano()), WithPersistedShards(4))
	blockShardConsumer(ctx, t, tr.queue, 1)
	subs := tr.conn.NumSubscriptions()

	done, err := tr.StartPersistedConsumer(ctx, func(ctx context.Context, item PersistedItem) {
		_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 1})
	})
	asrt.NoErr(err)
	time.Sleep(500 * time.Millisecond) // let shard 1 fail a reconnect attempt or two

	asrt.NoErr(tr.StopPersistedConsumer())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer group did not stop while a shard was retrying")
	}
	for deadline := time.Now().Add(2 * time.Second); tr.conn.NumSubscriptions() != subs; {
		if time.Now().After(deadline) {
			t.Fatalf("pull subscriptions left after stop: %d, want %d", tr.conn.NumSubscriptions(), subs)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNATSPersistedQueue_StartFailsWhenNoShardConnects keeps the start failing
// when no shard's consumer can be created, so the cache retries it as before
// RT-15781; a later start succeeds once the consumer can be created. Skips when
// no JetStream server is available.
func TestNATSPersistedQueue_StartFailsWhenNoShardConnects(t *testing.T) {
	asrt := is.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tr := makeNATSPersistedTransport(ctx, t, "nostarttest", "node-a",
		fmt.Sprintf("quasar_nostarttest_%d", time.Now().UnixNano()))
	unblock := blockShardConsumer(ctx, t, tr.queue, 0)
	apply := func(ctx context.Context, item PersistedItem) {
		_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 1})
	}

	_, err := tr.StartPersistedConsumer(ctx, apply)
	asrt.True(err != nil) // the only shard is blocked

	unblock()
	_, err = tr.StartPersistedConsumer(ctx, apply)
	asrt.NoErr(err)
	asrt.NoErr(tr.StopPersistedConsumer())
}

// keysOnTwoShards returns two routing keys that hash to different shards.
func keysOnTwoShards(q *natsPersistedQueue) (string, string) {
	for i := 1; ; i++ {
		if k := fmt.Sprintf("k%d", i); q.shardOf(k) != q.shardOf("k0") {
			return "k0", k
		}
	}
}

// blockShardConsumer makes the stream refuse shard's durable consumer.
// Work-queue retention allows only one consumer per subject, so a second
// consumer on the shard's filter (an unfiltered one for a single-shard queue)
// keeps the shard's CreateOrUpdateConsumer failing until it is deleted. Returns
// the func that deletes it.
func blockShardConsumer(ctx context.Context, t *testing.T, q *natsPersistedQueue, shard int) func() {
	t.Helper()

	stream, err := q.ensureStream(ctx)
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	cfg := jetstream.ConsumerConfig{Durable: "blocker", AckPolicy: jetstream.AckExplicitPolicy}
	if q.shards > 1 {
		cfg.FilterSubject = q.shardFilter(shard)
	}
	if _, err := stream.CreateOrUpdateConsumer(ctx, cfg); err != nil {
		t.Fatalf("create blocker consumer: %v", err)
	}
	return func() {
		if err := stream.DeleteConsumer(ctx, cfg.Durable); err != nil {
			t.Fatalf("delete blocker consumer: %v", err)
		}
	}
}
