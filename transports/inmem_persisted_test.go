// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/tehsphinx/quasar/pb/v1"
)

// TestInmemQueueHub_PublishConsumeReplySuccess verifies the happy path:
// a producer publish lands in the hub, the active consumer drains the
// item, ReplySuccess delivers the response back to the publisher.
func TestInmemQueueHub_PublishConsumeReplySuccess(t *testing.T) {
	asrt := is.New(t)

	hub := NewInmemQueueHub()
	_, producer := NewInmemTransport("")
	_, consumer := NewInmemTransport("")
	ConnectInmemQueueHub(hub, producer, consumer)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := consumer.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	// Run the consumer drain in a goroutine.
	drainErr := make(chan error, 1)
	go func() {
		for item := range ch {
			if string(item.Command().Key) != "k1" {
				drainErr <- errors.New("unexpected key")
				return
			}
			if e := item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 42}); e != nil {
				drainErr <- e
				return
			}
		}
		drainErr <- nil
	}()

	resp, err := producer.StorePersisted(ctx, &pb.Store{Key: "k1", Data: []byte("v1")}, PersistedStoreOpts{})
	asrt.NoErr(err)
	asrt.Equal(resp.Uid, uint64(42))

	asrt.NoErr(consumer.StopPersistedConsumer())
	asrt.NoErr(<-drainErr)
}

// TestInmemQueueHub_ReplyError surfaces consumer-side errors to the
// publisher unchanged.
func TestInmemQueueHub_ReplyError(t *testing.T) {
	asrt := is.New(t)

	hub := NewInmemQueueHub()
	_, producer := NewInmemTransport("")
	_, consumer := NewInmemTransport("")
	ConnectInmemQueueHub(hub, producer, consumer)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := consumer.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	applyErr := errors.New("fsm rejected payload")
	go func() {
		for item := range ch {
			_ = item.ReplyError(ctx, applyErr)
		}
	}()

	_, err = producer.StorePersisted(ctx, &pb.Store{Key: "k1"}, PersistedStoreOpts{})
	asrt.True(err != nil)
	asrt.Equal(err.Error(), applyErr.Error())

	asrt.NoErr(consumer.StopPersistedConsumer())
}

// TestInmemQueueHub_NackRequeuesToNextConsumer verifies that an in-flight
// item NAK'd by Stop is delivered to the next consumer claimant — the
// leader-flip scenario.
func TestInmemQueueHub_NackRequeuesToNextConsumer(t *testing.T) {
	asrt := is.New(t)

	hub := NewInmemQueueHub()
	_, producer := NewInmemTransport("")
	_, leaderA := NewInmemTransport("")
	_, leaderB := NewInmemTransport("")
	ConnectInmemQueueHub(hub, producer, leaderA, leaderB)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	chA, err := leaderA.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	// Publisher fires asynchronously; the publisher is blocked on the
	// reply, so we must not wait on it before the second consumer claims.
	publishResult := make(chan struct {
		resp *pb.StoreResponse
		err  error
	}, 1)
	go func() {
		r, e := producer.StorePersisted(ctx, &pb.Store{Key: "kFlip"}, PersistedStoreOpts{})
		publishResult <- struct {
			resp *pb.StoreResponse
			err  error
		}{r, e}
	}()

	// LeaderA receives the item, then steps down before replying. The
	// hub's stopConsumer requeues the item at the head.
	item := <-chA
	asrt.Equal(string(item.Command().Key), "kFlip")
	asrt.NoErr(leaderA.StopPersistedConsumer())

	// LeaderB claims the consumer and receives the requeued item.
	chB, err := leaderB.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	select {
	case item2 := <-chB:
		asrt.Equal(string(item2.Command().Key), "kFlip")
		asrt.NoErr(item2.ReplySuccess(ctx, &pb.StoreResponse{Uid: 7}))
	case <-time.After(2 * time.Second):
		t.Fatal("leaderB did not receive requeued item")
	}

	res := <-publishResult
	asrt.NoErr(res.err)
	asrt.Equal(res.resp.Uid, uint64(7))

	asrt.NoErr(leaderB.StopPersistedConsumer())
}

// TestInmemQueueHub_FIFOOrder verifies that producers fanning into the
// hub are drained in publish order — the single-in-flight invariant
// (one item delivered to the consumer at a time) is enough to preserve
// ordering as long as each publish lands on the queue before the next
// arrives.
func TestInmemQueueHub_FIFOOrder(t *testing.T) {
	asrt := is.New(t)

	hub := NewInmemQueueHub()
	_, producer := NewInmemTransport("")
	_, consumer := NewInmemTransport("")
	ConnectInmemQueueHub(hub, producer, consumer)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := consumer.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	var (
		seen   []string
		seenMu sync.Mutex
		done   = make(chan struct{})
	)
	go func() {
		defer close(done)
		for item := range ch {
			seenMu.Lock()
			seen = append(seen, string(item.Command().Key))
			seenMu.Unlock()
			_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: uint64(len(seen))})
		}
	}()

	const N = 8
	keys := make([]string, 0, N)
	for i := 0; i < N; i++ {
		k := "k" + time.Now().Format("150405.000000") + "_" + string(rune('a'+i))
		keys = append(keys, k)
		_, err := producer.StorePersisted(ctx, &pb.Store{Key: k}, PersistedStoreOpts{})
		asrt.NoErr(err)
	}

	asrt.NoErr(consumer.StopPersistedConsumer())
	<-done

	seenMu.Lock()
	defer seenMu.Unlock()
	asrt.Equal(len(seen), N)
	for i := range keys {
		asrt.Equal(seen[i], keys[i])
	}
}

// TestInmemQueueHub_PublisherCtxCancelDropsPending verifies that a
// publisher whose context is cancelled before the consumer drains its
// item leaves no ghost in the hub.
func TestInmemQueueHub_PublisherCtxCancelDropsPending(t *testing.T) {
	asrt := is.New(t)

	hub := NewInmemQueueHub()
	_, producer := NewInmemTransport("")
	_, consumer := NewInmemTransport("")
	ConnectInmemQueueHub(hub, producer, consumer)

	// Cancel the publish ctx without a running consumer; the item sits
	// in the queue with nobody draining it.
	publishCtx, cancelPublish := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelPublish()
	_, err := producer.StorePersisted(publishCtx, &pb.Store{Key: "ghost"}, PersistedStoreOpts{})
	asrt.True(errors.Is(err, context.DeadlineExceeded))

	// Start the consumer; it must NOT receive the cancelled item.
	consumeCtx, cancelConsume := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelConsume()
	ch, err := consumer.StartPersistedConsumer(consumeCtx)
	asrt.NoErr(err)

	select {
	case item, ok := <-ch:
		if ok {
			t.Fatalf("unexpected delivery after publisher cancellation: %v", item.Command().Key)
		}
	case <-time.After(200 * time.Millisecond):
		// Expected: nothing arrives.
	}

	asrt.NoErr(consumer.StopPersistedConsumer())
}
