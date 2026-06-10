// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tehsphinx/quasar/pb/v1"
)

// makeNATSPersistedTransport connects to the local NATS server with
// JetStream enabled and returns a transport configured for
// persisted-FIFO mode. Skips the test when NATS or JetStream isn't
// available locally.
func makeNATSPersistedTransport(ctx context.Context, t *testing.T, cacheName, serverName, streamName string,
	queueOpts ...PersistedQueueOption,
) *NATSTransport {
	t.Helper()

	nc, err := nats.Connect(natsURL, nats.Timeout(natsTimeout))
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Skipf("JetStream not available: %v", err)
	}
	if _, err := js.AccountInfo(ctx); err != nil {
		nc.Close()
		t.Skipf("JetStream not enabled on server: %v", err)
	}

	tr, err := NewNATSTransport(
		ctx, nc, cacheName, serverName,
		WithNATSLogger(newTestLogger(t)),
		WithNATSTimeout(natsTimeout),
		WithNATSPersistedQueue(streamName, queueOpts...),
	)
	if err != nil {
		nc.Close()
		t.Fatalf("create persisted transport: %v", err)
	}
	t.Cleanup(func() {
		// Clean up the test stream so reruns don't accumulate state.
		_ = js.DeleteStream(context.Background(), streamName)
		nc.Close()
	})
	return tr
}

// TestNATSPersistedQueue_PublishConsumeReply verifies the publish →
// consume → ReplySuccess round-trip across the JetStream work-queue.
func TestNATSPersistedQueue_PublishConsumeReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	asrt := is.New(t)

	streamName := fmt.Sprintf("quasar_test_queue_%d", time.Now().UnixNano())
	// Both the producer and the leader bind to the same stream — that's
	// the design (every voter shares the stream; leader claims the
	// consumer).
	producer := makeNATSPersistedTransport(ctx, t, "test-cache", "producer", streamName)
	leader := makeNATSPersistedTransport(ctx, t, "test-cache", "leader", streamName)

	asrt.True(producer.SupportsPersisted())
	asrt.True(leader.SupportsPersisted())

	ch, err := leader.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	drainErr := make(chan error, 1)
	go func() {
		for item := range ch {
			if string(item.Command().Key) != "lastseen" {
				drainErr <- fmt.Errorf("unexpected key %q", item.Command().Key)
				return
			}
			if e := item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 99}); e != nil {
				drainErr <- e
				return
			}
		}
		drainErr <- nil
	}()

	resp, err := producer.StorePersisted(ctx, &pb.Store{Key: "lastseen", Data: []byte("42")})
	asrt.NoErr(err)
	asrt.Equal(resp.Uid, uint64(99))

	asrt.NoErr(leader.StopPersistedConsumer())
	asrt.NoErr(<-drainErr)
}

// TestNATSPersistedQueue_ReplyError surfaces apply-side errors to the
// publisher unchanged.
func TestNATSPersistedQueue_ReplyError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	asrt := is.New(t)
	streamName := fmt.Sprintf("quasar_test_queue_%d", time.Now().UnixNano())
	producer := makeNATSPersistedTransport(ctx, t, "test-cache", "producer", streamName)
	leader := makeNATSPersistedTransport(ctx, t, "test-cache", "leader", streamName)

	ch, err := leader.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	applyErr := errors.New("fsm rejected payload")
	go func() {
		for item := range ch {
			_ = item.ReplyError(ctx, applyErr)
		}
	}()

	_, err = producer.StorePersisted(ctx, &pb.Store{Key: "k"})
	asrt.True(err != nil)
	asrt.Equal(err.Error(), applyErr.Error())

	asrt.NoErr(leader.StopPersistedConsumer())
}

// TestNATSPersistedQueue_StreamManaged verifies that a non-managing
// transport (WithPersistedStreamManaged(false), used by nonvoters) never
// creates the stream: its publish fails until a managing transport
// (the voter) has created it, after which the same non-managing transport
// binds to the existing stream and publishes successfully.
func TestNATSPersistedQueue_StreamManaged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	asrt := is.New(t)
	streamName := fmt.Sprintf("quasar_test_queue_%d", time.Now().UnixNano())

	// Independent js handle to observe stream existence directly.
	nc, err := nats.Connect(natsURL, nats.Timeout(natsTimeout))
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	asrt.NoErr(err)
	if _, e := js.AccountInfo(ctx); e != nil {
		t.Skipf("JetStream not enabled on server: %v", e)
	}

	// Nonvoter: must not create the stream.
	nonvoter := makeNATSPersistedTransport(ctx, t, "test-cache", "nonvoter", streamName,
		WithPersistedStreamManaged(false))

	// Publishing before the stream exists fails, and crucially does not
	// create the stream nor permanently cache the failure.
	_, err = nonvoter.StorePersisted(ctx, &pb.Store{Key: "k"})
	asrt.True(err != nil)
	_, err = js.Stream(ctx, streamName)
	asrt.True(errors.Is(err, jetstream.ErrStreamNotFound))

	// Voter (manager) creates the stream and drains it.
	voter := makeNATSPersistedTransport(ctx, t, "test-cache", "voter", streamName)
	ch, err := voter.StartPersistedConsumer(ctx)
	asrt.NoErr(err)
	go func() {
		for item := range ch {
			_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 7})
		}
	}()

	// The same nonvoter now binds to the existing stream and succeeds.
	resp, err := nonvoter.StorePersisted(ctx, &pb.Store{Key: "k", Data: []byte("v")})
	asrt.NoErr(err)
	asrt.Equal(resp.Uid, uint64(7))

	asrt.NoErr(voter.StopPersistedConsumer())
}

// TestNATSPersistedQueue_RestartAfterConsumerDeleted is the RT-13042 M4
// regression test. When the JetStream consumer dies server-side (deleted, or
// an unrecoverable JS error), the puller goroutine exits and closes the items
// channel — but the queue used to keep its q.consumer reference, so every
// subsequent StartPersistedConsumer returned the dead consumer's CLOSED
// channel: the death was permanent until a leadership flip plus an explicit
// StopPersistedConsumer, stalling all persisted writes cluster-wide. With the
// fix the dead consumer clears itself, so a restart creates a fresh consumer
// that delivers again.
func TestNATSPersistedQueue_RestartAfterConsumerDeleted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	asrt := is.New(t)
	streamName := fmt.Sprintf("quasar_test_queue_%d", time.Now().UnixNano())
	producer := makeNATSPersistedTransport(ctx, t, "test-cache", "producer", streamName)
	leader := makeNATSPersistedTransport(ctx, t, "test-cache", "leader", streamName)

	ch, err := leader.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	// Kill the consumer server-side, as an operator or stream rebuild would.
	js, err := jetstream.New(leader.conn)
	asrt.NoErr(err)
	asrt.NoErr(js.DeleteConsumer(ctx, streamName, persistedConsumerNamePrefix))

	// The puller loop notices — at the latest via missed idle heartbeats —
	// and closes the items channel.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected item delivered after consumer deletion")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("items channel did not close after the JetStream consumer was deleted")
	}

	// Restarting must yield a LIVE consumer — not the dead one's closed
	// channel — and deliver a fresh publish end to end.
	ch2, err := leader.StartPersistedConsumer(ctx)
	asrt.NoErr(err)

	go func() {
		for item := range ch2 {
			_ = item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 11})
		}
	}()

	resp, err := producer.StorePersisted(ctx, &pb.Store{Key: "k", Data: []byte("v")})
	asrt.NoErr(err)
	asrt.Equal(resp.Uid, uint64(11))

	asrt.NoErr(leader.StopPersistedConsumer())
}

// TestNATSPersistedQueue_NotConfigured confirms transports built
// without WithNATSPersistedQueue report SupportsPersisted() == false
// and return ErrPersistedNotSupported from the persisted methods.
func TestNATSPersistedQueue_NotConfigured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	asrt := is.New(t)
	tr, err := makeNATSTransport(ctx, t, "test-cache", "no-queue")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer tr.conn.Close()

	asrt.Equal(tr.SupportsPersisted(), false)

	_, err = tr.StorePersisted(ctx, &pb.Store{Key: "k"})
	asrt.True(errors.Is(err, ErrPersistedNotSupported))

	_, err = tr.StartPersistedConsumer(ctx)
	asrt.True(errors.Is(err, ErrPersistedNotSupported))

	asrt.NoErr(tr.StopPersistedConsumer())
}
