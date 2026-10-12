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

// RT-14885: only a failed reply wait after the PubAck wraps
// ErrPersistedNoReply. The cache turns exactly that into ErrRetrying, so a
// failure before the PubAck must come back without it.

// TestNATSPersistedQueue_RetryNoPubAckIsNotNoReply: a retry publish that gets
// no PubAck within the publish timeout is not known to be queued.
func TestNATSPersistedQueue_RetryNoPubAckIsNotNoReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	asrt := is.New(t)
	suffix := time.Now().UnixNano()
	cacheName := fmt.Sprintf("test-nopuback-%d", suffix)
	streamName := fmt.Sprintf("quasar_test_queue_%d", suffix)

	tr := makeNATSPersistedTransport(ctx, t, cacheName, "producer", streamName, WithPersistedStreamManaged(false))

	// The stream exists but does not capture the queue subject, and a core
	// subscriber that never answers takes the publish: no PubAck, no
	// no-responders error, the publish runs into its timeout.
	js, err := jetstream.New(tr.conn)
	asrt.NoErr(err)
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{fmt.Sprintf("quasar.%s.other", cacheName)},
	})
	asrt.NoErr(err)
	sub, err := tr.conn.Subscribe(fmt.Sprintf("quasar.%s.queue.msg", cacheName), func(*nats.Msg) {})
	asrt.NoErr(err)
	defer func() { _ = sub.Unsubscribe() }()

	prev := persistedPublishTimeout
	persistedPublishTimeout = 300 * time.Millisecond
	t.Cleanup(func() { persistedPublishTimeout = prev })

	_, err = tr.StorePersisted(ctx, &pb.Store{Key: "k"}, PersistedStoreOpts{Retry: true})
	asrt.True(err != nil)
	asrt.True(errors.Is(err, context.DeadlineExceeded)) // the publish timed out
	asrt.True(!errors.Is(err, ErrPersistedNoReply))
}

// TestNATSPersistedQueue_RetryMissingStreamIsNotNoReply: a retry publish of a
// non-managing node whose stream does not exist yet queued nothing.
func TestNATSPersistedQueue_RetryMissingStreamIsNotNoReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	asrt := is.New(t)
	suffix := time.Now().UnixNano()
	tr := makeNATSPersistedTransport(ctx, t, fmt.Sprintf("test-nostream-%d", suffix), "producer",
		fmt.Sprintf("quasar_test_queue_%d", suffix), WithPersistedStreamManaged(false))

	_, err := tr.StorePersisted(ctx, &pb.Store{Key: "k"}, PersistedStoreOpts{Retry: true})
	asrt.True(errors.Is(err, jetstream.ErrStreamNotFound))
	asrt.True(!errors.Is(err, ErrPersistedNoReply))
}

// TestNATSPersistedQueue_RetryPublishLandsDespiteCancelledCtx: the first retry
// publish of a process resolves the stream under the detached publish
// context, so a caller ctx the leader gate already cancelled neither fails
// the stream lookup nor keeps the command out of the stream.
func TestNATSPersistedQueue_RetryPublishLandsDespiteCancelledCtx(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	asrt := is.New(t)
	suffix := time.Now().UnixNano()
	streamName := fmt.Sprintf("quasar_test_queue_%d", suffix)
	tr := makeNATSPersistedTransport(ctx, t, fmt.Sprintf("test-gap-%d", suffix), "producer", streamName)

	callCtx, callCancel := context.WithCancel(ctx)
	callCancel()
	_, err := tr.StorePersisted(callCtx, &pb.Store{Key: "k"}, PersistedStoreOpts{Retry: true})
	asrt.True(errors.Is(err, ErrPersistedNoReply))
	asrt.True(errors.Is(err, context.Canceled))

	js, err := jetstream.New(tr.conn)
	asrt.NoErr(err)
	stream, err := js.Stream(ctx, streamName)
	asrt.NoErr(err)
	info, err := stream.Info(ctx)
	asrt.NoErr(err)
	asrt.Equal(info.State.Msgs, uint64(1))
}
