// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"errors"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/matryer/is"
	"github.com/nats-io/nats.go/jetstream"
)

// stubMessagesContext satisfies jetstream.MessagesContext; connect only
// stores it, so no methods are exercised.
type stubMessagesContext struct{ jetstream.MessagesContext }

// stubConsumer is a jetstream.Consumer whose Info can be forced to fail,
// mimicking a durable that survived a file-store corruption half-dead.
type stubConsumer struct {
	jetstream.Consumer
	infoErr error
}

func (s stubConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	if s.infoErr != nil {
		return nil, s.infoErr
	}
	return &jetstream.ConsumerInfo{}, nil
}

func (s stubConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return stubMessagesContext{}, nil
}

// rebuildStream records consumer creates and deletes; when brokenFirst is
// set, the first created consumer reports an unreadable info.
type rebuildStream struct {
	jetstream.Stream
	brokenFirst bool
	creates     int
	deleted     []string
}

func (s *rebuildStream) CreateOrUpdateConsumer(_ context.Context, _ jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	s.creates++
	if s.brokenFirst && s.creates == 1 {
		return stubConsumer{infoErr: errors.New("error opening msg block file")}, nil
	}
	return stubConsumer{}, nil
}

func (s *rebuildStream) DeleteConsumer(_ context.Context, consumer string) error {
	s.deleted = append(s.deleted, consumer)
	return nil
}

func newRebuildTestQueue() *natsPersistedQueue {
	return &natsPersistedQueue{
		streamName: "test_queue",
		shards:     1,
		logger:     hclog.NewNullLogger(),
	}
}

// connect must detect a durable whose info is unreadable, delete it and
// recreate it instead of subscribing to a consumer that will never deliver.
func TestConnectRebuildsUnreadableConsumer(t *testing.T) {
	asrt := is.New(t)

	q := newRebuildTestQueue()
	c := &natsPersistedConsumer{queue: q}
	stream := &rebuildStream{brokenFirst: true}

	asrt.NoErr(c.connect(context.Background(), stream))
	asrt.Equal(stream.creates, 2)
	asrt.Equal(stream.deleted, []string{q.shardDurable(0)})
}

// connect must leave a healthy durable untouched.
func TestConnectKeepsReadableConsumer(t *testing.T) {
	asrt := is.New(t)

	q := newRebuildTestQueue()
	c := &natsPersistedConsumer{queue: q}
	stream := &rebuildStream{}

	asrt.NoErr(c.connect(context.Background(), stream))
	asrt.Equal(stream.creates, 1)
	asrt.Equal(len(stream.deleted), 0)
}
