package transports

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// natsPersistedItem implements PersistedItem on top of a JetStream Msg.
type natsPersistedItem struct {
	queue   *natsPersistedQueue
	msg     jetstream.Msg
	command *pb.Store

	settledM    sync.Mutex
	settledDone bool
	settled     chan struct{}
}

func (i *natsPersistedItem) Command() *pb.Store {
	return i.command
}

func (i *natsPersistedItem) Retry() bool {
	return persistedRetryFromMsg(i.msg)
}

func (i *natsPersistedItem) Deadline() (time.Time, bool) {
	return persistedDeadlineFromMsg(i.msg)
}

// beginSettle claims the right to settle this item. It returns false when
// the item was already settled — the caller lost the race and must not
// touch the underlying message again. A lost race is reported to the loser
// as ErrAlreadySettled (RT-13042 M5): silently no-oping made a Nack that
// voided a successful apply undetectable on the apply side.
func (i *natsPersistedItem) beginSettle() bool {
	i.settledM.Lock()
	defer i.settledM.Unlock()

	if i.settledDone {
		return false
	}
	i.settledDone = true
	return true
}

func (i *natsPersistedItem) ReplySuccess(_ context.Context, resp *pb.StoreResponse) error {
	return i.terminate(&pb.CommandResponse{Resp: &pb.CommandResponse_Store{Store: resp}}, true)
}

func (i *natsPersistedItem) ReplyError(_ context.Context, err error) error {
	return i.terminate(&pb.CommandResponse{Error: err.Error()}, true)
}

func (i *natsPersistedItem) Nack(_ context.Context) error {
	if !i.beginSettle() {
		return ErrAlreadySettled
	}
	err := i.msg.Nak()
	close(i.settled)
	return err
}

func (i *natsPersistedItem) terminate(protoResp *pb.CommandResponse, ack bool) error {
	if !i.beginSettle() {
		return ErrAlreadySettled
	}
	defer close(i.settled)

	bts, mErr := proto.Marshal(protoResp)
	if mErr != nil {
		// Even on marshal failure we need to ack to avoid
		// poison-message redelivery storms; the publisher will
		// time out and surface its own error.
		_ = i.msg.Ack()
		return mErr
	}

	var err error
	if reply := persistedReplyInbox(i.msg); reply != "" {
		if pErr := i.queue.conn.Publish(reply, bts); pErr != nil {
			err = pErr
		}
	}
	if ack {
		if aErr := i.msg.Ack(); aErr != nil && err == nil {
			err = aErr
		}
	}
	return err
}
