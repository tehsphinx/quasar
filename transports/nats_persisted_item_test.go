// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tehsphinx/quasar/pb/v1"
)

// fakeJSMsg is a minimal jetstream.Msg that records ack/nak calls.
type fakeJSMsg struct {
	acked        atomic.Int32
	naked        atomic.Int32
	numDelivered uint64
	headers      nats.Header
}

func (m *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.numDelivered}, nil
}
func (m *fakeJSMsg) Data() []byte                     { return nil }
func (m *fakeJSMsg) Headers() nats.Header             { return m.headers }
func (m *fakeJSMsg) Subject() string                  { return "test.subject" }
func (m *fakeJSMsg) Reply() string                    { return "" }
func (m *fakeJSMsg) Ack() error                       { m.acked.Add(1); return nil }
func (m *fakeJSMsg) DoubleAck(context.Context) error  { m.acked.Add(1); return nil }
func (m *fakeJSMsg) Nak() error                       { m.naked.Add(1); return nil }
func (m *fakeJSMsg) NakWithDelay(time.Duration) error { m.naked.Add(1); return nil }
func (m *fakeJSMsg) InProgress() error                { return nil }
func (m *fakeJSMsg) Term() error                      { return nil }
func (m *fakeJSMsg) TermWithReason(string) error      { return nil }

func newTestPersistedItem(msg jetstream.Msg) *natsPersistedItem {
	return &natsPersistedItem{
		queue:   &natsPersistedQueue{ackWait: time.Second},
		msg:     msg,
		settled: make(chan struct{}),
	}
}

// TestNatsPersistedItem_LostSettleRaceIsReported is part of the RT-13042 M5
// regression. A second settle attempt used to be a silent no-op returning
// nil — so a Nack that lost against a completed ReplySuccess (or vice versa)
// was undetectable. The loser must get ErrAlreadySettled and must not touch
// the underlying message.
func TestNatsPersistedItem_LostSettleRaceIsReported(t *testing.T) {
	ctx := context.Background()

	item := newTestPersistedItem(&fakeJSMsg{})
	msg := item.msg.(*fakeJSMsg)

	if err := item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 1}); err != nil {
		t.Fatalf("ReplySuccess: %v", err)
	}
	if err := item.Nack(ctx); !errors.Is(err, ErrAlreadySettled) {
		t.Fatalf("Nack after ReplySuccess returned %v, want ErrAlreadySettled", err)
	}

	if got := msg.acked.Load(); got != 1 {
		t.Fatalf("message acked %d times, want 1", got)
	}
	if got := msg.naked.Load(); got != 0 {
		t.Fatalf("message naked %d times after successful reply, want 0", got)
	}
}

// TestNatsPersistedConsumer_StopPrefersSettleOverNack is the RT-13042 M5
// regression test. stop() used to Nack the in-flight item unconditionally —
// even when the apply loop had already committed the command to raft and was
// about to ReplySuccess. The winning Nack made ReplySuccess a silent no-op:
// the publisher's reply was never sent and the already-applied command was
// redelivered to the next leader for a second apply. stop() must give the
// in-flight apply a bounded chance to settle; only an item nobody settles
// within the AckWait window gets nacked.
func TestNatsPersistedConsumer_StopPrefersSettleOverNack(t *testing.T) {
	ctx := context.Background()

	item := newTestPersistedItem(&fakeJSMsg{})
	msg := item.msg.(*fakeJSMsg)

	pullCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &natsPersistedConsumer{
		queue: item.queue,
		items: make(chan PersistedItem),
		mctx:  noopMessagesContext{ctx: pullCtx},
	}
	c.setInflight(item)

	// Leadership flip: stop() runs while the apply loop is mid-apply.
	stopped := make(chan struct{})
	go func() {
		c.drainInflight()
		close(stopped)
	}()

	// The in-flight apply completes shortly after.
	time.Sleep(100 * time.Millisecond)
	if err := item.ReplySuccess(ctx, &pb.StoreResponse{Uid: 7}); err != nil {
		t.Fatalf("ReplySuccess during stop: %v", err)
	}

	select {
	case <-stopped:
	case <-time.After(item.queue.settleWait() / 2):
		t.Fatal("stop() did not return promptly after the in-flight item settled")
	}

	if got := msg.naked.Load(); got != 0 {
		t.Fatalf("in-flight item was nacked %d times despite a successful apply (RT-13042 M5)", got)
	}
	if got := msg.acked.Load(); got != 1 {
		t.Fatalf("message acked %d times, want 1", got)
	}
}

// TestNatsPersistedConsumer_StopNacksUnsettledItem confirms the fallback: an
// in-flight item that nobody settles within the AckWait window is still
// nacked so the next leader picks it up.
func TestNatsPersistedConsumer_StopNacksUnsettledItem(t *testing.T) {
	item := newTestPersistedItem(&fakeJSMsg{})
	item.queue.ackWait = 100 * time.Millisecond
	msg := item.msg.(*fakeJSMsg)

	pullCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &natsPersistedConsumer{
		queue: item.queue,
		items: make(chan PersistedItem),
		mctx:  noopMessagesContext{ctx: pullCtx},
	}
	c.setInflight(item)

	c.drainInflight()

	if got := msg.naked.Load(); got != 1 {
		t.Fatalf("unsettled in-flight item naked %d times, want 1", got)
	}
}

// TestNatsPersistedItem_RetryAndDeadlineHeaders verifies the RT-12964 wire
// accessors: Retry() and Deadline() read the per-message headers stamped by
// publish, and default to non-retry / no-deadline when the headers are absent
// or malformed.
func TestNatsPersistedItem_RetryAndDeadlineHeaders(t *testing.T) {
	deadline := time.Unix(0, 1_700_000_000_000_000_000)

	t.Run("headers present", func(t *testing.T) {
		hdr := nats.Header{}
		hdr.Set(persistedRetryHeader, "1")
		hdr.Set(persistedDeadlineHeader, strconv.FormatInt(deadline.UnixNano(), 10))
		item := newTestPersistedItem(&fakeJSMsg{headers: hdr})

		if !item.Retry() {
			t.Fatal("Retry() = false, want true")
		}
		got, ok := item.Deadline()
		if !ok {
			t.Fatal("Deadline() ok = false, want true")
		}
		if !got.Equal(deadline) {
			t.Fatalf("Deadline() = %v, want %v", got, deadline)
		}
	})

	t.Run("headers absent", func(t *testing.T) {
		item := newTestPersistedItem(&fakeJSMsg{})
		if item.Retry() {
			t.Fatal("Retry() = true with no header, want false")
		}
		if _, ok := item.Deadline(); ok {
			t.Fatal("Deadline() ok = true with no header, want false")
		}
	})

	t.Run("malformed deadline", func(t *testing.T) {
		hdr := nats.Header{}
		hdr.Set(persistedDeadlineHeader, "not-a-number")
		item := newTestPersistedItem(&fakeJSMsg{headers: hdr})
		if _, ok := item.Deadline(); ok {
			t.Fatal("Deadline() ok = true for malformed header, want false")
		}
	})
}

// noopMessagesContext satisfies jetstream.MessagesContext for consumer unit
// tests that never pull.
type noopMessagesContext struct {
	ctx context.Context
}

func (m noopMessagesContext) Next() (jetstream.Msg, error) {
	<-m.ctx.Done()
	return nil, jetstream.ErrMsgIteratorClosed
}
func (m noopMessagesContext) Stop()  {}
func (m noopMessagesContext) Drain() {}
