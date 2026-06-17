package transports

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// natsPersistedConsumer wraps one shard's JS Messages pull context and the
// shared outgoing PersistedItem channel.
type natsPersistedConsumer struct {
	queue *natsPersistedQueue
	items chan PersistedItem
	shard int
	group *natsPersistedConsumerGroup

	// mctx is replaced on every reconnect, so it is guarded: the puller
	// goroutine swaps it in reconnect while stop() reads it to unblock a
	// blocked Next().
	mctxM sync.Mutex
	mctx  jetstream.MessagesContext

	inflightM sync.Mutex
	inflight  *natsPersistedItem
}

// connect creates (or opens) this shard's durable JetStream consumer and opens a
// heartbeated pull subscription, attaching the resulting Messages context to
// the consumer. It does not launch the puller goroutine — the group launches
// every puller together once all shards have opened (see startConsumer).
func (c *natsPersistedConsumer) connect(ctx context.Context, stream jetstream.Stream) error {
	// Durable consumer name keyed to the stream (and shard) so a leadership
	// flip resumes from the same pending state instead of starting from the
	// beginning. The leader-flip handover cancels the previous group's pullers
	// (which Nak any in-flight items) and starts fresh pulls on the same
	// durables.
	consumerCfg := jetstream.ConsumerConfig{
		Durable:       c.queue.shardDurable(c.shard),
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 1,
		AckWait:       c.queue.ackWait,
		MaxDeliver:    c.queue.maxDeliver,
	}
	// WorkQueue retention requires non-overlapping consumer filters; the
	// per-shard subjects (…s0.>, …s1.>, …) are disjoint. With a single shard we
	// keep the original unfiltered subscription.
	if c.queue.shards > 1 {
		consumerCfg.FilterSubject = c.queue.shardFilter(c.shard)
	}
	jsConsumer, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return fmt.Errorf("create persisted consumer shard %d: %w", c.shard, err)
	}
	mctx, err := jsConsumer.Messages(jetstream.PullHeartbeat(persistedPullHeartbeat))
	if err != nil {
		return fmt.Errorf("start persisted messages context shard %d: %w", c.shard, err)
	}
	c.setMctx(mctx)
	return nil
}

// setMctx installs this shard's current messages context.
func (c *natsPersistedConsumer) setMctx(mctx jetstream.MessagesContext) {
	c.mctxM.Lock()
	c.mctx = mctx
	c.mctxM.Unlock()
}

// nextMsg pulls the next message from the shard's current messages context.
func (c *natsPersistedConsumer) nextMsg() (jetstream.Msg, error) {
	c.mctxM.Lock()
	mctx := c.mctx
	c.mctxM.Unlock()
	return mctx.Next()
}

// stopMctx stops the shard's current messages context, draining its internal
// goroutines and unsubscribing its ephemeral pull inbox. It only tears down
// the client-side subscription — the durable JetStream consumer is persistent
// and is left untouched so a reconnect resumes from its pending state. Safe to
// call repeatedly and from both the puller and stop().
func (c *natsPersistedConsumer) stopMctx() {
	c.mctxM.Lock()
	mctx := c.mctx
	c.mctxM.Unlock()
	if mctx != nil {
		mctx.Stop()
	}
}

// run is one shard's supervisor loop. It pulls until the subscription ends,
// then decides whether the group is stopping (exit) or this shard died on its
// own (rebuild just this shard and resume). A spontaneous death no longer
// tears the whole group down — the sibling shards keep draining and the shared
// items channel stays open; only this goroutine's shard pauses while it
// reconnects (RT-12964).
func (c *natsPersistedConsumer) run(ctx context.Context) {
	defer c.group.wg.Done()

	for {
		c.pull(ctx)

		// pull returned: the shard's subscription ended. Tear this shard's
		// client-side resources down cleanly either way — stopMctx drains the
		// messages context's goroutines and unsubscribes its pull inbox so
		// nothing leaks. The durable JetStream consumer is persistent and is
		// left in place; the reconnect below resumes from its pending state.
		c.stopMctx()

		if ctx.Err() != nil {
			// Group is stopping (leadership loss / shutdown). stop() drives the
			// in-flight Nak for prompt handover; just exit.
			return
		}

		// Spontaneous death of this shard alone. Nak any in-flight item so its
		// redelivery isn't stalled for AckWait, then rebuild this shard and
		// resume. reconnect returns false only when the group stops mid-retry.
		c.drainInflight()
		if !c.reconnect(ctx) {
			return
		}
	}
}

// pull is one shard's inner puller loop: pull one message, hand it to the
// shared consumer channel, repeat. With MaxAckPending = 1 JetStream enforces
// the strict-FIFO single-in-flight invariant per shard; we just keep up the
// conventional ack/nak protocol. It returns when Next errors (subscription
// stopped or died) or when consume observes ctx cancellation.
func (c *natsPersistedConsumer) pull(ctx context.Context) {
	for {
		msg, err := c.nextMsg()
		if err != nil {
			// Either ctx canceled (Stop drained the context) or the consumer
			// died server-side. run decides whether to exit or reconnect.
			return
		}

		if ok := c.consume(ctx, msg); !ok {
			return
		}
	}
}

// reconnect rebuilds this shard's pull subscription after a spontaneous death,
// retrying with backoff until it succeeds or the group stops. It reuses the
// existing durable consumer (CreateOrUpdateConsumer on the same name), so the
// shard resumes from its persisted pending state rather than starting over.
// Returns false only when ctx is canceled (group stopping).
func (c *natsPersistedConsumer) reconnect(ctx context.Context) bool {
	backoff := persistedReconnectBackoff
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return false
		}

		stream, err := c.queue.ensureStream(ctx)
		if err == nil {
			err = c.connect(ctx, stream)
		}
		if err == nil {
			// If the group stopped while we were connecting, the freshly opened
			// messages context would never be stopped by stop() (it stopped the
			// previous one before this existed). stop() cancels ctx before it
			// calls stopMctx, so a non-nil Err here means we own the cleanup.
			if ctx.Err() != nil {
				c.stopMctx()
				return false
			}
			c.queue.logger.Warn("persisted consumer shard reconnected",
				"shard", c.shard, "attempts", attempt)
			return true
		}

		c.queue.logger.Error("persisted consumer shard reconnect failed; retrying",
			"shard", c.shard, "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff < persistedReconnectMaxBackoff {
			if backoff *= 2; backoff > persistedReconnectMaxBackoff {
				backoff = persistedReconnectMaxBackoff
			}
		}
	}
}

// drainInflight gives this shard's in-flight item a bounded chance to settle,
// then Naks it for immediate redelivery to the next leader (RT-13042 M5).
func (c *natsPersistedConsumer) drainInflight() {
	c.inflightM.Lock()
	in := c.inflight
	c.inflightM.Unlock()
	if in == nil {
		return
	}
	select {
	case <-in.settled:
	case <-time.After(c.queue.settleWait()):
		_ = in.Nack(context.Background())
	}
}

// consume is the puller loop's message handler. It returns true if the
// consumer should continue, false if it should exit.
func (c *natsPersistedConsumer) consume(ctx context.Context, msg jetstream.Msg) bool {
	item := &natsPersistedItem{
		queue:   c.queue,
		msg:     msg,
		settled: make(chan struct{}),
	}
	c.setInflight(item)
	defer c.clearInflight(item)

	// Terminal-delivery detection (RT-13042 M19): once NumDelivered reaches
	// MaxDeliver, JetStream will not redeliver this message again. With
	// WorkQueue retention and no DLQ that means a Nack here (e.g. another
	// leader flip) drops the "persisted" write for good. We still process
	// the delivery — most terminal deliveries do apply successfully — but we
	// log loudly so an operator sees a write that is one Nack away from
	// being lost rather than losing it silently.
	if c.queue.maxDeliver > 0 {
		if meta, mErr := msg.Metadata(); mErr == nil && meta.NumDelivered >= uint64(c.queue.maxDeliver) {
			c.queue.logger.Error("persisted write reached terminal delivery; a further Nack will silently drop it",
				"subject", msg.Subject(),
				"num_delivered", meta.NumDelivered,
				"max_deliver", c.queue.maxDeliver,
			)
		}
	}

	// Decode the command. If decoding fails we cannot deliver
	// it to the consumer loop meaningfully — reject with
	// `Term` (ack as a terminal failure so JetStream doesn't
	// redeliver poison messages indefinitely) and continue.
	var protoMsg pb.Store
	if r := proto.Unmarshal(msg.Data(), &protoMsg); r != nil {
		_ = c.replyDecodeError(item, r)
		return true
	}
	item.command = &protoMsg

	select {
	case c.items <- item:
	case <-ctx.Done():
		// Consumer is going away. Nak the in-flight item so the
		// next claimant gets it without waiting for AckWait.
		_ = item.Nack(context.Background())
		return false
	}
	// Wait for the consumer to settle the item before fetching
	// the next one. With MaxAckPending = 1 the next Next() call
	// would block on the server side anyway, but waiting locally
	// also gives us a deterministic place to observe ctx
	// cancellation against an unsettled inflight item.
	select {
	case <-item.settled:
	case <-ctx.Done():
		// The item is with the apply loop and may already be committed to
		// raft. An immediate Nack here could void that successful apply:
		// ReplySuccess loses the settle race, the publisher's reply never
		// goes out, and the next leader applies the same command a second
		// time. Prefer letting the in-flight apply settle, bounded by
		// AckWait — past that JetStream redelivers anyway (RT-13042 M5).
		select {
		case <-item.settled:
		case <-time.After(c.queue.settleWait()):
			_ = item.Nack(context.Background())
		}
		return false
	}
	return true
}

func (c *natsPersistedConsumer) setInflight(item *natsPersistedItem) {
	c.inflightM.Lock()
	c.inflight = item
	c.inflightM.Unlock()
}

func (c *natsPersistedConsumer) clearInflight(item *natsPersistedItem) {
	c.inflightM.Lock()
	if c.inflight == item {
		c.inflight = nil
	}
	c.inflightM.Unlock()
}

func (c *natsPersistedConsumer) replyDecodeError(item *natsPersistedItem, decodeErr error) error {
	protoResp := &pb.CommandResponse{Error: decodeErr.Error()}
	bts, _ := proto.Marshal(protoResp)
	if reply := persistedReplyInbox(item.msg); reply != "" {
		_ = c.queue.conn.Publish(reply, bts)
	}
	return item.msg.Ack()
}
