package transports

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// Default tuning for the persisted-FIFO consumer. Sized to match the
// gatexcache plan: AckWait * MaxDeliver ≈ 40s total redelivery horizon
// (~2× quorumRecoveryAfter), with MaxAckPending = 1 giving strict
// in-order delivery at the cost of throughput. Operators can tune
// AckWait / MaxDeliver per call site via WithNATSPersistedQueue
// options; MaxAckPending is fixed at 1 by design (D4).
const (
	defaultPersistedAckWait    = 10 * time.Second
	defaultPersistedMaxDeliver = 4
	defaultPersistedMaxAge     = 1 * time.Hour

	persistedConsumerNamePrefix = "quasar-cache-consumer"

	// persistedPullHeartbeat is the idle-heartbeat interval requested on
	// the persisted consumer's pull subscription. Without it, a consumer
	// that dies server-side (deleted, stream rebuilt) is only noticed when
	// the long-lived pull request expires; with it the client errors out
	// of Next after ~2 missed heartbeats, so the death is detected within
	// seconds and the consumer can be restarted (RT-13042 M4).
	persistedPullHeartbeat = 5 * time.Second

	// persistedReplyHeader carries the publisher's reply-inbox subject
	// across the JetStream hop. We can't use the wire-level Reply field
	// because JetStream rewrites it to the ack-inbox subject when the
	// message is delivered to a consumer.
	persistedReplyHeader = "Quasar-Reply"
)

// natsPersistedQueue is the JetStream-backed persisted-FIFO mode for a
// NATSTransport. One instance per transport; the underlying JS stream
// is cluster-wide (every voter / nonvoter shares the same stream),
// each transport publishes into it and only the leader claims the
// pull consumer.
type natsPersistedQueue struct {
	conn          *nats.Conn
	js            jetstream.JetStream
	streamName    string
	subject       string
	subjectFilter string
	ackWait       time.Duration
	maxAge        time.Duration
	maxDeliver    int
	replicas      int
	manageStream  bool

	// stream is created/opened lazily on the first publish or consumer
	// start so callers don't have to coordinate which transport
	// instance "owns" the stream. streamM guards the lazy resolution and
	// caches only successful results, so a non-managing node that
	// publishes before the managing node has created the stream simply
	// fails that write and retries on the next publish.
	streamM sync.Mutex
	stream  jetstream.Stream

	// consumerM serializes Start/StopPersistedConsumer on this
	// transport.
	consumerM sync.Mutex
	consumer  *natsPersistedConsumer
}

// natsPersistedQueueConfig captures the construction-time parameters
// for the persisted-FIFO mode. Populated by WithNATSPersistedQueue and
// applied in NewNATSTransport via newNatsPersistedQueue.
type natsPersistedQueueConfig struct {
	streamName   string
	ackWait      time.Duration
	maxAge       time.Duration
	maxDeliver   int
	replicas     int
	manageStream bool
}

// newNatsPersistedQueue constructs the queue helper attached to a
// NATSTransport. Returns nil when cfg is the zero-value (i.e. when the
// transport was built without WithNATSPersistedQueue).
//
// JetStream stream names cannot contain `.`; subjects can. The default
// stream name encodes the cache name with underscores, while the
// publish subject uses the dotted form to keep wire subjects aligned
// with the rest of the quasar NATS conventions.
func newNatsPersistedQueue(conn *nats.Conn, cacheName string, cfg natsPersistedQueueConfig) (*natsPersistedQueue, error) {
	streamName := cfg.streamName
	if streamName == "" {
		streamName = fmt.Sprintf("quasar_%s_queue", sanitizeStreamName(cacheName))
	}
	subject := fmt.Sprintf("quasar.%s.queue", cacheName)

	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("init jetstream: %w", err)
	}

	return &natsPersistedQueue{
		conn:          conn,
		js:            js,
		streamName:    streamName,
		subject:       subject + ".msg",
		subjectFilter: subject + ".>",
		ackWait:       cfg.ackWait,
		maxDeliver:    cfg.maxDeliver,
		maxAge:        cfg.maxAge,
		replicas:      cfg.replicas,
		manageStream:  cfg.manageStream,
	}, nil
}

// ensureStream resolves the persisted-FIFO stream (WorkQueuePolicy) and
// caches it for subsequent publishers / consumers. When manageStream is
// set this node creates the stream (or updates it to the configured
// shape); otherwise it only binds to an already-existing stream and never
// creates or mutates it, leaving ownership of the stream config (replicas,
// retention) to the managing node. Only successful resolutions are cached,
// so a non-managing node that publishes before the managing node has
// created the stream fails that write and retries on the next call.
func (q *natsPersistedQueue) ensureStream(ctx context.Context) (jetstream.Stream, error) {
	q.streamM.Lock()
	defer q.streamM.Unlock()

	if q.stream != nil {
		return q.stream, nil
	}

	var (
		stream jetstream.Stream
		err    error
	)
	if q.manageStream {
		stream, err = q.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:      q.streamName,
			Subjects:  []string{q.subjectFilter},
			Retention: jetstream.WorkQueuePolicy,
			MaxAge:    q.maxAge,
			Replicas:  q.replicas,
			// Storage left at default (file).
		})
		if err != nil {
			return nil, fmt.Errorf("create persisted stream %q: %w", q.streamName, err)
		}
	} else {
		stream, err = q.js.Stream(ctx, q.streamName)
		if err != nil {
			return nil, fmt.Errorf("open persisted stream %q: %w", q.streamName, err)
		}
	}

	q.stream = stream
	return q.stream, nil
}

// publish marshals cmd onto the JS work-queue and blocks on a NATS
// request-reply inbox for the leader's reply.
func (q *natsPersistedQueue) publish(ctx context.Context, cmd *pb.Store) (*pb.StoreResponse, error) {
	if _, err := q.ensureStream(ctx); err != nil {
		return nil, err
	}

	bts, err := proto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal persisted store: %w", err)
	}

	inbox := nats.NewInbox()
	// Subscribe before publishing — once the leader processes the
	// item it'll publish on the inbox, and we want to be listening.
	sub, err := q.conn.SubscribeSync(inbox)
	if err != nil {
		return nil, fmt.Errorf("subscribe persisted reply inbox: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	msg := nats.NewMsg(q.subject)
	msg.Header.Set(persistedReplyHeader, inbox)
	msg.Data = bts
	if _, err = q.js.PublishMsg(ctx, msg); err != nil {
		return nil, fmt.Errorf("publish persisted store: %w", err)
	}

	replyMsg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return nil, err
	}

	var protoResp pb.CommandResponse
	if r := proto.Unmarshal(replyMsg.Data, &protoResp); r != nil {
		return nil, fmt.Errorf("decode persisted reply: %w", r)
	}
	if errStr := protoResp.GetError(); errStr != "" {
		return nil, errors.New(errStr)
	}
	return protoResp.GetStore(), nil
}

// startConsumer creates / opens the pull consumer and begins draining
// the queue. Returns a channel of PersistedItem; close signals consumer
// shutdown. Only one consumer is active per node at a time; concurrent
// callers reuse the active consumer.
func (q *natsPersistedQueue) startConsumer(ctx context.Context) (<-chan PersistedItem, error) {
	q.consumerM.Lock()
	defer q.consumerM.Unlock()

	if q.consumer != nil {
		return q.consumer.items, nil
	}

	stream, err := q.ensureStream(ctx)
	if err != nil {
		return nil, err
	}

	// Durable consumer name keyed to the stream so a leadership flip
	// resumes from the same pending state instead of starting from the
	// beginning. The leader-flip handover happens by cancelling the
	// previous consumer's puller (which Naks any in-flight item) and
	// starting a fresh pull on the same durable.
	consumerCfg := jetstream.ConsumerConfig{
		Durable:       persistedConsumerNamePrefix,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 1,
		AckWait:       q.ackWait,
		MaxDeliver:    q.maxDeliver,
	}
	jsConsumer, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("create persisted consumer: %w", err)
	}

	pullCtx, cancel := context.WithCancel(context.Background())
	mctx, err := jsConsumer.Messages(jetstream.PullHeartbeat(persistedPullHeartbeat))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("start persisted messages context: %w", err)
	}

	items := make(chan PersistedItem)
	c := &natsPersistedConsumer{
		queue:  q,
		items:  items,
		cancel: cancel,
		mctx:   mctx,
	}
	q.consumer = c

	go c.run(pullCtx)
	return items, nil
}

// stopConsumer cancels the active consumer goroutine and Naks the
// in-flight item (if any) so the next leader picks it up immediately.
func (q *natsPersistedQueue) stopConsumer() error {
	q.consumerM.Lock()
	defer q.consumerM.Unlock()
	if q.consumer == nil {
		return nil
	}
	c := q.consumer
	q.consumer = nil
	c.stop()
	return nil
}

// settleWait is how long a stopping consumer waits for the in-flight item
// to be settled by the apply loop before falling back to a Nack.
func (q *natsPersistedQueue) settleWait() time.Duration {
	if q.ackWait > 0 {
		return q.ackWait
	}
	return defaultPersistedAckWait
}

// clearConsumer drops the active-consumer reference if it still points at c.
// Called from the puller goroutine's exit path: when the consumer dies
// spontaneously (e.g. the JetStream consumer was deleted server-side or hit
// an unrecoverable JS error) the queue must not keep handing the dead
// consumer's closed items channel out of startConsumer — that would make
// the death permanent until a leadership flip AND an explicit
// StopPersistedConsumer (RT-13042 M4). On the regular stop path
// stopConsumer has already cleared the reference, so this is a no-op.
func (q *natsPersistedQueue) clearConsumer(c *natsPersistedConsumer) {
	q.consumerM.Lock()
	if q.consumer == c {
		q.consumer = nil
	}
	q.consumerM.Unlock()
}

// natsPersistedConsumer wraps the JS Messages pull context and the
// outgoing PersistedItem channel.
type natsPersistedConsumer struct {
	queue  *natsPersistedQueue
	items  chan PersistedItem
	cancel context.CancelFunc
	mctx   jetstream.MessagesContext

	inflightM sync.Mutex
	inflight  *natsPersistedItem
}

// run is the puller loop: pull one message, hand it to the consumer
// channel, repeat. With MaxAckPending = 1, JetStream itself enforces
// the strict-FIFO single-in-flight invariant; we just need to keep up
// the conventional ack/nak protocol.
func (c *natsPersistedConsumer) run(ctx context.Context) {
	defer close(c.items)
	defer c.mctx.Stop()
	defer c.queue.clearConsumer(c)
	for {
		msg, err := c.mctx.Next()
		if err != nil {
			// Either ctx canceled (Stop drained the context) or the
			// consumer is being torn down. Either way, exit.
			return
		}

		if ok := c.consume(ctx, msg); !ok {
			return
		}
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

func (c *natsPersistedConsumer) stop() {
	c.cancel()
	c.mctx.Stop()
	c.inflightM.Lock()
	in := c.inflight
	c.inflightM.Unlock()
	if in == nil {
		return
	}
	// Same reasoning as the ctx.Done branch in consume: the in-flight item
	// may be mid-apply on the loop side, so give it a bounded chance to
	// settle before nacking it for redelivery (RT-13042 M5).
	select {
	case <-in.settled:
	case <-time.After(c.queue.settleWait()):
		_ = in.Nack(context.Background())
	}
}

func (c *natsPersistedConsumer) replyDecodeError(item *natsPersistedItem, decodeErr error) error {
	protoResp := &pb.CommandResponse{Error: decodeErr.Error()}
	bts, _ := proto.Marshal(protoResp)
	if reply := persistedReplyInbox(item.msg); reply != "" {
		_ = c.queue.conn.Publish(reply, bts)
	}
	return item.msg.Ack()
}

// persistedReplyInbox extracts the publisher's reply-inbox subject from
// the message header, falling back to the wire-level Reply field for
// forward compatibility (in case the producer changes how it stamps
// the reply target).
func persistedReplyInbox(msg jetstream.Msg) string {
	if hdr := msg.Headers(); hdr != nil {
		if v := hdr.Get(persistedReplyHeader); v != "" {
			return v
		}
	}
	return msg.Reply()
}

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

// sanitizeStreamName replaces JetStream-illegal characters (dots,
// spaces, asterisks, greater-than) with underscores so a dotted cache
// name still produces a valid default stream name.
func sanitizeStreamName(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '.', ' ', '*', '>':
			out = append(out, '_')
		default:
			out = append(out, c)
		}
	}
	return string(out)
}
