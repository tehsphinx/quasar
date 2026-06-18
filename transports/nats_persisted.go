package transports

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// Default tuning for the persisted-FIFO consumer. Sized to match the
// gatexcache plan: AckWait * MaxDeliver gives the total redelivery
// horizon, with MaxAckPending = 1 giving strict in-order delivery at
// the cost of throughput. Operators can tune AckWait / MaxDeliver per
// call site via WithNATSPersistedQueue options; MaxAckPending is fixed
// at 1 by design (D4).
//
// MaxDeliver is bumped from the original 4 to 16: with a single shared
// durable, a leadership flip Naks the in-flight item to hand it over,
// and each Nack burns one delivery attempt. A short flap (or a brief
// Nack ping-pong between the outgoing and incoming leader's sessions)
// could exhaust 4 attempts and — because the stream uses WorkQueue
// retention with no DLQ — SILENTLY drop a "persisted" write once
// JetStream stops redelivering. 16 gives a far wider margin without
// redesigning the durable; a terminal delivery is additionally logged
// loudly at Error level (see consume) so an operator notices a write
// that genuinely cannot be applied rather than losing it in silence
// (RT-13042 M19).
const (
	defaultPersistedAckWait    = 10 * time.Second
	defaultPersistedMaxDeliver = 16
	defaultPersistedMaxAge     = 1 * time.Hour

	persistedConsumerNamePrefix = "quasar-cache-consumer"

	// persistedPullHeartbeat is the idle-heartbeat interval requested on
	// the persisted consumer's pull subscription. Without it, a consumer
	// that dies server-side (deleted, stream rebuilt) is only noticed when
	// the long-lived pull request expires; with it the client errors out
	// of Next after ~2 missed heartbeats, so the death is detected within
	// seconds and the consumer can be restarted (RT-13042 M4).
	persistedPullHeartbeat = 5 * time.Second

	// persistedReconnectBackoff / persistedReconnectMaxBackoff bound the
	// retry loop a shard runs after its pull subscription dies spontaneously
	// (JS consumer deleted/rebuilt server-side, missed heartbeats, transient
	// JS error). A dead shard rebuilds only itself, leaving the sibling shards
	// draining, so the backoff just keeps a shard that can't reconnect yet
	// (e.g. JS briefly unavailable) from hot-looping until it recovers.
	persistedReconnectBackoff    = 250 * time.Millisecond
	persistedReconnectMaxBackoff = 5 * time.Second

	// persistedReplyHeader carries the publisher's reply-inbox subject
	// across the JetStream hop. We can't use the wire-level Reply field
	// because JetStream rewrites it to the ack-inbox subject when the
	// message is delivered to a consumer.
	persistedReplyHeader = "Quasar-Reply"

	// persistedRetryHeader carries the publisher's WithRetry choice across
	// the JetStream hop so the consuming leader (possibly a different node)
	// can decide whether to redeliver or terminate a command it cannot apply
	// yet. Present and "1" means retry; absent / anything else means non-retry
	// (RT-12964).
	persistedRetryHeader = "Quasar-Retry"

	// persistedDeadlineHeader carries the publisher's call deadline (Unix
	// nanoseconds) so the consumer can drop a non-retry command picked up
	// after the publisher has already given up, instead of silently applying
	// it late (RT-12964). Absent when the publisher had no deadline.
	persistedDeadlineHeader = "Quasar-Deadline"
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
	subjectBase   string
	subject       string
	subjectFilter string
	ackWait       time.Duration
	maxAge        time.Duration
	maxDeliver    int
	replicas      int
	shards        int
	manageStream  bool
	logger        hclog.Logger

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
	group     *natsPersistedConsumerGroup
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
	shards       int
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

	shards := cfg.shards
	if shards < 1 {
		shards = 1
	}

	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("init jetstream: %w", err)
	}

	// The persisted queue is constructed without a NATSTransport logger
	// handle (newNatsPersistedQueue is called before the transport is
	// fully wired), so default to the standard hclog logger here. The
	// only thing it is used for is the terminal-delivery Error below.
	return &natsPersistedQueue{
		conn:          conn,
		js:            js,
		streamName:    streamName,
		subjectBase:   subject,
		subject:       subject + ".msg",
		subjectFilter: subject + ".>",
		ackWait:       cfg.ackWait,
		maxDeliver:    cfg.maxDeliver,
		maxAge:        cfg.maxAge,
		replicas:      cfg.replicas,
		shards:        shards,
		manageStream:  cfg.manageStream,
		logger:        hclog.New(&hclog.LoggerOptions{Name: "quasar-persisted"}),
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
// request-reply inbox for the leader's reply. opts.ShardKey selects the FIFO
// partition (RT-12964).
func (q *natsPersistedQueue) publish(ctx context.Context, cmd *pb.Store, opts PersistedStoreOpts) (*pb.StoreResponse, error) {
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

	msg := nats.NewMsg(q.shardSubject(opts.ShardKey))
	msg.Header.Set(persistedReplyHeader, inbox)
	if opts.Retry {
		msg.Header.Set(persistedRetryHeader, "1")
	}
	if dl, ok := ctx.Deadline(); ok {
		msg.Header.Set(persistedDeadlineHeader, strconv.FormatInt(dl.UnixNano(), 10))
	}
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

// startConsumer creates / opens the per-shard pull consumers and begins
// draining the queue. Returns a single channel of PersistedItem that fans in
// every shard; close signals consumer shutdown. Only one consumer group is
// active per node at a time; concurrent callers reuse the active group.
//
// Sharding (RT-12964): each of the q.shards partitions gets its own durable
// consumer with MaxAckPending = 1, so writes stay strictly in order WITHIN a
// shard while independent shards drain in parallel — a write that is stalling
// or being redelivered only blocks its own shard, not every subsequent write
// cluster-wide. With q.shards == 1 this is byte-for-byte the original
// single-consumer behaviour (durable name and unfiltered subscription
// unchanged), so existing deployments are unaffected.
func (q *natsPersistedQueue) startConsumer(ctx context.Context) (<-chan PersistedItem, error) {
	q.consumerM.Lock()
	defer q.consumerM.Unlock()

	if q.group != nil {
		return q.group.items, nil
	}

	stream, err := q.ensureStream(ctx)
	if err != nil {
		return nil, err
	}

	pullCtx, cancel := context.WithCancel(context.Background())

	items := make(chan PersistedItem)
	g := &natsPersistedConsumerGroup{
		items:  items,
		cancel: cancel,
	}

	// Create each shard's consumer and open its pull subscription first; only
	// launch the puller goroutines once all succeeded, so a mid-loop failure
	// tears down cleanly without leaking half-started pullers.
	for i := 0; i < q.shards; i++ {
		c := &natsPersistedConsumer{
			queue: q,
			items: items,
			shard: i,
			group: g,
		}
		if r := c.connect(ctx, stream); r != nil {
			g.stopContexts()
			cancel()
			return nil, r
		}
		g.consumers = append(g.consumers, c)
	}

	g.launch(pullCtx)

	// Close the shared items channel once every puller has exited. A shard
	// that dies spontaneously now rebuilds itself in place and keeps running
	// (see run), so this only fires on a real stop (leadership loss / ctx
	// cancel). Closing the channel is what makes the cache's apply loop
	// observe the stop; the clearGroup belt-and-suspenders restart path
	// remains for the degenerate case of every puller exiting (RT-13042 M4).
	go func() {
		g.wg.Wait()
		close(items)
		q.clearGroup(g)
	}()

	q.group = g
	return items, nil
}

// stopConsumer cancels the active consumer group and Naks each shard's
// in-flight item (if any) so the next leader picks them up immediately.
func (q *natsPersistedQueue) stopConsumer() error {
	q.consumerM.Lock()
	defer q.consumerM.Unlock()
	if q.group == nil {
		return nil
	}
	g := q.group
	q.group = nil
	g.stop()
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

// clearGroup drops the active-group reference if it still points at g.
// Called from the group's closer goroutine once every puller has exited.
// A shard that dies spontaneously now rebuilds itself in place rather than
// exiting, so on the regular stop path stopConsumer has already cleared the
// reference and this is a no-op. It remains as a safety net for the degenerate
// case of every puller exiting without a stopConsumer: the queue must not keep
// handing the dead group's closed items channel out of startConsumer, or the
// death would persist until a leadership flip AND an explicit
// StopPersistedConsumer (RT-13042 M4).
func (q *natsPersistedQueue) clearGroup(g *natsPersistedConsumerGroup) {
	q.consumerM.Lock()
	if q.group == g {
		q.group = nil
	}
	q.consumerM.Unlock()
}

// shardDurable returns the durable consumer name for shard i. With a single
// shard it is the original unsuffixed name so existing durables are reused.
func (q *natsPersistedQueue) shardDurable(i int) string {
	if q.shards <= 1 {
		return persistedConsumerNamePrefix
	}
	return fmt.Sprintf("%s-s%d", persistedConsumerNamePrefix, i)
}

// shardFilter returns the subject filter for shard i's consumer.
func (q *natsPersistedQueue) shardFilter(i int) string {
	return fmt.Sprintf("%s.s%d.>", q.subjectBase, i)
}

// shardSubject returns the publish subject for the given routing key. Keys
// hash deterministically to a shard so all writes for one key keep their
// relative order; an empty key routes to shard 0. With a single shard the
// original subject is used unchanged.
func (q *natsPersistedQueue) shardSubject(key string) string {
	if q.shards <= 1 {
		return q.subject
	}
	return fmt.Sprintf("%s.s%d.msg", q.subjectBase, q.shardOf(key))
}

func (q *natsPersistedQueue) shardOf(key string) int {
	if q.shards <= 1 || key == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(q.shards))
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

// persistedRetryFromMsg reports whether the message was published with the
// retry flag set (persistedRetryHeader == "1"). Absent header means non-retry.
func persistedRetryFromMsg(msg jetstream.Msg) bool {
	if hdr := msg.Headers(); hdr != nil {
		return hdr.Get(persistedRetryHeader) == "1"
	}
	return false
}

// persistedDeadlineFromMsg extracts the publisher's call deadline from the
// message header. ok is false when no deadline header was stamped (publisher
// had no deadline) or it cannot be parsed.
func persistedDeadlineFromMsg(msg jetstream.Msg) (time.Time, bool) {
	hdr := msg.Headers()
	if hdr == nil {
		return time.Time{}, false
	}
	v := hdr.Get(persistedDeadlineHeader)
	if v == "" {
		return time.Time{}, false
	}
	nanos, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, nanos), true
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
