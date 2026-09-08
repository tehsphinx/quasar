package transports

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tehsphinx/quasar/pb/v1"
)

// InmemQueueHub is the persisted-FIFO backing store for InmemTransport.
// One hub is shared across every transport in an in-memory test cluster
// — that gives the same single-in-flight-item invariant the NATS JS
// work-queue provides, but in-process and without dependencies. The
// active consumer (the leader's transport) drains queueCh; producers
// (any transport, leader or follower) publish through publish().
//
// The hub is intentionally simple: it owns a goroutine per pending
// publish to fan out the leader's reply back to the publisher, and the
// active consumer claim is a single-slot mutex. Adequate for tests.
//
// NewInmemQueueHub creates an unstarted hub; AttachQueueHub on each
// InmemTransport (or the ConnectInmemQueueHub helper) wires the
// transports into it.
type InmemQueueHub struct {
	m sync.Mutex

	// shards are the FIFO partitions. A single-shard hub (NewInmemQueueHub) is
	// the original un-partitioned behaviour; NewShardedInmemQueueHub gives a
	// test the same routing the NATS transport does, so the cache's per-shard
	// apply workers can be exercised in-process (RT-14337).
	shards []*inmemQueueShard

	// activeConsumer holds the transport that currently owns the
	// consumer. nil when no leader is draining. When set, deliver()
	// fans each shard's next queued item out via inflightCh; when cleared
	// (stopConsumer), every in-flight item is requeued at its shard's head
	// so the next claimant gets it immediately.
	activeConsumer *InmemTransport
	inflightCh     chan PersistedItem

	// releaseCh is closed by releaseLocked whenever the current claim
	// ends, signalling the per-claim watchdog goroutine to exit. Without
	// it a watchdog that started for a claim ended via stopConsumer would
	// stay blocked on its ctx until that ctx happened to be cancelled,
	// leaking one goroutine per leadership flip (RT-13042 m21).
	releaseCh chan struct{}
}

// inmemQueueShard is one FIFO partition: its own pending queue and its own
// single in-flight slot. The slot is the in-memory equivalent of the NATS
// transport's per-shard durable with MaxAckPending = 1 — one unsettled item
// per partition, independent of every other partition.
type inmemQueueShard struct {
	// queue is this partition's pending buffer. publish() pushes onto it; the
	// active consumer takes the head via inflightCh in arrival order.
	queue []*inmemPersistedItem
	// inflight is the item handed to the consumer and not yet settled. While
	// it is set this partition delivers nothing further.
	inflight *inmemPersistedItem
}

// NewInmemQueueHub constructs an empty single-partition hub. Attach it to
// every Inmem transport in the test cluster (via AttachQueueHub or the
// ConnectInmemQueueHub helper) before constructing the cache instances
// so newCache observes SupportsPersisted() == true.
func NewInmemQueueHub() *InmemQueueHub {
	return NewShardedInmemQueueHub(1)
}

// NewShardedInmemQueueHub constructs an empty hub with n FIFO partitions,
// routing by PersistedStoreOpts.ShardKey with the same hash and modulo the
// NATS transport uses (see persistedShardOf). Each partition keeps one item in
// flight independently, so a test can exercise the real
// publish -> partition -> per-shard apply worker path in-process instead of
// needing a JetStream server.
//
// n < 1 is treated as 1, which is exactly NewInmemQueueHub.
func NewShardedInmemQueueHub(n int) *InmemQueueHub {
	if n < 1 {
		n = 1
	}
	shards := make([]*inmemQueueShard, n)
	for i := range shards {
		shards[i] = &inmemQueueShard{}
	}
	return &InmemQueueHub{shards: shards}
}

// ensureShardsLocked gives a zero-value hub its single partition, so a hub
// built as &InmemQueueHub{} still behaves like NewInmemQueueHub(). Called with
// h.m held.
func (h *InmemQueueHub) ensureShardsLocked() {
	if len(h.shards) == 0 {
		h.shards = []*inmemQueueShard{{}}
	}
}

// ConnectInmemQueueHub attaches the given hub to every passed Inmem
// transport, then returns the hub so the caller can keep a handle to
// it. Convenience wrapper for the common test-setup pattern.
func ConnectInmemQueueHub(hub *InmemQueueHub, transports ...*InmemTransport) *InmemQueueHub {
	if hub == nil {
		hub = NewInmemQueueHub()
	}
	for _, t := range transports {
		t.AttachQueueHub(hub)
	}
	return hub
}

// WithInmemPersistedQueue wires a freshly-constructed Inmem transport
// to the given hub. Designed to be passed at construction time when
// the caller doesn't want to call AttachQueueHub separately. Equivalent
// to AttachQueueHub.
func WithInmemPersistedQueue(hub *InmemQueueHub) func(*InmemTransport) {
	return func(t *InmemTransport) {
		t.AttachQueueHub(hub)
	}
}

// publish enqueues a Store command into the partition selected by
// opts.ShardKey and blocks until the active consumer either replies or the
// context is cancelled.
func (h *InmemQueueHub) publish(ctx context.Context, command *pb.Store, opts PersistedStoreOpts) (*pb.StoreResponse, error) {
	item := &inmemPersistedItem{
		hub:     h,
		command: command,
		replyCh: make(chan inmemPersistedReply, 1),
		retry:   opts.Retry,
	}
	item.deadline, item.hasDeadline = ctx.Deadline()

	h.m.Lock()
	h.ensureShardsLocked()
	item.shard = persistedShardOf(opts.ShardKey, len(h.shards))
	sh := h.shards[item.shard]
	sh.queue = append(sh.queue, item)
	h.deliverLocked()
	h.m.Unlock()

	select {
	case reply := <-item.replyCh:
		return reply.resp, reply.err
	case <-ctx.Done():
		// A non-retry publish gives up with its context: drop the still-queued
		// item so it is never applied after the caller stopped waiting. A retry
		// publish leaves the item on the queue — like a durable JetStream
		// message, redelivery is independent of the publisher's wait, so the
		// next leader still applies it (RT-12964).
		if !opts.Retry {
			h.cancelPublish(item)
		}
		return nil, ctx.Err()
	}
}

// cancelPublish removes a still-queued item from its partition's buffer when
// the publisher's context is cancelled before delivery. If the item is
// already in flight, the consumer's reply still lands on its replyCh —
// but the publisher has stopped listening, so that's a no-op.
func (h *InmemQueueHub) cancelPublish(item *inmemPersistedItem) {
	h.m.Lock()
	defer h.m.Unlock()
	sh := h.shards[item.shard]
	for i, q := range sh.queue {
		if q == item {
			sh.queue = append(sh.queue[:i], sh.queue[i+1:]...)
			return
		}
	}
}

// startConsumer claims the consumer slot for the given transport. Only
// one claim is allowed at a time; a second concurrent claim returns an
// error and the existing consumer keeps running.
func (h *InmemQueueHub) startConsumer(ctx context.Context, t *InmemTransport) (<-chan PersistedItem, error) {
	h.m.Lock()
	defer h.m.Unlock()

	if h.activeConsumer != nil && h.activeConsumer != t {
		return nil, errors.New("inmem persisted queue: another consumer is already active")
	}
	if h.activeConsumer == t && h.inflightCh != nil {
		return h.inflightCh, nil
	}

	h.ensureShardsLocked()
	h.activeConsumer = t
	// One slot per partition: every shard can have an item in flight at the
	// same time, and deliverLocked must never have to put one back.
	h.inflightCh = make(chan PersistedItem, len(h.shards))
	h.releaseCh = make(chan struct{})

	go func(ch chan PersistedItem, released <-chan struct{}) {
		select {
		case <-ctx.Done():
			// Best-effort: if the consumer's ctx is cancelled before
			// stopConsumer fires, drop the claim so the next leader can
			// take over without waiting.
			h.releaseConsumerIfOwner(t, ch)
		case <-released:
			// The claim ended through stopConsumer (or a ctx-cancel for a
			// different claim that re-claimed in between); nothing to do
			// but exit so this goroutine does not outlive its claim.
		}
	}(h.inflightCh, h.releaseCh)

	h.deliverLocked()
	return h.inflightCh, nil
}

// stopConsumer releases the consumer slot. The in-flight item (if any)
// is requeued at the head so the next claimant picks it up immediately.
func (h *InmemQueueHub) stopConsumer(t *InmemTransport) error {
	h.m.Lock()
	defer h.m.Unlock()
	return h.releaseLocked(t)
}

// releaseConsumerIfOwner is the goroutine-safe counterpart for the
// ctx-cancellation path. The check ensures we don't tear down a
// consumer that has already been replaced.
func (h *InmemQueueHub) releaseConsumerIfOwner(t *InmemTransport, ch chan PersistedItem) {
	h.m.Lock()
	defer h.m.Unlock()
	if h.activeConsumer != t || h.inflightCh != ch {
		return
	}
	_ = h.releaseLocked(t)
}

func (h *InmemQueueHub) releaseLocked(t *InmemTransport) error {
	if h.activeConsumer != t {
		return nil
	}
	// Put every in-flight item back at the head of its own partition so the
	// next claimant sees it first. Equivalent to a JS NAK per shard.
	for _, sh := range h.shards {
		if sh.inflight != nil {
			sh.queue = append([]*inmemPersistedItem{sh.inflight}, sh.queue...)
			sh.inflight = nil
		}
	}
	if h.inflightCh != nil {
		close(h.inflightCh)
		h.inflightCh = nil
	}
	if h.releaseCh != nil {
		// Wake the per-claim watchdog so it exits with the claim instead
		// of leaking until its ctx is cancelled (RT-13042 m21).
		close(h.releaseCh)
		h.releaseCh = nil
	}
	h.activeConsumer = nil
	return nil
}

// deliverLocked moves each partition's queue head into that partition's
// inflight slot, for every partition that has a consumer, something queued and
// nothing in flight. Partitions are independent: one shard holding an
// unsettled item never delays another shard's delivery. Called with h.m held.
func (h *InmemQueueHub) deliverLocked() {
	if h.activeConsumer == nil {
		return
	}
	for _, sh := range h.shards {
		if sh.inflight != nil || len(sh.queue) == 0 {
			continue
		}
		item := sh.queue[0]
		sh.queue = sh.queue[1:]
		sh.inflight = item
		// Non-blocking send: inflightCh has one slot per partition and each
		// partition holds at most one item in flight, so this always succeeds.
		//
		// The default branch cannot be reached by a concurrent close: a
		// select's default does NOT make a send on a closed channel safe —
		// that would panic. It is safe only because inflightCh is closed and
		// nil'd together under h.m (releaseLocked), and deliverLocked also
		// runs under h.m; so whenever we get here inflightCh is non-nil and
		// open. The default is kept as a defensive no-op for the
		// otherwise-impossible full-buffer miss.
		select {
		case h.inflightCh <- item:
		default:
			// Item could not be handed off; put it back at the head of its
			// partition for the next delivery pass.
			sh.queue = append([]*inmemPersistedItem{item}, sh.queue...)
			sh.inflight = nil
		}
	}
}

// settle is called by an inmemPersistedItem when the consumer terminates
// it (success / error / NAK). The hub uses this hook to clear that
// partition's inflight slot and deliver its next queued item.
func (h *InmemQueueHub) settle(item *inmemPersistedItem, reply inmemPersistedReply, requeue bool) {
	h.m.Lock()
	defer h.m.Unlock()

	sh := h.shards[item.shard]
	if sh.inflight != item {
		return
	}
	sh.inflight = nil
	if requeue {
		// NAK path: keep the item, push back to the head of its partition,
		// deliver to the next consumer (often after a leader flip).
		sh.queue = append([]*inmemPersistedItem{item}, sh.queue...)
	} else {
		// Success / error path: notify the publisher.
		select {
		case item.replyCh <- reply:
		default:
		}
	}
	h.deliverLocked()
}

// inmemPersistedReply is the unified reply envelope (success or error).
type inmemPersistedReply struct {
	resp *pb.StoreResponse
	err  error
}

// inmemPersistedItem is the in-memory equivalent of a NATS JS message in
// flight to the leader. The consumer terminates it via ReplySuccess,
// ReplyError, or Nack — each lands in the hub's settle() so the next
// queued item can be delivered.
type inmemPersistedItem struct {
	hub     *InmemQueueHub
	command *pb.Store
	replyCh chan inmemPersistedReply
	retry   bool
	// shard is the partition publish() routed this item to. Set once, before
	// the item is queued, and read without the hub lock afterwards.
	shard int
	// deadline mirrors the publisher's call deadline; hasDeadline is false
	// when the publisher had no deadline. Carried so the leader's apply path
	// can drop a non-retry command picked up after the publisher gave up,
	// exactly as the NATS transport does via persistedDeadlineHeader.
	deadline    time.Time
	hasDeadline bool
	settled     bool
	m           sync.Mutex
}

func (i *inmemPersistedItem) Command() *pb.Store {
	return i.command
}

func (i *inmemPersistedItem) Shard() int {
	return i.shard
}

func (i *inmemPersistedItem) Retry() bool {
	return i.retry
}

func (i *inmemPersistedItem) Deadline() (time.Time, bool) {
	return i.deadline, i.hasDeadline
}

func (i *inmemPersistedItem) ReplySuccess(_ context.Context, resp *pb.StoreResponse) error {
	return i.terminate(inmemPersistedReply{resp: resp}, false)
}

func (i *inmemPersistedItem) ReplyError(_ context.Context, err error) error {
	return i.terminate(inmemPersistedReply{err: err}, false)
}

func (i *inmemPersistedItem) Nack(_ context.Context) error {
	return i.terminate(inmemPersistedReply{}, true)
}

// NackBackoff requeues the item like Nack. The inmem hub redelivers from the
// head with no MaxDeliver budget to exhaust, so the delayed-redelivery contract
// (which exists to protect the NATS MaxDeliver budget) is a no-op here.
func (i *inmemPersistedItem) NackWithDelay(_ context.Context) error {
	return i.terminate(inmemPersistedReply{}, true)
}

func (i *inmemPersistedItem) terminate(reply inmemPersistedReply, requeue bool) error {
	i.m.Lock()
	if i.settled {
		i.m.Unlock()
		return errors.New("inmem persisted item already settled")
	}
	i.settled = true
	i.m.Unlock()
	i.hub.settle(i, reply, requeue)
	return nil
}
