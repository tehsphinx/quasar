package transports

import (
	"context"
	"errors"
	"sync"

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

	// queue is the in-memory persisted-FIFO buffer. publish() pushes
	// onto it; the active consumer reads from inflightCh in arrival
	// order. Bounded enough that producers can fan in faster than
	// the consumer drains without deadlocking the test cluster.
	queue []*inmemPersistedItem

	// activeConsumer holds the transport that currently owns the
	// consumer. nil when no leader is draining. When set, deliver()
	// fans the next queued item out via inflightCh; when cleared
	// (stopConsumer), any in-flight item is requeued at the head so
	// the next claimant gets it immediately.
	activeConsumer *InmemTransport
	inflightCh     chan PersistedItem
	inflightItem   *inmemPersistedItem

	// releaseCh is closed by releaseLocked whenever the current claim
	// ends, signalling the per-claim watchdog goroutine to exit. Without
	// it a watchdog that started for a claim ended via stopConsumer would
	// stay blocked on its ctx until that ctx happened to be cancelled,
	// leaking one goroutine per leadership flip (RT-13042 m21).
	releaseCh chan struct{}
}

// NewInmemQueueHub constructs an empty hub. Attach it to every Inmem
// transport in the test cluster (via AttachQueueHub or the
// ConnectInmemQueueHub helper) before constructing the cache instances
// so newCache observes SupportsPersisted() == true.
func NewInmemQueueHub() *InmemQueueHub {
	return &InmemQueueHub{}
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

// publish enqueues a Store command and blocks until the active
// consumer either replies or the context is cancelled.
// The inmem hub is a single un-sharded queue, so PersistedStoreOpts (ShardKey)
// carries nothing it needs to act on.
func (h *InmemQueueHub) publish(ctx context.Context, command *pb.Store, _ PersistedStoreOpts) (*pb.StoreResponse, error) {
	item := &inmemPersistedItem{
		hub:     h,
		command: command,
		replyCh: make(chan inmemPersistedReply, 1),
	}

	h.m.Lock()
	h.queue = append(h.queue, item)
	h.deliverLocked()
	h.m.Unlock()

	select {
	case reply := <-item.replyCh:
		return reply.resp, reply.err
	case <-ctx.Done():
		h.cancelPublish(item)
		return nil, ctx.Err()
	}
}

// cancelPublish removes a still-queued item from the buffer when the
// publisher's context is cancelled before delivery. If the item is
// already in flight, the consumer's reply still lands on its replyCh —
// but the publisher has stopped listening, so that's a no-op.
func (h *InmemQueueHub) cancelPublish(item *inmemPersistedItem) {
	h.m.Lock()
	defer h.m.Unlock()
	for i, q := range h.queue {
		if q == item {
			h.queue = append(h.queue[:i], h.queue[i+1:]...)
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

	h.activeConsumer = t
	h.inflightCh = make(chan PersistedItem, 1)
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
	if h.inflightItem != nil {
		// Put the in-flight item back at the head of the queue so
		// the next claimant sees it first. Equivalent to a JS NAK.
		h.queue = append([]*inmemPersistedItem{h.inflightItem}, h.queue...)
		h.inflightItem = nil
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

// deliverLocked moves the head of the queue into the consumer's inflight
// slot if both a consumer is active and no item is currently in flight.
// Called with h.m held.
func (h *InmemQueueHub) deliverLocked() {
	if h.activeConsumer == nil || h.inflightItem != nil || len(h.queue) == 0 {
		return
	}
	item := h.queue[0]
	h.queue = h.queue[1:]
	h.inflightItem = item
	// Non-blocking send: inflightCh is buffered with cap 1, so this
	// always succeeds when inflightItem was nil (deliver is only
	// reached when the previous item was settled).
	//
	// The default branch cannot be reached by a concurrent close: a
	// select's default does NOT make a send on a closed channel safe —
	// that would panic. It is safe only because inflightCh is closed and
	// nil'd together under h.m (releaseLocked), and deliverLocked also
	// runs under h.m; so whenever we get here inflightCh is non-nil, open,
	// and (since inflightItem was just nil) empty. The default is kept as
	// a defensive no-op for the otherwise-impossible empty-buffer miss.
	select {
	case h.inflightCh <- item:
	default:
		// Item could not be handed off; put it back at the head for the
		// next claimant.
		h.queue = append([]*inmemPersistedItem{item}, h.queue...)
		h.inflightItem = nil
	}
}

// settle is called by an inmemPersistedItem when the consumer terminates
// it (success / error / NAK). The hub uses this hook to clear the
// inflight slot and deliver the next queued item.
func (h *InmemQueueHub) settle(item *inmemPersistedItem, reply inmemPersistedReply, requeue bool) {
	h.m.Lock()
	defer h.m.Unlock()
	if h.inflightItem != item {
		return
	}
	h.inflightItem = nil
	if requeue {
		// NAK path: keep the item, push back to the head, deliver to
		// the next consumer (often after a leader flip).
		h.queue = append([]*inmemPersistedItem{item}, h.queue...)
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
	settled bool
	m       sync.Mutex
}

func (i *inmemPersistedItem) Command() *pb.Store {
	return i.command
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
