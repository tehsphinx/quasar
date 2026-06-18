// Package transports contains different transport implementations for the quasar cache.
package transports

import (
	"context"
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
)

const (
	defaultTimout = 5 * time.Second
	// defaultHeartbeatTimeout bounds a single heartbeat AppendEntries
	// round-trip. It is much shorter than defaultTimout (which also covers
	// snapshot-sized entry appends) and tracks raft's default
	// HeartbeatTimeout (1s): a follower that does not answer a heartbeat
	// within this window is treated as not responding on the dedicated
	// heartbeat subject, and the leader falls back to entries.append
	// (see NATSTransport.AppendEntries, RT-13010).
	defaultHeartbeatTimeout = 1 * time.Second
	consumerChanSize        = 16
)

// ErrPersistedNotSupported is returned by transports that do not implement
// the persisted-FIFO mode when StorePersisted / StartPersistedConsumer is
// invoked on them. Callers should check SupportsPersisted() first.
var ErrPersistedNotSupported = errors.New("transport does not support persisted FIFO")

// ErrAlreadySettled is returned by PersistedItem.ReplySuccess / ReplyError /
// Nack when the item was already settled by a concurrent call. The caller's
// intended outcome did NOT take effect: most notably, a ReplySuccess that
// loses against a shutdown-path Nack means the command was applied locally
// but the queue item goes back for redelivery — the next leader will apply
// it again (RT-13042 M5).
var ErrAlreadySettled = errors.New("persisted item already settled")

// PersistedStoreOpts carries per-call routing metadata from the cache to the
// persisted-FIFO transport. It travels with the published message (NATS
// header / in-memory item field) so the consuming leader — which may be a
// different node than the publisher — can honor it (RT-12964).
type PersistedStoreOpts struct {
	// ShardKey selects the FIFO partition this command is routed to. Empty
	// routes to the default shard.
	ShardKey string

	// Retry marks the command as eligible for at-least-once redelivery when
	// the apply cannot run yet (no leader / leadership lost).
	Retry bool
}

// Transport defines an extended raft.Transport with additional commands for the cache.
type Transport interface {
	raft.Transport

	// CacheConsumer returns the cache consumer channel to which all incoming cache commands are sent.
	CacheConsumer() <-chan raft.RPC

	// Store asks the master to apply a change command to the raft cluster.
	Store(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.Store) (*pb.StoreResponse, error)

	// ResetCache asks the master to reset the cache.
	ResetCache(ctx context.Context, _ raft.ServerID, address raft.ServerAddress, request *pb.ResetCache) (*pb.ResetCacheResponse, error)

	// LatestUID asks the master to return its latest known / generated uid.
	LatestUID(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.LatestUid) (*pb.LatestUidResponse, error)

	// RemoveServer asks the leader to remove a server from the raft configuration.
	RemoveServer(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.RemoveServer) (*pb.RemoveServerResponse, error)

	// SupportsPersisted reports whether this transport instance is configured
	// with a persisted-FIFO backing store (NATS JetStream work-queue for the
	// NATS transport, an in-memory FIFO channel for the Inmem transport).
	//
	// When true, the cache routes ALL Store commands (leader's own writes
	// included) through StorePersisted / StartPersistedConsumer instead of
	// the synchronous leader-RPC path. The single in-flight item on the
	// queue provides strict cluster-wide FIFO ordering of writes, and a
	// missing leader is no longer a write blocker — the publish lands in
	// the persisted store and the next leader's consumer applies it.
	//
	// Transports that do not support persisted-FIFO must return false here
	// and may return ErrPersistedNotSupported from StorePersisted /
	// StartPersistedConsumer.
	SupportsPersisted() bool

	// StorePersisted publishes a Store command into the persisted-FIFO
	// stream and waits for the leader's reply (success+UID or apply error).
	// The publish path itself is leader-agnostic; the reply travels back
	// over an out-of-band channel (NATS request-reply inbox for the NATS
	// transport, per-call result channel for the Inmem transport).
	//
	// Returns when the leader replies, when ctx is done, or when the
	// transport-level publish fails.
	//
	// opts carries routing (ShardKey) metadata that is published alongside the
	// command so the consuming leader routes it to the right shard.
	StorePersisted(ctx context.Context, command *pb.Store, opts PersistedStoreOpts) (*pb.StoreResponse, error)

	// StartPersistedConsumer begins draining the persisted-FIFO stream on
	// this node and returns a channel of PersistedItem. Called by the
	// quasar cache when this node becomes leader. Each item must be
	// terminated by ReplySuccess, ReplyError, or Nack — the queue's
	// single-in-flight-item invariant blocks the next delivery until
	// that happens.
	//
	// Calling StartPersistedConsumer when the transport doesn't support
	// persisted-FIFO returns ErrPersistedNotSupported.
	StartPersistedConsumer(ctx context.Context) (<-chan PersistedItem, error)

	// StopPersistedConsumer stops the consumer started by
	// StartPersistedConsumer. The in-flight item (if any) is Nak'd so the
	// next consumer (typically the new leader after a leadership flip)
	// picks it up without waiting for the AckWait window to expire.
	StopPersistedConsumer() error
}

// PersistedItem is a single Store command delivered by
// StartPersistedConsumer. The consumer must terminate every item by
// calling exactly one of ReplySuccess / ReplyError / Nack — the
// persisted stream's MaxAckPending = 1 invariant pauses delivery until
// the in-flight item is settled.
//
// Delivery on the persisted path is AT-LEAST-ONCE: redelivery is inherent
// to the backing queue (AckWait expiry, leadership flips, a Nack racing a
// successful apply — see ErrAlreadySettled). FSM implementations whose
// commands are not naturally idempotent must deduplicate replayed
// commands themselves.
type PersistedItem interface {
	// Command returns the underlying Store command.
	Command() *pb.Store

	// Retry reports whether the publisher marked this command as eligible
	// for at-least-once redelivery (PersistedStoreOpts.Retry). The consumer
	// uses it to decide, on a leadership-transition error, whether to Nack
	// for redelivery (retry) or terminate the publisher's call (non-retry).
	Retry() bool

	// Deadline returns the publisher's call deadline; ok is false when the
	// publisher had no deadline. The consumer uses it to drop a non-retry
	// command picked up after the caller already gave up (RT-12964).
	Deadline() (deadline time.Time, ok bool)

	// ReplySuccess sends the apply result back to the publisher and acks
	// the underlying queue item. The next item is delivered after the
	// current one is acked.
	ReplySuccess(ctx context.Context, resp *pb.StoreResponse) error

	// ReplyError sends an apply error back to the publisher and acks the
	// queue item. Apply errors are terminal for the publisher's call —
	// JetStream will not redeliver, because the payload itself was
	// rejected by the FSM rather than failing to reach a leader.
	ReplyError(ctx context.Context, err error) error

	// Nack returns the item to the queue for immediate redelivery to the
	// next consumer. Used when the consumer is shutting down mid-flight
	// (e.g. leader step-down) so the next leader doesn't have to wait
	// for the AckWait window to expire.
	Nack(ctx context.Context) error

	// NackWithDelay returns the item to the queue for redelivery after a
	// transport-chosen backoff (≈ the consumer's AckWait), rather than the
	// immediate redelivery of Nack. Used on a leadership-transition error so a
	// flip doesn't hot-loop the shard through its MaxDeliver budget before a new
	// leader is elected (RT-12964).
	NackWithDelay(ctx context.Context) error
}

func snapshotTimeout(origTimeout time.Duration, size int64) time.Duration {
	timeout := origTimeout * time.Duration(size/int64(raft.DefaultTimeoutScale))
	if timeout < origTimeout {
		return origTimeout
	}
	return timeout
}
