package quasar

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/transports"
)

// persistedConsumerRestartDelay throttles restarting a persisted consumer
// whose item channel closed while this node is still leader (RT-13042 M4).
const persistedConsumerRestartDelay = 500 * time.Millisecond

// routePersisted is the write path used when the configured transport
// supports persisted-FIFO. Every Store call from any node (leader or
// follower) publishes a single message into the persisted-FIFO stream
// and waits for the leader's reply on the embedded reply inbox. The
// queue's MaxAckPending = 1 invariant gives strict cluster-wide FIFO
// ordering of writes.
//
// When the caller asked for WithRetry() and the wait for the reply
// times out (ctx.Err() != nil), we wrap the error as
// ErrRetrying — the publish itself succeeded and the JS-level
// MaxDeliver × AckWait redelivery budget continues independently.
// Without WithRetry() we return the underlying error unwrapped, which
// stays compatible with today's ErrNoLeader-returning callers.
func (s *Cache) routePersisted(ctx context.Context, storeCmd *pb.Store, opts storeOpts) (*pb.CommandResponse, uint64, error) {
	pOpts := transports.PersistedStoreOpts{ShardKey: opts.shardKey, Retry: opts.retry}

	// The publish into the persisted-FIFO stream is durable and leader-agnostic,
	// but the reply only arrives once a leader's consumer applies the item. With
	// no leader there is no consumer, so the reply wait would block until a
	// leader appears (or ctx expires). When no leader is currently visible, gate
	// the wait on leadership using the same fast quorum probe the forward path
	// uses: the moment the cluster is confirmed leaderless, cancel the wait and
	// return. The publish has already landed (the transport detaches a retry
	// publish from this cancellation), so a retry write stays queued for the next
	// leader (ErrRetrying) and a non-retry write is dropped by its deadline
	// (ErrNoLeader) instead of the caller hanging (RT-12964).
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if !s.hasLeader() {
		go func() {
			if _, _, err := s.getLeaderWait(ctx); err != nil {
				cancel(ErrNoLeader)
			}
		}()
	}

	resp, err := s.transport.StorePersisted(ctx, storeCmd, pOpts)
	if err != nil {
		err = reattachNoLeader(err)
		if cause := context.Cause(ctx); !opts.retry && errors.Is(cause, ErrNoLeader) {
			return nil, 0, cause
		}
		if opts.retry && ctxTimedOut(ctx, err) {
			return nil, 0, fmt.Errorf("%w: %w", ErrRetrying, err)
		}
		return nil, 0, err
	}
	return respStore(resp), resp.GetUid(), nil
}

// ctxTimedOut reports whether err is a wait-timeout / deadline expiry
// on ctx. Used to distinguish a "publish succeeded, no reply yet" wait
// timeout from a transport-level publish failure: only the wait timeout
// becomes ErrRetrying under WithRetry.
func ctxTimedOut(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if ctx.Err() != nil {
		return true
	}
	return false
}

// runPersistedConsumer is the long-lived loop that drives the
// persisted-FIFO consumer for the local node. It blocks until the
// cache shuts down. Internally it watches for leadership changes and
// starts / stops the underlying transport consumer based on whether
// this node is currently leader.
//
// Bound to the cache via newCache when the transport reports
// SupportsPersisted() == true. No-op for transports without
// persisted-FIFO.
func (s *Cache) runPersistedConsumer(ctx context.Context) {
	for {
		rft, ctxRaft := s.getRaftWithCtx()
		if rft == nil {
			// Cache is still wiring up; wait briefly before retrying so this
			// loop doesn't busy-spin the CPU (m7), mirroring
			// runQuorumRecoveryWatch.
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		s.watchLeadershipOnRaft(ctx, ctxRaft, rft)

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// watchLeadershipOnRaft watches the supplied raft instance for
// leadership transitions and toggles the persisted consumer. Returns
// when ctx (cache shutdown) or ctxRaft (this raft replaced — e.g.
// recoverQuorum) is done so the outer loop can re-register on the new
// raft.
func (s *Cache) watchLeadershipOnRaft(ctx, ctxRaft context.Context, rft *raft.Raft) {
	chObs := make(chan raft.Observation, observationChanSize)
	observer := raft.NewObserver(chObs, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	rft.RegisterObserver(observer)
	defer rft.DeregisterObserver(observer)

	// Seed with the current leadership state; raft only emits
	// LeaderObservation on changes.
	s.applyLeadershipState(ctx)

	for {
		select {
		case <-ctx.Done():
			_ = s.transport.StopPersistedConsumer()
			return
		case <-ctxRaft.Done():
			_ = s.transport.StopPersistedConsumer()
			return
		case <-chObs:
			s.applyLeadershipState(ctx)
		}
	}
}

// applyLeadershipState is the decision point that starts the persisted
// consumer when we become leader and stops it when we lose it.
func (s *Cache) applyLeadershipState(ctx context.Context) {
	if s.IsLeader() {
		s.startPersistedConsumerOnce(ctx)
		return
	}
	_ = s.transport.StopPersistedConsumer()
}

// startPersistedConsumerOnce starts a goroutine that drains the
// transport's persisted-FIFO channel and applies each item locally.
// Idempotent: a second call while a previous consumer is still
// draining returns the existing channel from the transport, which
// hashes by transport identity, so we still drive only one
// applyLoop per leader claim.
func (s *Cache) startPersistedConsumerOnce(ctx context.Context) {
	ch, err := s.transport.StartPersistedConsumer(ctx)
	if err != nil {
		if !errors.Is(err, transports.ErrPersistedNotSupported) {
			s.logger.Error("failed to start persisted consumer", "error", err)
		}
		return
	}
	go s.runPersistedApplyLoop(ctx, ch)
}

// runPersistedApplyLoop applies items received from the persisted-FIFO
// consumer. Exits when the channel is closed (StopPersistedConsumer
// fired, or the consumer died on its own) or when ctx is done. Each item
// is settled exactly once via ReplySuccess, ReplyError, or Nack.
func (s *Cache) runPersistedApplyLoop(ctx context.Context, ch <-chan transports.PersistedItem) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-ch:
			if !ok {
				s.restartPersistedConsumerIfLeader(ctx)
				return
			}
			s.applyPersistedItem(ctx, item)
		}
	}
}

// restartPersistedConsumerIfLeader restarts the persisted consumer after its
// item channel closed underneath a node that is still leader. A closed
// channel normally means StopPersistedConsumer ran on leadership loss — but
// the consumer can also die spontaneously (JetStream consumer deleted
// server-side, unrecoverable JS error). In that case this node stays leader,
// so no leadership observation will ever restart the consumer, and every
// persisted write cluster-wide stalls until a leadership flip (RT-13042 M4).
// The brief delay keeps a consumer that dies instantly on start from
// hot-looping.
func (s *Cache) restartPersistedConsumerIfLeader(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(persistedConsumerRestartDelay):
	}

	if !s.IsLeader() {
		return
	}
	s.logger.Warn("persisted consumer stopped while still leader; restarting it")
	s.startPersistedConsumerOnce(ctx)
}

// applyPersistedItem runs the standard leader-local apply path for the queued
// Store and routes the outcome back to the publisher. Whether a command that
// cannot be applied is retried is a per-message property carried from the
// publisher (WithRetry):
//
//   - retry: leadership-transition errors (raft.ErrNotLeader,
//     ErrLeadershipLost, ErrEnqueueTimeout, ErrRaftShutdown) Nak the item with
//     a backoff so the next leader's consumer picks it up (at-least-once
//     background sync). The backoff (NackBackoff, not the immediate Nack used
//     for step-down handover) keeps a leadership transition from re-pulling and
//     re-failing the item in a tight loop that burns the MaxDeliver budget
//     before a new leader can be elected.
//   - non-retry: the item is terminated with ErrNoLeader instead of being
//     redelivered, and it is dropped outright once the publisher's deadline
//     has passed — a non-retry write must never be silently applied after the
//     caller was already told it failed (RT-12964).
//
// Apply-side errors (FSM rejection, persist failure) always reply with the
// error to terminate the publisher's wait, regardless of the retry flag.
func (s *Cache) applyPersistedItem(ctx context.Context, item transports.PersistedItem) {
	// Non-retry deadline protection is enforced in two places: here at pickup,
	// dropping an already-expired item before any work, and by binding the raft
	// apply to the same deadline (applyCtx) so an item picked up just before its
	// deadline can't keep applying past it (RT-12964).
	if persistedExpired(item, time.Now()) {
		_ = item.ReplyError(ctx, ErrNoLeader)
		return
	}

	applyCtx := ctx
	if dl, ok := item.Deadline(); ok && !item.Retry() {
		var cancel context.CancelFunc
		applyCtx, cancel = context.WithDeadline(ctx, dl)
		defer cancel()
	}

	cmd := &pb.Command{Cmd: &pb.Command_Store{Store: item.Command()}}
	resp, uid, err := s.applyLocal(applyCtx, cmd)
	if err != nil {
		if isLeadershipTransitionError(err) {
			if item.Retry() {
				_ = item.NackWithDelay(ctx)
			} else {
				_ = item.ReplyError(ctx, ErrNoLeader)
			}
			return
		}
		_ = item.ReplyError(ctx, err)
		return
	}
	storeResp := resp.GetStore()
	if storeResp == nil {
		storeResp = &pb.StoreResponse{}
	}
	if storeResp.Uid == 0 {
		storeResp.Uid = uid
	}
	if r := item.ReplySuccess(ctx, storeResp); errors.Is(r, transports.ErrAlreadySettled) {
		// A shutdown-path Nack won the settle race after the apply already
		// committed: the publisher gets no reply and the item is redelivered
		// to the next leader, which applies it a second time (RT-13042 M5).
		s.logger.Warn("persisted item nacked despite successful apply; command will be redelivered — FSM applies must be idempotent",
			"uid", storeResp.Uid)
	}
}

// persistedExpired reports whether a non-retry write must be dropped because
// its publisher deadline has already passed (RT-12964).
func persistedExpired(item transports.PersistedItem, now time.Time) bool {
	if item.Retry() {
		return false
	}
	dl, ok := item.Deadline()
	return ok && now.After(dl)
}

// isLeadershipTransitionError reports whether err signals that the
// apply could not run on the local raft because we are no longer (or
// not yet) the leader. The persisted-FIFO consumer Naks instead of
// replying with an error so the next leader can apply the same item.
func isLeadershipTransitionError(err error) bool {
	return errors.Is(err, raft.ErrNotLeader) ||
		errors.Is(err, raft.ErrLeadershipLost) ||
		errors.Is(err, raft.ErrLeadershipTransferInProgress) ||
		errors.Is(err, raft.ErrEnqueueTimeout) ||
		errors.Is(err, raft.ErrRaftShutdown)
}
