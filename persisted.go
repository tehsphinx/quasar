package quasar

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/transports"
)

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
	resp, err := s.transport.StorePersisted(ctx, storeCmd)
	if err != nil {
		err = reattachNoLeader(err)
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
			select {
			case <-ctx.Done():
				return
			default:
			}
			// Cache is still wiring up; retry on the next ctxRaft cycle.
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
// fired) or when ctx is done. Each item is settled exactly once via
// ReplySuccess, ReplyError, or Nack.
func (s *Cache) runPersistedApplyLoop(ctx context.Context, ch <-chan transports.PersistedItem) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-ch:
			if !ok {
				return
			}
			s.applyPersistedItem(ctx, item)
		}
	}
}

// applyPersistedItem runs the standard leader-local apply path for
// the queued Store and routes the outcome back to the publisher.
// Leadership-transition errors (raft.ErrNotLeader, ErrLeadershipLost,
// ErrEnqueueTimeout, ErrRaftShutdown) Nak the item so the next leader's
// consumer picks it up; apply-side errors (FSM rejection, persist
// failure) reply with an error to terminate the publisher's wait.
func (s *Cache) applyPersistedItem(ctx context.Context, item transports.PersistedItem) {
	cmd := &pb.Command{Cmd: &pb.Command_Store{Store: item.Command()}}
	resp, uid, err := s.applyLocal(ctx, cmd)
	if err != nil {
		if isLeadershipTransitionError(err) {
			_ = item.Nack(ctx)
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
	_ = item.ReplySuccess(ctx, storeResp)
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
