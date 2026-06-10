package quasar

import (
	"context"
	"time"
)

const (
	defaultWait = 5 * time.Second
	maxWait     = 30 * time.Second
)

// FSMInjector implements an object the FSM implementation gets access to, to control the data relevant part of the cache.
type FSMInjector struct {
	cache *Cache
}

// WaitFor waits for the given `uid` to be applied by the local FSM.
func (s *FSMInjector) WaitFor(ctx context.Context, uid uint64) error {
	ctx, cancel := getWaitCtx(ctx)
	defer cancel()

	return s.cache.fsm.WaitFor(ctx, uid)
}

// WaitForMasterLatest waits for the latest entry in the master's Raft log to be applied by the local FSM.
func (s *FSMInjector) WaitForMasterLatest(ctx context.Context) error {
	ctx, cancel := getWaitCtx(ctx)
	defer cancel()

	// TODO: add context to masterLastIndex
	uid, err := s.cache.masterLastIndex(ctx)
	if err != nil {
		return err
	}
	return s.cache.fsm.WaitFor(ctx, uid)
}

// WaitForKnownLatest waits for the latest locally known entry in the cache's Raft log to be applied by the local FSM.
func (s *FSMInjector) WaitForKnownLatest(ctx context.Context) error {
	ctx, cancel := getWaitCtx(ctx)
	defer cancel()

	// Anchor on the committed index, not LastIndex(): LastIndex includes
	// stored-but-uncommitted entries, so after a leadership loss the awaited
	// entry may never commit and the caller burns its whole deadline — the
	// same trap localLastIndex documents (RT-13042 M10). CommitIndex gives
	// the "known but not yet applied" entries this call is meant to wait out.
	uid := s.cache.raft().CommitIndex()
	return s.cache.fsm.WaitFor(ctx, uid)
}

// Store stores the given byte slice in the cache. It invokes the store method of the cache instance.
// It returns the unique identifier (UID) associated with the stored data and
// any error encountered during the operation.
//
// Variadic StoreOption arguments configure how the call interacts with a
// transport that supports persisted-FIFO mode (see WithRetry). Options
// are no-ops on transports that do not implement persisted-FIFO.
func (s *FSMInjector) Store(ctx context.Context, bts []byte, opts ...StoreOption) (uint64, error) {
	return s.cache.store(ctx, "", bts, opts...)
}

// IsLeader returns if the cache is the current leader. This is not a verified
// check, so it might be that it is not able to perform leadership actions.
func (s *FSMInjector) IsLeader() bool {
	return s.cache.IsLeader()
}

// HasLeader returns if the cache has a leader. Or at least if the current node
// thinks there is a leader. The leader might already be unreachable.
func (s *FSMInjector) HasLeader() bool {
	return s.cache.hasLeader()
}

// CacheID returns the id of the cache the fms is attached to.
func (s *FSMInjector) CacheID() string {
	return s.cache.localID
}

// getWaitCtx derives the context used for a consistency wait. A caller
// deadline at or under maxWait is honored as-is; a longer caller budget is
// capped at maxWait rather than being collapsed to defaultWait (RT-13042 m3 —
// previously a 60s caller budget silently became a 5s wait and maxWait was
// never used as an actual bound). When the caller has no deadline, defaultWait
// applies.
func getWaitCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithTimeout(ctx, defaultWait)
	}
	if time.Until(deadline) <= maxWait {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, maxWait)
}
