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

	uid := s.cache.raft.LastIndex()
	return s.cache.fsm.WaitFor(ctx, uid)
}

// Store stores the given byte slice in the cache. It invokes the store method of the cache instance.
// It returns the unique identifier (UID) associated with the stored data and
// any error encountered during the operation.
func (s *FSMInjector) Store(ctx context.Context, bts []byte) (uint64, error) {
	return s.cache.store(ctx, "", bts)
}

// IsLeader returns if the cache is the current leader. This is not a verified
// check, so it might be that it is not able to perform leadership actions.
func (s *FSMInjector) IsLeader() bool {
	return s.cache.isLeader()
}

func getWaitCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < maxWait {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultWait)
}
