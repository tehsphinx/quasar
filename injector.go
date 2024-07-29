package quasar

import (
	"context"
	"time"
)

const defaultWait = 5 * time.Second
const maxWait = 30 * time.Second

type FSMInjector struct {
	cache *Cache
}

func (s *FSMInjector) WaitFor(ctx context.Context, uid uint64) error {
	ctx, cancel := getWaitCtx(ctx)
	defer cancel()

	return s.cache.fsm.WaitFor(ctx, uid)
}

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

func (s *FSMInjector) WaitForKnownLatest(ctx context.Context) error {
	ctx, cancel := getWaitCtx(ctx)
	defer cancel()

	uid := s.cache.raft.LastIndex()
	return s.cache.fsm.WaitFor(ctx, uid)
}

func (s *FSMInjector) Store(ctx context.Context, bts []byte) (uint64, error) {
	return s.cache.store(ctx, "", bts)
}

func getWaitCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < maxWait {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultWait)
}
