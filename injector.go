package quasar

import (
	"context"
)

type FSMInjector struct {
	cache *Cache
}

func (s *FSMInjector) WaitFor(ctx context.Context, uid uint64) error {
	return s.cache.fsm.WaitFor(ctx, uid)
}

func (s *FSMInjector) WaitForMasterLatest(ctx context.Context) error {
	// TODO: add context to masterLastIndex
	uid, err := s.cache.masterLastIndex()
	if err != nil {
		return err
	}
	return s.cache.fsm.WaitFor(ctx, uid)
}

func (s *FSMInjector) WaitForKnownLatest(ctx context.Context) error {
	uid := s.cache.raft.LastIndex()
	return s.cache.fsm.WaitFor(ctx, uid)
}

func (s *FSMInjector) Store(bts []byte) (uint64, error) {
	return s.cache.store("", bts)
}
