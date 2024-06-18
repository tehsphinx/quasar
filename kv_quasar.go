package quasar

import (
	"context"
)

func NewKVCache(ctx context.Context, opts ...Option) (*KVCache, error) {
	cfg := getOptions(opts)

	fsm := newKeyValueFSM(cfg.kv)

	cache, err := newCache(ctx, fsm, opts...)
	if err != nil {
		return nil, err
	}

	c := &KVCache{
		Cache: cache,
		fsm:   fsm,
	}
	return c, nil
}

type KVCache struct {
	*Cache

	fsm *kvFSM
}

func (s *KVCache) Store(key string, data []byte) (uint64, error) {
	return s.store(key, data)
}

func (s *KVCache) Load(key string, opts ...LoadOption) ([]byte, error) {
	cfg := getLoadOptions(opts)

	uid := cfg.waitFor
	if uid == 0 {
		id, err := s.masterLastIndex()
		if err != nil {
			return nil, err
		}
		uid = id
	}
	s.fsm.WaitFor(uid)

	return s.fsm.Load(key)
}

func (s *KVCache) LoadLocal(key string, opts ...LoadOption) ([]byte, error) {
	cfg := getLoadOptions(opts)

	if cfg.waitFor != 0 {
		s.fsm.WaitFor(cfg.waitFor)
	}

	return s.fsm.Load(key)
}

// func (s *KVCache) observeLeader(ctx context.Context, change chan raft.Observation) {
// 	var obs raft.Observation
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case obs = <-change:
// 		}
//
// 		leaderObs, ok := obs.Data.(raft.LeaderObservation)
// 		if !ok {
// 			continue
// 		}
// 		_ = leaderObs
//
// 		s.transport.SetLeader(leaderObs.LeaderID, leaderObs.LeaderAddr)
// 	}
// }
