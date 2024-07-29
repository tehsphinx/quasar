package quasar

import (
	"context"
)

func NewKVCache(ctx context.Context, opts ...Option) (*KVCache, error) {
	cfg := getOptions(opts)

	fsm := newKeyValueFSM(cfg.kv)

	cache, err := newCache(ctx, wrapFSM(fsm), opts...)
	if err != nil {
		return nil, err
	}
	fsm.Inject(&FSMInjector{cache: cache})

	return &KVCache{
		Cache: cache,
		fsm:   fsm,
	}, nil
}

type KVCache struct {
	*Cache

	fsm *kvFSM
}

func (s *KVCache) Store(ctx context.Context, key string, data []byte) (uint64, error) {
	return s.store(ctx, key, data)
}

func (s *KVCache) Load(ctx context.Context, key string, opts ...LoadOption) ([]byte, error) {
	cfg := getLoadOptions(opts)

	uid := cfg.waitFor
	if uid == 0 {
		id, err := s.masterLastIndex(ctx)
		if err != nil {
			return nil, err
		}
		uid = id
	}
	if err := s.Cache.fsm.WaitFor(ctx, uid); err != nil {
		return nil, err
	}

	return s.fsm.Load(key)
}

func (s *KVCache) LoadLocal(ctx context.Context, key string, opts ...LoadOption) ([]byte, error) {
	cfg := getLoadOptions(opts)

	if cfg.waitFor != 0 {
		if err := s.Cache.fsm.WaitFor(ctx, cfg.waitFor); err != nil {
			return nil, err
		}
	}

	return s.fsm.Load(key)
}
