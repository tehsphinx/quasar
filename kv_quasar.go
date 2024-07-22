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
	// TODO: expose context as LoadOption
	s.Cache.fsm.WaitFor(context.TODO(), uid)

	return s.fsm.Load(key)
}

func (s *KVCache) LoadLocal(key string, opts ...LoadOption) ([]byte, error) {
	cfg := getLoadOptions(opts)

	if cfg.waitFor != 0 {
		// TODO: expose context as LoadOption
		s.Cache.fsm.WaitFor(context.TODO(), cfg.waitFor)
	}

	return s.fsm.Load(key)
}
