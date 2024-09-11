package quasar

import (
	"context"

	"github.com/tehsphinx/quasar/stores"
)

// NewKVCache creates a new key-value based cache.
func NewKVCache(ctx context.Context, opts ...Option) (*KVCache, error) {
	cfg := getOptions(opts)
	if cfg.kv == nil {
		cfg.kv = stores.NewInMemKVStore()
	}

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

// KVCache implements a key-value based cache.
type KVCache struct {
	*Cache

	fsm *kvFSM
}

// Store stores the given data associated with the provided key in the cache.
// It returns the UID (unique identifier) of the stored data and any error that occurred during the process.
func (s *KVCache) Store(ctx context.Context, key string, data []byte) (uint64, error) {
	return s.store(ctx, key, data)
}

// Load loads the value associated with the given key from the cache.
// If the WaitForUID value is specified in the LoadOption, it will wait for that UID.
// Otherwise, it will ask the master for the latest known UID to wait for.
// If the context times out, a context error will be returned.
// If the value is not found in the cache, an error will be returned.
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

// LoadLocal loads the value associated with the given key from the local cache.
// It will wait for the specified WaitForUID value if provided.
// If the context times out, a context error will be returned.
// If the value is not found in the cache, an error will be returned.
func (s *KVCache) LoadLocal(ctx context.Context, key string, opts ...LoadOption) ([]byte, error) {
	cfg := getLoadOptions(opts)

	if cfg.waitFor != 0 {
		if err := s.Cache.fsm.WaitFor(ctx, cfg.waitFor); err != nil {
			return nil, err
		}
	}

	return s.fsm.Load(key)
}
