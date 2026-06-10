package stores

import "sync"

// NewStableInMemory creates a new in-memory stable store for the cache.
func NewStableInMemory() *StableInMemory {
	return &StableInMemory{
		store:       map[string][]byte{},
		storeUint64: map[string]uint64{},
	}
}

// StableInMemory implements a in-memory stable store for the cache.
// It is safe for concurrent use: during quorum recovery the old and new raft
// instance briefly share the same store instance (RT-13042 C2).
type StableInMemory struct {
	mu          sync.RWMutex
	store       map[string][]byte
	storeUint64 map[string]uint64
}

// Set sets a value.
func (s *StableInMemory) Set(key, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[string(key)] = val
	return nil
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *StableInMemory) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val := s.store[string(key)]
	return val, nil
}

// SetUint64 sets a uint64 value.
func (s *StableInMemory) SetUint64(key []byte, val uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.storeUint64[string(key)] = val
	return nil
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *StableInMemory) GetUint64(key []byte) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val := s.storeUint64[string(key)]
	return val, nil
}
