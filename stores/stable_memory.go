package stores

// NewStableInMemory creates a new in-memory stable store for the cache.
func NewStableInMemory() *StableInMemory {
	return &StableInMemory{
		store:       map[string][]byte{},
		storeUint64: map[string]uint64{},
	}
}

// StableInMemory implements a in-memory stable store for the cache.
type StableInMemory struct {
	store       map[string][]byte
	storeUint64 map[string]uint64
}

// Set sets a value.
func (s *StableInMemory) Set(key, val []byte) error {
	s.store[string(key)] = val
	return nil
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *StableInMemory) Get(key []byte) ([]byte, error) {
	val := s.store[string(key)]
	return val, nil
}

// SetUint64 sets a uint64 value.
func (s *StableInMemory) SetUint64(key []byte, val uint64) error {
	s.storeUint64[string(key)] = val
	return nil
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *StableInMemory) GetUint64(key []byte) (uint64, error) {
	val := s.storeUint64[string(key)]
	return val, nil
}
