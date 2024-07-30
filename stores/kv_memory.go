package stores

import (
	"sync"
)

// NewInMemKVStore creates a very simple in-memory key/value store based on a map.
func NewInMemKVStore() KVStore {
	return &memKVStore{
		store: map[string][]byte{},
	}
}

var _ KVStore = (*memKVStore)(nil)

type memKVStore struct {
	store map[string][]byte
	m     sync.RWMutex
}

func (s *memKVStore) Store(key string, data []byte) error {
	s.m.Lock()
	defer s.m.Unlock()

	s.store[key] = data
	return nil
}

func (s *memKVStore) Load(key string) ([]byte, error) {
	s.m.Lock()
	defer s.m.Unlock()

	data, ok := s.store[key]
	if !ok {
		return nil, ErrNotFound
	}
	return data, nil
}
