package stores

import (
	"sync"
)

func NewInMemKVStore() KVStore {
	return &memKVStore{
		store: map[string][]byte{},
	}
}

var _ KVStore = (*memKVStore)(nil)

type memKVStore struct {
	m     sync.RWMutex
	store map[string][]byte
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
