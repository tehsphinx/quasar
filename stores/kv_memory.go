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

var _ SnapshotKVStore = (*memKVStore)(nil)

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

// Items returns a copy of all key/value pairs. Implements SnapshotKVStore.
func (s *memKVStore) Items() (map[string][]byte, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	items := make(map[string][]byte, len(s.store))
	for k, v := range s.store {
		data := make([]byte, len(v))
		copy(data, v)
		items[k] = data
	}
	return items, nil
}

// SetItems replaces the entire store content. Implements SnapshotKVStore.
func (s *memKVStore) SetItems(items map[string][]byte) error {
	store := make(map[string][]byte, len(items))
	for k, v := range items {
		data := make([]byte, len(v))
		copy(data, v)
		store[k] = data
	}

	s.m.Lock()
	defer s.m.Unlock()

	s.store = store
	return nil
}

// Clear removes all pairs. Implements SnapshotKVStore.
func (s *memKVStore) Clear() error {
	s.m.Lock()
	defer s.m.Unlock()

	s.store = map[string][]byte{}
	return nil
}
