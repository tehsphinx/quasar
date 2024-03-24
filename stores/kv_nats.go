package stores

import (
	"errors"

	"github.com/nats-io/nats.go"
)

func NewNatsKVStore(kv nats.KeyValue) KVStore {
	return &natsKVStore{
		kv: kv,
	}
}

var _ KVStore = (*natsKVStore)(nil)

type natsKVStore struct {
	kv nats.KeyValue
}

func (s *natsKVStore) Store(key string, data []byte) error {
	_, err := s.kv.Put(key, data)
	return err
}

func (s *natsKVStore) Load(key string) ([]byte, error) {
	entry, err := s.kv.Get(key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return entry.Value(), nil
}
