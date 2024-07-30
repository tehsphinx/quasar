package stores

import (
	"encoding/binary"

	"github.com/nats-io/nats.go"
)

// NewStableNatsKV creates a new stable store for the cache based on NATS key/value store.
func NewStableNatsKV(kv nats.KeyValue) *StableNatsKV {
	return &StableNatsKV{
		kv: kv,
	}
}

// StableNatsKV implements a stable store for the cache based on NATS key/value store.
type StableNatsKV struct {
	kv nats.KeyValue
}

// Set sets a value.
func (s *StableNatsKV) Set(key, val []byte) error {
	_, err := s.kv.Put(string(key), val)
	return err
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *StableNatsKV) Get(key []byte) ([]byte, error) {
	entry, err := s.kv.Get(string(key))
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

// SetUint64 sets a uint64 value.
func (s *StableNatsKV) SetUint64(key []byte, val uint64) error {
	const uint64Bytes = 8

	b := make([]byte, uint64Bytes)
	binary.LittleEndian.PutUint64(b, val)

	_, err := s.kv.Put(string(key), b)
	return err
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *StableNatsKV) GetUint64(key []byte) (uint64, error) {
	entry, err := s.kv.Get(string(key))
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(entry.Value()), nil
}
