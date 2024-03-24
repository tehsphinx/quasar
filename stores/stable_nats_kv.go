package stores

import (
	"encoding/binary"

	"github.com/nats-io/nats.go"
)

func NewStableNatsKV(kv nats.KeyValue) *StableNatsKV {
	return &StableNatsKV{
		kv: kv,
	}
}

type StableNatsKV struct {
	kv nats.KeyValue
}

func (s *StableNatsKV) Set(key []byte, val []byte) error {
	_, err := s.kv.Put(string(key), val)
	return err
}

func (s *StableNatsKV) Get(key []byte) ([]byte, error) {
	entry, err := s.kv.Get(string(key))
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

func (s *StableNatsKV) SetUint64(key []byte, val uint64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)

	_, err := s.kv.Put(string(key), b)
	return err
}

func (s *StableNatsKV) GetUint64(key []byte) (uint64, error) {
	entry, err := s.kv.Get(string(key))
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(entry.Value()), nil
}
