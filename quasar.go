package quasar

import (
	"github.com/hashicorp/raft"
)

func NewCache(rft *raft.Raft) (*Cache, error) {

	return &Cache{
		raft: rft,
	}, nil
}

type Cache struct {
	raft *raft.Raft
}

func (s *Cache) Store(key string, data []byte) (int, error) {
	panic("not implemented")
}

func (s *Cache) Load(key string) ([]byte, error) {
	panic("not implemented")
}

func (s *Cache) LoadLocal(key string, opts ...LoadOption) ([]byte, error) {
	// cgf := getLoadOptions(opts)
	panic("not implemented")
}
