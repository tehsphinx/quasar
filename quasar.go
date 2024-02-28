package quasar

import (
	"os"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/store"
)

func NewCache([]string) (*Cache, error) {
	bindAddr := ":28224"

	logStore := raft.NewInmemStore()
	snapshotStore := raft.NewDiscardSnapshotStore()
	stableStore := store.NewStableInMemory()
	transport, err := raft.NewTCPTransport(bindAddr, nil, 10, 5*time.Second, os.Stdout)
	if err != nil {
		return nil, err
	}
	fsm := NewFSM()

	rft, err := raft.NewRaft(&raft.Config{}, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	return &Cache{
		raft: rft,
	}, nil
}

type Cache struct {
	raft *raft.Raft
}

func (s *Cache) Set(key string, data []byte) (int, error) {
	panic("not implemented")
}

func (s *Cache) Get(key string) ([]byte, error) {
	panic("not implemented")
}

func (s *Cache) GetLocal(key string, opts ...GetOption) ([]byte, error) {
	panic("not implemented")
}
