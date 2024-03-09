package quasar

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb"
	"google.golang.org/protobuf/proto"
)

const applyTimout = 5 * time.Second

func NewCache(opts ...Option) (*Cache, error) {
	cfg := getOptions(opts)

	rft, err := getRaft(cfg)
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

func (s *Cache) Store(key string, data []byte) (uint64, error) {
	cmd := cmdStore(key, data)
	_, index, err := s.apply(cmd)
	return index, err
}

func (s *Cache) Load(key string, opts ...LoadOption) ([]byte, error) {
	cfg := getLoadOptions(opts)
	// TODO: apply load options
	_ = cfg

	cmd := cmdLoad(key)
	resp, _, err := s.apply(cmd)
	return resp.GetLoadValue().GetData(), err
}

func (s *Cache) LoadLocal(key string, opts ...LoadOption) ([]byte, error) {
	cfg := getLoadOptions(opts)
	// TODO: apply load options
	_ = cfg

	cmd := cmdLoad(key)
	resp, _, err := s.applyLocal(cmd)
	return resp.GetLoadValue().GetData(), err

}

func (s *Cache) apply(cmd *pb.Command) (*pb.CommandResponse, uint64, error) {
	if s.isLeader() {
		return s.applyLocal(cmd)
	}
	return s.applyRemote(cmd)
}

func (s *Cache) applyLocal(cmd *pb.Command) (*pb.CommandResponse, uint64, error) {
	bts, err := proto.Marshal(cmd)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal command: %w", err)
	}

	fut := s.raft.Apply(bts, applyTimout)
	if r := fut.Error(); r != nil {
		return nil, 0, fmt.Errorf("applying failed with: %w", err)
	}

	index := fut.Index()
	resp := fut.Response().(applyResponse)
	if r := resp.err; r != nil {
		return nil, 0, fmt.Errorf("apply function returned error: %w", r)
	}

	return resp.resp, index, nil
}

func (s *Cache) applyRemote(cmd *pb.Command) (*pb.CommandResponse, uint64, error) {
	bts, err := proto.Marshal(cmd)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal command: %w", err)
	}

	_ = bts

	panic("not implemented")
}

func (s *Cache) isLeader() bool {
	return s.raft.State() == raft.Leader
}

// WaitReady is a helper function to wait for the raft cluster to be ready.
// Specifically it waits for a leader to be elected. The context can be used
// to add a timeout or cancel waiting.
func (s *Cache) WaitReady(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, id := s.raft.LeaderWithID()
		if id != "" {
			return nil
		}
	}
}
