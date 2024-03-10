package quasar

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/transports"
	"google.golang.org/protobuf/proto"
)

const applyTimout = 5 * time.Second

func NewCache(ctx context.Context, opts ...Option) (*Cache, error) {
	cfg := getOptions(opts)

	transport, err := getTransport(cfg)
	if err != nil {
		return nil, err
	}

	fsm := NewFSM()

	rft, err := getRaft(cfg, fsm, transport)
	if err != nil {
		return nil, err
	}

	chLeaderChange := make(chan raft.Observation, 3)
	observer := raft.NewObserver(chLeaderChange, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	rft.RegisterObserver(observer)

	c := &Cache{
		fsm:       fsm,
		raft:      rft,
		transport: transport,
	}
	go c.consume(transport.CacheConsumer())
	// go c.observeLeader(ctx, chLeaderChange)
	return c, nil
}

type Cache struct {
	fsm FSM

	raft      *raft.Raft
	transport transports.Transport
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
	resp, err := s.fsm.Apply(cmd)
	if err != nil {
		return nil, err
	}
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
		return nil, 0, fmt.Errorf("applying failed with: %w", r)
	}

	index := fut.Index()
	resp := fut.Response().(applyResponse)
	if r := resp.err; r != nil {
		return nil, 0, fmt.Errorf("apply function returned error: %w", r)
	}

	return resp.resp, index, nil
}

func (s *Cache) applyRemote(command *pb.Command) (*pb.CommandResponse, uint64, error) {
	addr, id := s.raft.LeaderWithID()
	if id == "" {
		return nil, 0, ErrNoLeader
	}

	switch cmd := command.GetCmd().(type) {
	case *pb.Command_StoreValue:
		resp, err := s.transport.Store(id, addr, cmd.StoreValue)
		return respStore(resp), resp.Uid, err
	case *pb.Command_LoadValue:
		resp, err := s.transport.Load(id, addr, cmd.LoadValue)
		return respLoad(resp), resp.Uid, err
	}

	return nil, 0, errors.New("leader request type not implemented")
}

func (s *Cache) isLeader() bool {
	return s.raft.State() == raft.Leader
}

// WaitReady is a helper function to wait for the raft cluster to be ready.
// Specifically it waits for a leader to be elected. The context can be used
// to add a timeout or cancel waiting.
// TODO: optimize e.g. based on observeLeader. Only recheck if there is a change.
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

func (s *Cache) consume(ch <-chan raft.RPC) {
	for rpc := range ch {
		var (
			resp interface{}
			err  error
		)
		switch cmd := rpc.Command.(type) {
		case *pb.StoreValue:
			var uid uint64
			uid, err = s.Store(cmd.Key, cmd.Data)
			resp = &pb.StoreValueResponse{Uid: uid}
		case *pb.LoadValue:
			var data []byte
			data, err = s.Load(cmd.Key)
			resp = &pb.LoadValueResponse{Data: data}
		}
		fmt.Printf("%+v\n", rpc)
		rpc.RespChan <- raft.RPCResponse{
			Response: resp,
			Error:    err,
		}
	}
}

// func (s *Cache) observeLeader(ctx context.Context, change chan raft.Observation) {
// 	var obs raft.Observation
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case obs = <-change:
// 		}
//
// 		leaderObs, ok := obs.Data.(raft.LeaderObservation)
// 		if !ok {
// 			continue
// 		}
// 		_ = leaderObs
//
// 		s.transport.SetLeader(leaderObs.LeaderID, leaderObs.LeaderAddr)
// 	}
// }
