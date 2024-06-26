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

var ErrNoLeader = errors.New("cluster does not have a leader")

func NewCache(ctx context.Context, fsm FSM, opts ...Option) (*Cache, error) {
	cache, err := newCache(ctx, wrapFSM(fsm), opts...)
	if err != nil {
		return nil, err
	}

	fsm.Inject(&FSMInjector{cache: cache})

	return cache, err
}

func newCache(ctx context.Context, fsm cacheFSM, opts ...Option) (*Cache, error) {
	cfg := getOptions(opts)

	transport, err := getTransport(cfg)
	if err != nil {
		return nil, err
	}

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
		localID: cfg.localID,
		// kv:        cfg.kv,
		fsm:       fsm,
		raft:      rft,
		transport: transport,
	}

	go c.consume(transport.CacheConsumer())
	// go c.observeLeader(ctx, chLeaderChange)
	return c, nil
}

type Cache struct {
	localID string

	fsm cacheFSM
	// kv  stores.KVStore

	raft      *raft.Raft
	transport transports.Transport
}

func (s *Cache) store(key string, data []byte) (uint64, error) {
	cmd := cmdStore(key, data)
	_, uid, err := s.apply(cmd)
	return uid, err
}

func (s *Cache) masterLastIndex() (uint64, error) {
	if s.isLeader() {
		return s.localLastIndex(), nil
	}

	cmd := cmdLatestUID()
	_, uid, err := s.apply(cmd)
	if err != nil {
		return 0, err
	}

	return uid, nil
}

func (s *Cache) localLastIndex() uint64 {
	return s.raft.LastIndex()
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

	// TODO: apply to persistent storage if one is set

	return resp.resp, index, nil
}

func (s *Cache) applyRemote(command *pb.Command) (*pb.CommandResponse, uint64, error) {
	addr, id := s.raft.LeaderWithID()
	if id == "" {
		return nil, 0, ErrNoLeader
	}

	switch cmd := command.GetCmd().(type) {
	case *pb.Command_Store:
		resp, err := s.transport.Store(id, addr, cmd.Store)
		return respStore(resp), resp.Uid, err
	case *pb.Command_LatestUid:
		resp, err := s.transport.LatestUID(id, addr, cmd.LatestUid)
		return respLatestUID(resp), resp.Uid, err
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
		case *pb.Store:
			var uid uint64
			uid, err = s.store(cmd.Key, cmd.Data)
			resp = &pb.StoreResponse{Uid: uid}
		case *pb.LatestUid:
			uid := s.localLastIndex()
			resp = &pb.LatestUidResponse{Uid: uid}
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
