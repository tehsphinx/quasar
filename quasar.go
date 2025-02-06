// Package quasar implements a raft based distributed cache.
package quasar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
	"google.golang.org/protobuf/proto"
)

const (
	applyTimout         = 5 * time.Second
	noLeaderTimeout     = 8 * time.Second
	observationChanSize = 5
)

// ErrNoLeader defines an error returned if there is no leader.
var ErrNoLeader = errors.New("cluster does not have a leader")

// NewCache instantiates a new Cache. In contrast to NewKVCache (which holds []byte) this is meant
// for use with custom types. The FSM implementation should hold all the data that is supposed to be synced
// by the cache.
// An example implementation can be seen in ./examples/generic/exampleFSM/example.go.
func NewCache(ctx context.Context, fsm FSM, opts ...Option) (*Cache, error) {
	cache, err := newCache(ctx, wrapFSM(fsm), opts...)
	if err != nil {
		return nil, err
	}

	fsm.Inject(&FSMInjector{cache: cache})

	return cache, err
}

func newCache(ctx context.Context, fsm *fsmWrapper, opts ...Option) (*Cache, error) {
	ctx, closeCache := context.WithCancel(ctx)
	cfg := getOptions(opts)

	c := &Cache{
		cfg:      cfg,
		ctx:      ctx,
		name:     cfg.cacheName,
		localID:  cfg.localID,
		fsm:      fsm,
		pStore:   cfg.pStore,
		suffrage: cfg.suffrage,
		close:    closeCache,
	}

	transport, err := getTransport(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c.transport = transport

	discovery := newDiscoveryInjector(c)
	c.discovery = discovery

	logStore := wrapStore(raft.NewInmemStore(), fsm)

	rft, err := getRaft(cfg, fsm, logStore, transport, discovery)
	if err != nil {
		return nil, err
	}
	c.raft = rft
	discovery.regObservation(ctx, rft)

	if cfg.discovery != nil {
		cfg.discovery.Inject(discovery)
		if e := cfg.discovery.Run(ctx); e != nil {
			return nil, e
		}
	}

	go c.consume(ctx, transport.CacheConsumer())
	return c, nil
}

// Cache implements the quasar cache. The cache is built on the raft consensus protocol but allows
// any participant to suggest changes to the data. When reading data it provides different guarantee
// levels for getting up-to-date data:
//   - no consistency guarantee at all. Read is done locally without any checks.
//   - local write/read consistency by waiting for a UID to be applied that was written by the local server.
//   - wait for latest locally known UID to be applied before reading. Doesn't catch the case if local server
//     is disconnected from master.
//   - ask master for latest known UID and wait for that to be applied locally before reading.
//
// Since there are different levels of waiting involved, a timeout context should be applied to read/wait functions.
// If a read/wait function returns a context.DeadlineExceeded error, most likely the cluster is in a bad state
// trying to repair itself.
//
//nolint:govet // Usually initialized once. Preferring readability to struct optimization here.
type Cache struct {
	name    string
	localID string
	cfg     options

	fsm    *fsmWrapper
	pStore stores.PersistentStorage

	raft      *raft.Raft
	transport transports.Transport
	discovery *DiscoveryInjector
	suffrage  raft.ServerSuffrage

	ctx   context.Context
	close context.CancelFunc
}

func (s *Cache) serverInfo() raft.Server {
	return raft.Server{
		ID:       raft.ServerID(s.localID),
		Address:  s.transport.LocalAddr(),
		Suffrage: s.suffrage,
	}
}

// GetLeader returns the current leader of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (s *Cache) GetLeader() raft.Server {
	addr, id := s.raft.LeaderWithID()
	return raft.Server{
		ID:       id,
		Address:  addr,
		Suffrage: raft.Voter,
	}
}

// GetServerList returns the servers in the cluster and whether they have votes.
func (s *Cache) GetServerList() ([]raft.Server, error) {
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	configuration := future.Configuration()
	return configuration.Servers, nil
}

func (s *Cache) store(ctx context.Context, key string, data []byte) (uint64, error) {
	cmd := cmdStore(key, data)
	_, uid, err := s.apply(ctx, cmd)
	return uid, err
}

func (s *Cache) masterLastIndex(ctx context.Context) (uint64, error) {
	if s.isLeader() {
		return s.localLastIndex(), nil
	}

	cmd := cmdLatestUID()
	_, uid, err := s.apply(ctx, cmd)
	if err != nil {
		return 0, err
	}

	return uid, nil
}

func (s *Cache) localLastIndex() uint64 {
	return s.raft.LastIndex()
}

func (s *Cache) apply(ctx context.Context, cmd *pb.Command) (*pb.CommandResponse, uint64, error) {
	if s.isLeader() {
		return s.applyLocal(ctx, cmd)
	}
	return s.applyRemote(ctx, cmd)
}

func (s *Cache) applyLocal(ctx context.Context, cmd *pb.Command) (*pb.CommandResponse, uint64, error) {
	cmd, err := s.persist(cmd)
	if err != nil {
		return nil, 0, err
	}

	bts, err := proto.Marshal(cmd)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal command: %w", err)
	}

	fut := s.raft.Apply(bts, getTimeout(ctx, applyTimout))
	if r := fut.Error(); r != nil {
		return nil, 0, fmt.Errorf("applying failed with: %w", r)
	}

	index := fut.Index()
	//nolint:forcetypeassert // we have a bug if this is not applyResponse.
	resp := fut.Response().(applyResponse)
	if r := resp.err; r != nil {
		return nil, 0, fmt.Errorf("apply function returned error: %w", r)
	}

	return resp.resp, index, nil
}

func (s *Cache) persist(cmd *pb.Command) (*pb.Command, error) {
	if s.pStore == nil {
		return cmd, nil
	}

	c := cmd.GetStore()
	if c == nil {
		return cmd, nil
	}

	pData := stores.NewPersistData(c.Data)
	if err := s.pStore.Store(pData); err != nil {
		return cmd, err
	}

	if pData.IsUpdated() {
		c.Data = pData.Data()
		cmd.Cmd = &pb.Command_Store{Store: c}
	}

	return cmd, nil
}

func getTimeout(ctx context.Context, timeout time.Duration) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return timeout
	}

	timeout = time.Until(deadline)
	if timeout <= 0 {
		return 1
	}
	return timeout
}

func (s *Cache) applyRemote(ctx context.Context, command *pb.Command) (*pb.CommandResponse, uint64, error) {
	addr, id, err := s.getLeaderWait(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get leader: %w: %w", err, ErrNoLeader)
	}

	switch cmd := command.GetCmd().(type) {
	case *pb.Command_Store:
		resp, err := s.transport.Store(ctx, id, addr, cmd.Store)
		if err != nil {
			return nil, 0, err
		}
		return respStore(resp), resp.Uid, nil
	case *pb.Command_LatestUid:
		resp, err := s.transport.LatestUID(ctx, id, addr, cmd.LatestUid)
		if err != nil {
			return nil, 0, err
		}
		return respLatestUID(resp), resp.Uid, nil
	}

	return nil, 0, errors.New("leader request type not implemented")
}

func (s *Cache) getLeaderWait(ctx context.Context) (raft.ServerAddress, raft.ServerID, error) {
	ctx, cancel := context.WithTimeout(ctx, noLeaderTimeout)
	defer cancel()

	addr, id := s.raft.LeaderWithID()
	if id == "" {
		if r := s.waitForLeader(ctx); r != nil {
			return "", "", r
		}
		addr, id = s.raft.LeaderWithID()
	}
	return addr, id, nil
}

// isLeader returns if the cache is the current leader. This is not a verified
// check, so it might be that it looses leadership soon or is not able to do leadership
// actions.
func (s *Cache) isLeader() bool {
	return s.raft.State() == raft.Leader
}

// hasLeader returns if the cache has a leader. Or at least if the current node
// thinks there is a leader. The leader might already be unreachable.
func (s *Cache) hasLeader() bool {
	_, id := s.raft.LeaderWithID()
	return id != ""
}

// WaitReady is a helper function to wait for the raft cluster to be ready.
// Specifically it waits for a leader to be elected. The context can be used
// to add a timeout or cancel waiting.
// This can be called at server startup to make sure the cache is ready
// before e.g. starting to try and answer requests that need the cache.
func (s *Cache) WaitReady(ctx context.Context) error {
	if err := s.waitForLeader(ctx); err != nil {
		return err
	}

	if s.isLeader() {
		// If we ourselves became leader, attempt leadership transfer.
		// This way we avoid new cache taking leadership of older instances.
		fut := s.raft.LeadershipTransfer()
		//nolint:staticcheck // empty branch will be filled later
		if r := fut.Error(); r != nil {
			// log error
		}

		if err := s.waitForLeader(ctx); err != nil {
			return err
		}
	}

	// wait for master index to be applied locally
	uid, err := s.masterLastIndex(ctx)
	if err != nil {
		return err
	}
	return s.fsm.WaitFor(ctx, uid)
}

func (s *Cache) waitForLeader(ctx context.Context) error {
	if s.hasLeader() {
		return nil
	}

	chChange := make(chan raft.Observation, 1)
	observer := raft.NewObserver(chChange, true, func(o *raft.Observation) bool {
		if _, ok := o.Data.(raft.LeaderObservation); ok {
			return true
		}
		return false
	})
	s.raft.RegisterObserver(observer)
	defer s.raft.DeregisterObserver(observer)

	if s.hasLeader() {
		// leader elected while starting to observe
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-chChange:
			if s.hasLeader() {
				return nil
			}
		}
	}
}

// Snapshot takes a snapshot of the cache.
func (s *Cache) Snapshot() (*raft.SnapshotMeta, io.ReadCloser, error) {
	fut := s.raft.Snapshot()
	if err := fut.Error(); err != nil {
		return nil, nil, err
	}
	return fut.Open()
}

// Restore restores a given snapshot to the cache.
func (s *Cache) Restore(ctx context.Context, meta *raft.SnapshotMeta, reader io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	return s.raft.Restore(meta, reader, timeout)
}

// ForceSnapshot triggers the underlying raft library to take a snapshot.
// Mostly used for testing purposes.
// TODO: remove and use Snapshot and Restore in tests instead.
func (s *Cache) ForceSnapshot() error {
	future := s.raft.Snapshot()
	return future.Error()
}

func (s *Cache) consume(ctx context.Context, ch <-chan raft.RPC) {
	for {
		select {
		case <-ctx.Done():
			_ = s.shutdown()
			return
		case rpc := <-ch:
			var (
				resp interface{}
				err  error
			)
			switch cmd := rpc.Command.(type) {
			case *pb.Store:
				var uid uint64
				uid, err = s.store(ctx, cmd.Key, cmd.Data)
				resp = &pb.StoreResponse{Uid: uid}
			case *pb.ResetCache:
				err = s.localReset()
				resp = &pb.ResetCacheResponse{}
			case *pb.LatestUid:
				uid := s.localLastIndex()
				resp = &pb.LatestUidResponse{Uid: uid}
			}

			rpc.RespChan <- raft.RPCResponse{
				Response: resp,
				Error:    err,
			}
		}
	}
}

// Shutdown performs a graceful shutdown of the Cache instance.
// Returns an error if there is any error during the shutdown process.
func (s *Cache) Shutdown() error {
	s.close()

	return s.shutdown()
}

func (s *Cache) shutdown() error {
	if trans, ok := s.transport.(io.Closer); ok {
		_ = trans.Close()
	}

	return s.raft.Shutdown().Error()
}

// Reset resets calls Reset the cache on all servers.
func (s *Cache) Reset(ctx context.Context) error {
	servers, err := s.GetServerList()
	if err != nil {
		return err
	}

	var (
		retErrs    []error
		foundLocal bool
	)
	for _, server := range servers {
		if server.ID == raft.ServerID(s.localID) {
			foundLocal = true
			if r := s.localReset(); r != nil {
				retErrs = append(retErrs, r)
			}
			continue
		}

		resp, r := s.transport.ResetCache(ctx, server.ID, server.Address, &pb.ResetCache{})
		if r != nil {
			retErrs = append(retErrs, r)
			continue
		}
		if resp.GetError() != "" {
			retErrs = append(retErrs, errors.New(resp.GetError()))
		}
	}
	if !foundLocal {
		if r := s.localReset(); r != nil {
			retErrs = append(retErrs, r)
		}
	}

	return errors.Join(retErrs...)
}

func (s *Cache) localReset() error {
	if err := s.fsm.applyReset(); err != nil {
		return err
	}

	if s.isLeader() {
		// don't reset raft itself on leader.
		return nil
	}

	s.fsm.applyRaftReset()
	s.raft.Shutdown()

	logStore := wrapStore(raft.NewInmemStore(), s.fsm)

	rft, err := getRaft(s.cfg, s.fsm, logStore, s.transport, s.discovery)
	if err != nil {
		return err
	}
	s.raft = rft
	s.discovery.regObservation(s.ctx, rft)

	return nil
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
