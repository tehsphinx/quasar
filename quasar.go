// Package quasar implements a raft based distributed cache.
package quasar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
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

var (
	// ErrNoLeader defines an error returned if there is no leader.
	ErrNoLeader = errors.New("cluster does not have a leader")

	// ErrWaitFor indicates a timeout occurred while waiting for a UID to be applied.
	ErrWaitFor = errors.New("timeout waiting for UID to be applied")
)

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
		logger:   cfg.getLogger(),
	}
	fsm.hasLeader = c.hasLeader

	transport, err := getTransport(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c.transport = transport

	discovery := newDiscoveryInjector(c)
	c.discovery = discovery

	c.newStores()

	rft, err := newRaft(cfg, fsm, c.logStore, c.stableStore, c.snapshotStore, transport)
	if err != nil {
		return nil, err
	}
	c.setRaft(rft)
	discovery.run(ctx, rft)

	if cfg.discovery != nil {
		cfg.discovery.Inject(discovery)
		if e := cfg.discovery.Run(ctx); e != nil {
			return nil, e
		}
	}

	if err := c.bootstrap(ctx, cfg, transport); err != nil {
		return nil, err
	}

	if cfg.recoverQuorumAfter > 0 && cfg.suffrage == raft.Voter {
		go c.runQuorumRecoveryWatch(ctx)
	}

	go c.consume(ctx, transport.CacheConsumer())
	return c, nil
}

// bootstrap drives the initial raft-configuration decision for this cache.
// With discovery and bootstrapWait set, a voter waits briefly for any peer
// ping; if one arrives, bootstrap is skipped and the existing cluster's
// leader pulls this node in via the standard new-peer path (AddVoter from
// discovery's ProcessServer on the leader side, or — when the leader still
// has this node in its configuration from before a restart — heartbeat
// replication onto the fresh raft). Without discovery, bootstrap falls back
// to the explicit WithServers / WithBootstrap configuration.
//
// Nonvoters never bootstrap: they sit waiting for the existing leader to
// AddNonvoter them via discovery.
//
// This is the RT-12775 fix for "voter restart spawns a competing
// single-node raft at term 1, then collides with the surviving cluster on
// term and log freshness". By giving discovery a brief window to learn the
// existing cluster, the restarter can avoid bootstrapping entirely and join
// cleanly via the leader's replication.
func (s *Cache) bootstrap(ctx context.Context, cfg options, transport transports.Transport) error {
	rft := s.raft()
	if rft == nil {
		return errors.New("raft not set (yet)")
	}

	if cfg.discovery != nil && cfg.suffrage == raft.Voter {
		if cfg.bootstrapWait > 0 {
			waitForBootstrapDecision(ctx, s.discovery, cfg.bootstrapWait)
		}
		select {
		case <-s.discovery.PeerInCluster():
			s.logger.Info("bootstrap: peer in established cluster discovered, skipping single-node bootstrap")
			return nil
		default:
		}
		s.logger.Info("bootstrap: no peer in established cluster, bootstrapping single-voter cluster",
			"local-id", s.localID,
			"local-address", transport.LocalAddr(),
			"bootstrap-wait", cfg.bootstrapWait,
		)
		rft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{ID: raft.ServerID(s.localID), Address: transport.LocalAddr()}},
		})
		return nil
	}

	if len(cfg.servers) > 0 {
		rft.BootstrapCluster(raft.Configuration{Servers: cfg.servers})
		return nil
	}

	if cfg.bootstrap {
		rft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{ID: raft.ServerID(s.localID), Address: transport.LocalAddr()}},
		})
	}
	return nil
}

// waitForBootstrapDecision blocks until discovery has observed a peer that
// is part of an established raft cluster, the given duration has elapsed,
// or the context is canceled. Used at startup to give an existing cluster
// a chance to announce itself before this voter would otherwise bootstrap
// a competing single-node universe. Peers that ping without proof of
// cluster membership (e.g. another node still in its own bootstrap-wait)
// don't end the wait, so two cold-starting voters fall through to
// independent bootstraps and converge via the standard discovery path.
func waitForBootstrapDecision(ctx context.Context, discovery *DiscoveryInjector, after time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, after)
	defer cancel()

	select {
	case <-ctx.Done():
	case <-discovery.PeerInCluster():
	}
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

	raftMutex     sync.RWMutex
	ctxRaft       context.Context
	closeRaft     context.CancelFunc
	lastResetID   string
	raftLocker    *raft.Raft
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore

	recoveryMutex sync.Mutex

	transport transports.Transport
	discovery *DiscoveryInjector
	suffrage  raft.ServerSuffrage
	logger    hclog.Logger

	ctx   context.Context
	close context.CancelFunc
}

// newStores (re)creates the in-memory raft stores and stashes them on the
// cache so they can be reused across raft restarts (localReset, quorum
// recovery). Stores are local to the *raft.Raft instance, so a fresh raft
// requires fresh stores; the recovery path is the exception (it reuses the
// existing stores via raft.RecoverCluster).
func (s *Cache) newStores() {
	s.logStore = wrapStore(raft.NewInmemStore(), s.fsm)
	s.stableStore = stores.NewStableInMemory()
	s.snapshotStore = raft.NewInmemSnapshotStore()
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
	addr, id := s.raft().LeaderWithID()
	return raft.Server{
		ID:       id,
		Address:  addr,
		Suffrage: raft.Voter,
	}
}

// GetServerList returns the servers in the cluster and whether they have votes.
func (s *Cache) GetServerList() ([]raft.Server, error) {
	future := s.raft().GetConfiguration()
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
	if s.IsLeader() {
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
	return s.raft().LastIndex()
}

func (s *Cache) apply(ctx context.Context, cmd *pb.Command) (*pb.CommandResponse, uint64, error) {
	if s.IsLeader() {
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

	fut := s.raft().Apply(bts, getTimeout(ctx, applyTimout))
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
	case *pb.Command_RemoveServer:
		resp, err := s.transport.RemoveServer(ctx, id, addr, cmd.RemoveServer)
		if err != nil {
			return nil, 0, err
		}
		return respRemoveServer(resp), 0, nil
	}

	return nil, 0, errors.New("leader request type not implemented")
}

func (s *Cache) getLeaderWait(ctx context.Context) (raft.ServerAddress, raft.ServerID, error) {
	ctx, cancel := context.WithTimeout(ctx, noLeaderTimeout)
	defer cancel()

	addr, id := s.raft().LeaderWithID()
	if id == "" {
		if r := s.waitForLeader(ctx); r != nil {
			return "", "", r
		}
		addr, id = s.raft().LeaderWithID()
	}
	return addr, id, nil
}

// IsLeader returns if the cache is the current leader. This is not a verified
// check, so it might be that it looses leadership soon or is not able to do leadership
// actions.
func (s *Cache) IsLeader() bool {
	return s.raft().State() == raft.Leader
}

// hasLeader returns if the cache has a leader. Or at least if the current node
// thinks there is a leader. The leader might already be unreachable.
func (s *Cache) hasLeader() bool {
	rft := s.raft()
	if rft == nil {
		return false
	}
	_, id := rft.LeaderWithID()
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

	if s.IsLeader() {
		// If we ourselves became leader, attempt leadership transfer.
		// This way we avoid new cache taking leadership of older instances.
		s.logger.Info("WaitReady: local node holds leadership after restart; attempting voluntary leadership transfer",
			"local-id", s.localID,
		)
		fut := s.raft().LeadershipTransfer()
		if r := fut.Error(); r != nil {
			s.logger.Info("WaitReady: voluntary leadership transfer failed; staying leader",
				"local-id", s.localID,
				"error", r.Error(),
			)
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

	rft, ctxRaft := s.getRaftWithCtx()
	rft.RegisterObserver(observer)
	defer rft.DeregisterObserver(observer)

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
		case <-ctxRaft.Done():
			return s.waitForLeader(ctx)
		}
	}
}

// Snapshot takes a snapshot of the cache.
func (s *Cache) Snapshot() (*raft.SnapshotMeta, io.ReadCloser, error) {
	fut := s.raft().Snapshot()
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
	return s.raft().Restore(meta, reader, timeout)
}

// ForceSnapshot triggers the underlying raft library to take a snapshot.
// Mostly used for testing purposes.
// TODO: remove and use Snapshot and Restore in tests instead.
func (s *Cache) ForceSnapshot() error {
	future := s.raft().Snapshot()
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
				err = s.localReset(cmd.Uuid)
				resp = &pb.ResetCacheResponse{Uuid: cmd.Uuid}
			case *pb.RemoveServer:
				err = s.removeServer(cmd.Id)
				resp = &pb.RemoveServerResponse{}
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

// RemoveAndShutdown removes this server from the raft configuration and then shuts it down.
func (s *Cache) RemoveAndShutdown(ctx context.Context) error {
	if err := s.removeSelf(ctx); err != nil {
		s.logger.Error("failed to remove self from raft", "error", err)
	}

	return s.Shutdown()
}

func (s *Cache) shutdown() error {
	if trans, ok := s.transport.(io.Closer); ok {
		_ = trans.Close()
	}

	return s.raft().Shutdown().Error()
}

func (s *Cache) removeSelf(ctx context.Context) error {
	if s.IsLeader() {
		return s.removeServer(s.localID)
	}

	_, _, err := s.applyRemote(ctx, cmdRemoveServer(s.localID))
	return err
}

func (s *Cache) removeServer(id string) error {
	rft := s.raft()
	if rft == nil {
		return errors.New("raft not set (yet)")
	}

	fut := rft.RemoveServer(raft.ServerID(id), 0, 0)
	return fut.Error()
}

// Reset resets calls Reset the cache on all servers.
func (s *Cache) Reset(ctx context.Context) error {
	servers, err := s.GetServerList()
	if err != nil {
		return err
	}

	s.logger.Info("cache reset triggered")

	var (
		retErrs    []error
		foundLocal bool
		resetID    = uuid.NewString()
	)
	for _, server := range servers {
		if server.ID == raft.ServerID(s.localID) {
			foundLocal = true
			if r := s.localReset(resetID); r != nil {
				retErrs = append(retErrs, r)
			}
			continue
		}

		resp, r := s.sendReset(ctx, server)
		if r != nil {
			retErrs = append(retErrs, r)
			continue
		}
		if resp.GetError() != "" {
			retErrs = append(retErrs, errors.New(resp.GetError()))
		}
	}
	if !foundLocal {
		if r := s.localReset(resetID); r != nil {
			retErrs = append(retErrs, r)
		}
	}

	return errors.Join(retErrs...)
}

func (s *Cache) sendReset(ctx context.Context, server raft.Server) (*pb.ResetCacheResponse, error) {
	resp, r := s.transport.ResetCache(ctx, server.ID, server.Address, &pb.ResetCache{
		Uuid: s.getLastResetID(),
	})
	return resp, r
}

func (s *Cache) localReset(resetID string) error {
	if resetID == s.getLastResetID() {
		return nil
	}
	s.setLastResetID(resetID)

	if err := s.fsm.applyReset(); err != nil {
		return err
	}

	if s.IsLeader() {
		// don't reset raft itself on leader.
		s.logger.Info("cache reset: FSM cleared; keeping raft state because this node is leader",
			"local-id", s.localID,
			"reset-id", resetID,
		)
		return nil
	}
	s.logger.Info("cache reset: FSM cleared; reinitializing raft because this node is a follower",
		"local-id", s.localID,
		"reset-id", resetID,
	)

	s.fsm.applyRaftReset()
	s.raft().Shutdown()

	s.newStores()

	rft, err := newRaft(s.cfg, s.fsm, s.logStore, s.stableStore, s.snapshotStore, s.transport)
	if err != nil {
		return err
	}
	s.setRaft(rft)
	s.discovery.run(s.ctx, rft)

	// No bootstrap here: localReset is a follower re-init after a Reset
	// command. The leader still has us in its raft configuration, so its
	// heartbeats will catch the fresh raft up via the standard
	// AppendEntries / InstallSnapshot probe-back path.
	return nil
}

// runQuorumRecoveryWatch is the long-running goroutine that decides when to
// invoke recoverQuorum. It exits when ctx is canceled (cache shutdown).
//
// The trigger is event-driven: a raft LeaderObservation arms (or cancels) a
// one-shot timer set to fire after recoverQuorumAfter of continuous
// leaderlessness. This avoids the ~recoverQuorumAfter/4 polling slop that
// the previous ticker-based implementation added on both sides of the
// threshold (first-tick to register the loss + post-threshold tick to act).
//
// When recoverQuorum rebuilds the raft instance, the per-raft observer
// dies with it; the outer loop re-registers on the new raft via
// getRaftWithCtx and continues.
func (s *Cache) runQuorumRecoveryWatch(ctx context.Context) {
	after := s.cfg.recoverQuorumAfter
	// Alive-window for "is this peer dead per discovery". Decoupled from
	// recoverQuorumAfter: the recovery threshold is a deliberate safety
	// buffer against flaps, but it does not need to also be the cutoff for
	// "we last saw the peer ping this recently". When auto-prune is
	// configured, pruneAfter is the canonical dead-peer threshold; fall
	// back to recoverQuorumAfter only if pruneAfter is zero.
	aliveCutoff := s.cfg.pruneAfter
	if aliveCutoff <= 0 {
		aliveCutoff = after
	}

	for {
		rft, ctxRaft := s.getRaftWithCtx()
		if rft == nil {
			// Cache is still wiring itself up; wait briefly and retry.
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		s.watchQuorumOnRaft(ctx, ctxRaft, rft, after, aliveCutoff)

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// watchQuorumOnRaft runs the event-driven recovery loop against a single
// raft instance. It returns when either ctx (cache shutdown) or ctxRaft
// (this raft was replaced — e.g. by recoverQuorum or localReset) is done,
// so the caller can re-register on the new raft.
func (s *Cache) watchQuorumOnRaft(ctx, ctxRaft context.Context, rft *raft.Raft, after, aliveCutoff time.Duration) {
	chObs := make(chan raft.Observation, observationChanSize)
	observer := raft.NewObserver(chObs, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	rft.RegisterObserver(observer)
	defer rft.DeregisterObserver(observer)

	// fire is buffered so the timer goroutine can deliver without blocking.
	// We drain it explicitly when we cancel; spurious wake-ups are harmless
	// (the shouldRecoverQuorum check is the source of truth).
	fire := make(chan struct{}, 1)
	var timer *time.Timer

	armTimer := func() {
		if timer != nil {
			return
		}
		timer = time.AfterFunc(after, func() {
			select {
			case fire <- struct{}{}:
			default:
			}
		})
	}
	cancelTimer := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		select {
		case <-fire:
		default:
		}
	}
	defer cancelTimer()

	// Seed: react to the current leader state. Initial LeaderObservation
	// events arrive only on changes, so a node started without a leader
	// would otherwise sit unarmed until the next election attempt.
	if !s.hasLeader() {
		armTimer()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ctxRaft.Done():
			return
		case <-chObs:
			if s.hasLeader() {
				cancelTimer()
			} else {
				armTimer()
			}
		case <-fire:
			timer = nil
			if !s.shouldRecoverQuorum(time.Now(), aliveCutoff) {
				// Either a leader came back between fire and now, or we
				// are not the captain / quorum is still reachable. If we
				// still have no leader, re-arm to retry on the next full
				// window — avoids tight-looping when a transient condition
				// (e.g. raft.GetConfiguration error) blocked recovery.
				if !s.hasLeader() {
					armTimer()
				}
				continue
			}
			if err := s.recoverQuorum(); err != nil {
				s.logger.Error("quorum recovery failed", "error", err)
				// Re-arm on the same raft so we wait a full window before
				// trying again. recoverQuorum only swaps raft on success.
				if !s.hasLeader() {
					armTimer()
				}
				continue
			}
			// recoverQuorum shut down the old raft and installed a new
			// one; ctxRaft is now done and we'll exit the select on the
			// next iteration to let the outer loop re-register.
		}
	}
}

// recoverQuorum forces a single-voter raft configuration on this cache,
// keeping only the live nonvoters. Used by the quorum-recovery watcher when
// this voter has been stranded without a leader past the configured
// threshold (and was elected captain by `shouldRecoverQuorum`). Preserves
// the FSM data via raft.RecoverCluster, then rebuilds the raft instance on
// top of the existing stores.
//
// All peer voters (alive or dead) are dropped from the new configuration
// and forgotten in discovery state. Two reasons:
//
//   - hashicorp/raft.RecoverCluster is fundamentally a single-node
//     operation — running it concurrently on two survivors with the same
//     "alive voters" config produces divergent snapshots that fail to
//     converge into one cluster. By becoming a single-voter cluster the
//     captain side-steps that.
//   - Once the captain is leader, any alive peer voter that pings via
//     discovery is treated as new (because forgetServer cleared its
//     bookkeeping) and is re-added through the standard AddVoter path,
//     which rebuilds replication state cleanly.
//
//nolint:funlen // single linear procedure; splitting hurts readability.
func (s *Cache) recoverQuorum() error {
	s.recoveryMutex.Lock()
	defer s.recoveryMutex.Unlock()

	rft := s.raft()
	if rft == nil {
		return errors.New("raft not set (yet)")
	}

	cfgFut := rft.GetConfiguration()
	if err := cfgFut.Error(); err != nil {
		return fmt.Errorf("get configuration: %w", err)
	}

	current := cfgFut.Configuration().Servers
	keep := make([]raft.Server, 0, len(current))
	dropped := make([]raft.Server, 0, len(current))
	selfPresent := false
	for _, srv := range current {
		if string(srv.ID) == s.localID {
			// Always keep self, force voter status.
			srv.Suffrage = raft.Voter
			keep = append(keep, srv)
			selfPresent = true
			continue
		}
		if srv.Suffrage == raft.Voter {
			dropped = append(dropped, srv)
			continue
		}
		// Keep nonvoters in the new configuration. They'll catch up via
		// normal replication once we are leader again.
		keep = append(keep, srv)
	}

	if len(dropped) == 0 {
		// No peer voters to drop — there's nothing for recovery to fix
		// here. The watcher should not normally call us in this state
		// (shouldRecoverQuorum requires aliveVoters < quorum), but be
		// defensive.
		return nil
	}

	// Make sure self is in the new configuration even if the old one had us
	// missing for some reason.
	if !selfPresent {
		keep = append(keep, raft.Server{
			ID:       raft.ServerID(s.localID),
			Address:  s.transport.LocalAddr(),
			Suffrage: raft.Voter,
		})
	}

	s.logger.Warn("quorum lost — forcing raft recovery",
		"keep", keep, "dropped", dropped)

	// IMPORTANT: do not call .Error() on the shutdown future here.
	// hashicorp/raft's shutdownFuture.Error() additionally invokes Close()
	// on the transport (raft@v1.7.3 future.go), which the InmemTransport
	// implements as DisconnectAll() and the TCPTransport implements as
	// permanently shutting the listener down. Either would brick the
	// transport for the new raft instance we're about to spin up on top of
	// the same transport. raft.Shutdown() itself is synchronous enough —
	// it sets the shutdown flag and closes the channel that all internal
	// goroutines select on. Cache.localReset uses the same pattern.
	rft.Shutdown()

	conf := raftConfig(s.cfg)
	if err := raft.RecoverCluster(conf, s.fsm, s.logStore, s.stableStore, s.snapshotStore,
		s.transport, raft.Configuration{Servers: keep}); err != nil {
		return fmt.Errorf("recover cluster: %w", err)
	}

	rftNew, err := newRaft(s.cfg, s.fsm, s.logStore, s.stableStore, s.snapshotStore, s.transport)
	if err != nil {
		return fmt.Errorf("restart raft: %w", err)
	}

	// Forget every peer voter we dropped so that, when they ping discovery
	// again (whether currently alive or after a restart), ProcessServer
	// treats them as brand-new and the standard AddVoter rejoin path runs.
	for _, srv := range dropped {
		s.discovery.forgetServer(srv.ID)
	}

	s.setRaft(rftNew)
	s.discovery.run(s.ctx, rftNew)
	return nil
}

// shouldRecoverQuorum returns true when this voter is stranded and the
// recovery procedure should run. The leaderless-duration threshold is the
// caller's responsibility (the recovery watcher arms a time.Timer for that);
// shouldRecoverQuorum only re-confirms there is still no leader and that
// this node is the elected recovery captain among the alive voters.
//
// aliveCutoff is the discovery-side "this peer is still alive" window. It
// is independent of the recovery threshold so the alive-window can match
// the auto-prune cutoff (the canonical dead-peer signal) rather than being
// forced to equal the safety-buffer threshold.
func (s *Cache) shouldRecoverQuorum(now time.Time, aliveCutoff time.Duration) bool {
	if s.suffrage != raft.Voter {
		return false
	}
	if s.IsLeader() || s.hasLeader() {
		return false
	}

	// No leader, threshold already elapsed. Two more checks before we tear
	// things down:
	//
	//  1. Count voters in the current config and how many are alive per
	//     discovery (self counts as alive — we are running this code). If
	//     enough are alive to form quorum, raft should be able to elect
	//     on its own and recovery would just cause churn.
	//  2. Pick a deterministic recovery captain — the alive voter with
	//     the highest reported raft.LastIndex(), tie-broken by lowest ID
	//     — so that with multiple stranded survivors only one runs
	//     raft.RecoverCluster. Concurrent recovery on multiple peers
	//     produces divergent snapshots that fail to converge. The
	//     LastIndex preference (RT-12862) prevents a lagging captain
	//     from defining the new cluster snapshot below a survivor's
	//     already-replicated log tail, which would otherwise trap the
	//     survivor in a snapshot-regression catchup loop. The other
	//     survivors stay in the captain's new config and get pulled to
	//     the recovered state via InstallSnapshot once the captain wins
	//     its election.
	rft := s.raft()
	if rft == nil {
		return false
	}
	cfgFut := rft.GetConfiguration()
	if err := cfgFut.Error(); err != nil {
		return false
	}

	var (
		configVoters     int
		aliveVoters      int
		selfInConfig     bool
		captain          = s.localID
		captainLastIndex = rft.LastIndex()
		pruneBefore      = now.Add(-aliveCutoff)
	)
	for _, srv := range cfgFut.Configuration().Servers {
		if srv.Suffrage != raft.Voter {
			continue
		}
		configVoters++
		var (
			alive     bool
			lastIndex uint64
		)
		if string(srv.ID) == s.localID {
			selfInConfig = true
			alive = true
			lastIndex = captainLastIndex
		} else {
			lastIndex, alive = s.discovery.isAliveSince(srv.ID, pruneBefore)
		}
		if !alive {
			continue
		}
		aliveVoters++
		if string(srv.ID) == s.localID {
			continue
		}
		// Prefer the alive voter with the highest reported LastIndex; on
		// ties, prefer the lowest ID. Initial captain is self, so we
		// only replace when a peer strictly wins on (lastIndex, -ID).
		if captainLastIndex < lastIndex  ||
			(lastIndex == captainLastIndex && string(srv.ID) < captain) {
			captain = string(srv.ID)
			captainLastIndex = lastIndex
		}
	}
	if !selfInConfig || configVoters == 0 {
		// Either we are not in the configuration as a voter (degenerate),
		// or there is nothing to recover. Skip.
		return false
	}
	quorum := configVoters/2 + 1
	if quorum <= aliveVoters {
		return false
	}
	return captain == s.localID
}

func (s *Cache) raft() *raft.Raft {
	s.raftMutex.RLock()
	defer s.raftMutex.RUnlock()

	return s.raftLocker
}

func (s *Cache) setRaft(rft *raft.Raft) {
	s.raftMutex.Lock()
	defer s.raftMutex.Unlock()

	if s.closeRaft != nil {
		s.closeRaft()
	}
	s.ctxRaft, s.closeRaft = context.WithCancel(s.ctx)
	s.raftLocker = rft
}

func (s *Cache) getLastResetID() string {
	s.raftMutex.RLock()
	defer s.raftMutex.RUnlock()

	return s.lastResetID
}

func (s *Cache) setLastResetID(resetID string) {
	s.raftMutex.Lock()
	defer s.raftMutex.Unlock()

	s.lastResetID = resetID
}

func (s *Cache) getRaftWithCtx() (*raft.Raft, context.Context) {
	s.raftMutex.RLock()
	defer s.raftMutex.RUnlock()

	return s.raftLocker, s.ctxRaft
}
