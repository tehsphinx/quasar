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
	applyTimeout           = 5 * time.Second
	defaultNoLeaderTimeout = 8 * time.Second
	// defaultBootstrapWait is how long a discovery-based voter waits at
	// startup for an existing cluster to announce itself before
	// bootstrapping a single-node cluster (see WithBootstrapWait). Sized to
	// cover the discovery ping cadence (the NATS discovery pings every
	// 2-5s) so a restarting voter reliably observes an established peer and
	// rejoins as a follower instead of racing into a competing single-node
	// universe. The wait aborts as soon as a peer is seen, so only a genuine
	// cold start (no cluster out there yet) pays the full duration.
	defaultBootstrapWait = 5 * time.Second
	observationChanSize  = 5
	// quorumProbeTimeout bounds the per-peer LatestUID probe used by
	// getLeaderWait to detect quorum loss without waiting for the
	// discovery alive-window to expire. Sized for a healthy LAN: long
	// enough to absorb a transient blip, short enough that the added
	// latency on a leaderless-but-healthy request (during a normal
	// election) is negligible compared to noLeaderTimeout.
	quorumProbeTimeout = 250 * time.Millisecond
	// quorumProbeCacheTTL caps how long a probe result is reused before
	// the next leaderless request triggers a fresh probe. Event-based
	// invalidation (PeerObservation / LeaderObservation) is the primary
	// mechanism; the TTL is the safety net for the case where a peer
	// recovers without an accompanying raft observation.
	quorumProbeCacheTTL = 4 * time.Second
)

var (
	// ErrNoLeader defines an error returned if there is no leader.
	ErrNoLeader = errors.New("cluster does not have a leader")

	// ErrWaitFor indicates a timeout occurred while waiting for a UID to be applied.
	ErrWaitFor = errors.New("timeout waiting for UID to be applied")

	// ErrRetrying indicates that a Store command was successfully
	// published to the persisted-FIFO transport but the leader's reply
	// did not arrive within the caller's context deadline. JetStream-level
	// redelivery (MaxDeliver × AckWait) continues; the next leader will
	// eventually apply the command. Returned only when the caller asked
	// for at-least-once semantics with WithRetry().
	ErrRetrying = errors.New("failed to apply: retrying in background")
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

	if transport.SupportsPersisted() {
		go c.runPersistedConsumer(ctx)
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
		s.setInstanceID(uuid.NewString())
		rft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{ID: raft.ServerID(s.localID), Address: transport.LocalAddr()}},
		})
		return nil
	}

	if len(cfg.servers) > 0 {
		s.setInstanceID(uuid.NewString())
		rft.BootstrapCluster(raft.Configuration{Servers: cfg.servers})
		return nil
	}

	if cfg.bootstrap {
		s.setInstanceID(uuid.NewString())
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
	instanceID    string
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

	// quorumProbe* cache the last quorumProbeReachable result so a burst
	// of leaderless requests doesn't re-fan-out one LatestUID RPC per
	// voter per call. quorumProbeAt zero means "no cached value";
	// invalidateQuorumProbeCache resets it on raft Peer/Leader changes.
	quorumProbeM       sync.Mutex
	quorumProbeAt      time.Time
	quorumProbeReached bool

	ctx   context.Context
	close context.CancelFunc
}

// newStores (re)creates the in-memory raft stores and stashes them on the
// cache so they can be reused across raft restarts (reinitRaftAdoptingInstance,
// quorum recovery). Stores are local to the *raft.Raft instance, so a fresh raft
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

func (s *Cache) store(ctx context.Context, key string, data []byte, opts ...StoreOption) (uint64, error) {
	cmd := cmdStore(key, data)
	_, uid, err := s.apply(ctx, cmd, resolveStoreOpts(opts))
	return uid, err
}

func (s *Cache) masterLastIndex(ctx context.Context) (uint64, error) {
	if s.IsLeader() {
		return s.localLastIndex(), nil
	}

	cmd := cmdLatestUID()
	_, uid, err := s.apply(ctx, cmd, storeOpts{})
	if err != nil {
		return 0, err
	}

	return uid, nil
}

func (s *Cache) localLastIndex() uint64 {
	return s.raft().LastIndex()
}

// apply is the central dispatcher for cache commands. When the configured
// transport supports persisted-FIFO mode, Store commands are routed through
// the queue (every write, leader's own included, so the queue's single
// in-flight item provides cluster-wide FIFO ordering). Non-Store commands
// and transports without persisted mode keep the existing leader-local /
// leader-RPC split.
func (s *Cache) apply(ctx context.Context, cmd *pb.Command, opts storeOpts) (*pb.CommandResponse, uint64, error) {
	if storeCmd := cmd.GetStore(); storeCmd != nil && s.transport.SupportsPersisted() {
		return s.routePersisted(ctx, storeCmd, opts)
	}

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

	fut := s.raft().Apply(bts, getTimeout(ctx, applyTimeout))
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
	resp, uid, err := s.applyRemoteCmd(ctx, command)
	return resp, uid, reattachNoLeader(err)
}

func (s *Cache) applyRemoteCmd(ctx context.Context, command *pb.Command) (*pb.CommandResponse, uint64, error) {
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
	case *pb.Command_ResetCache:
		resp, err := s.transport.ResetCache(ctx, id, addr, cmd.ResetCache)
		if err != nil {
			return nil, 0, err
		}
		return respResetCache(resp), 0, nil
	}

	return nil, 0, errors.New("leader request type not implemented")
}

func (s *Cache) getLeaderWait(ctx context.Context) (raft.ServerAddress, raft.ServerID, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.noLeaderTimeout)
	defer cancel()

	addr, id := s.raft().LeaderWithID()
	if id == "" {
		// Fail fast when quorum can't currently form. Waiting up to
		// noLeaderTimeout would just delay an inevitable timeout — no
		// election can complete without quorum, and recovery (if
		// configured) runs on its own clock. An active probe is used
		// instead of the discovery-cached lastSeen view so quorum loss
		// is detected as soon as peers stop responding rather than
		// only after the pruneAfter / aliveCutoff window has elapsed.
		if !s.quorumProbeReachable(ctx, quorumProbeTimeout) {
			return "", "", ErrNoLeader
		}
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
				if cmd.Hard {
					err = s.localHardReset(cmd.Uuid)
				} else {
					err = s.reset(ctx)
				}
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

// Reset clears the cache content on every node by proposing a reset command
// through the raft log. Like any committed entry it replicates to all current
// members, is captured by post-reset snapshots, and replays on restart /
// catch-up — so a reset can never be regressed by a snapshot restore or by
// quorum recovery (this is the RT-12994 fix for a cleared device re-appearing
// after a snapshot restore). Issued against a healthy cluster: it requires a
// leader+quorum to commit; with no leader it returns ErrNoLeader and the
// operator should recover quorum first.
func (s *Cache) Reset(ctx context.Context) error {
	s.logger.Info("cache reset triggered")
	return s.reset(ctx)
}

// reset proposes the reset command through the normal apply path: applied
// locally when this node is the leader, otherwise forwarded to the leader
// (consume's ResetCache case calls back here on the leader side). The FSM
// (gatexcache) decides what its Reset() actually clears.
func (s *Cache) reset(ctx context.Context) error {
	_, _, err := s.apply(ctx, cmdReset(), storeOpts{})
	return err
}

// HardReset forcibly resets the cache AND the underlying raft on every node by
// sending an out-of-band ResetCache RPC (hard=true) to each server in the
// configuration — it does NOT go through the raft log. A follower that receives
// it tears down and rebuilds its raft on fresh stores, discarding the
// log/term/vote/snapshot that made it ahead; the leader only clears its FSM and
// catches the wiped followers back up via heartbeats. This is the recovery path
// for a wedged cluster where the log-based Reset cannot commit because a node is
// ahead of a freshly (re)started leader and no leader can be elected (RT-13034).
//
// A HardReset must be followed by a normal Reset once the cluster is stable
// again, to re-anchor the reset in the new log history / snapshots — the hard
// reset clears each FSM out-of-band, which on its own does not carry the
// snapshot-regression guarantee the log-based reset provides (RT-12994).
func (s *Cache) HardReset(ctx context.Context) error {
	servers, err := s.GetServerList()
	if err != nil {
		return err
	}

	s.logger.Info("hard cache reset triggered")

	resetID := uuid.NewString()

	var retErrs []error
	for _, server := range servers {
		if server.ID == raft.ServerID(s.localID) {
			continue
		}
		if r := s.sendHardReset(ctx, server, resetID); r != nil {
			retErrs = append(retErrs, r)
		}
	}
	if r := s.localInitiatorReset(resetID); r != nil {
		retErrs = append(retErrs, r)
	}

	return errors.Join(retErrs...)
}

// localInitiatorReset clears the FSM on the node that initiates a HardReset but
// always KEEPS its raft, regardless of whether it currently holds leadership.
// The initiator is the node that must lead the cluster back: every remote peer
// is wiped to fresh stores, so if the initiator wiped its own raft too no node
// would retain a configuration and there would be nobody to bootstrap recovery
// — the cluster would deadlock with empty configs (RT-13034). Keeping the
// initiator's raft lets it (re)win the election against the now-reset peers and
// catch them up via the standard heartbeat / AppendEntries path.
func (s *Cache) localInitiatorReset(resetID string) error {
	if resetID != "" && resetID == s.getLastResetID() {
		return nil
	}
	s.setLastResetID(resetID)

	s.logger.Info("hard reset: FSM cleared; keeping raft state on the reset initiator",
		"local-id", s.localID, "reset-id", resetID)
	return s.fsm.applyReset()
}

// sendHardReset sends an out-of-band hard ResetCache RPC to a single remote
// server. The receiver runs localHardReset on its side (see consume).
func (s *Cache) sendHardReset(ctx context.Context, server raft.Server, resetID string) error {
	resp, err := s.transport.ResetCache(ctx, server.ID, server.Address, &pb.ResetCache{
		Uuid: resetID,
		Hard: true,
	})
	if err != nil {
		return err
	}
	if resp.GetError() != "" {
		return errors.New(resp.GetError())
	}
	return nil
}

// localHardReset applies a hard reset to this node. resetID deduplicates resets
// already applied here (an out-of-band RPC racing the addServer resend), so a
// node is never wiped twice for the same reset. On the leader it only clears the
// FSM (keeping raft so its heartbeats can catch up the wiped followers); on a
// follower it reinitializes raft on fresh stores via reinitRaftAdoptingInstance,
// discarding the log/term/vote/snapshot that made it ahead.
func (s *Cache) localHardReset(resetID string) error {
	if resetID != "" && resetID == s.getLastResetID() {
		return nil
	}
	s.setLastResetID(resetID)

	if s.IsLeader() {
		s.logger.Info("hard reset: FSM cleared; keeping raft state because this node is leader",
			"local-id", s.localID, "reset-id", resetID)
		return s.fsm.applyReset()
	}

	s.logger.Info("hard reset: reinitializing raft because this node is a follower",
		"local-id", s.localID, "reset-id", resetID)
	return s.reinitRaftAdoptingInstance("")
}

// reinitRaftAdoptingInstance tears down the local raft and rebuilds it on
// fresh stores while adopting the given instance ID. This is the
// fork-history recovery primitive (RT-12862): adoptLeaderInstance calls it
// when this node observed a forked-history ping from the current raft leader.
// It first drops stale FSM content the leader's snapshot might not overwrite,
// then discards the forked in-memory log/snapshot that would otherwise trap
// this node in a snapshot-regression catchup loop on top of the leader's
// (possibly older) snapshot. After reinit the leader still has us in its raft
// configuration, so its heartbeats catch the fresh raft up via the standard
// AppendEntries / InstallSnapshot probe-back path — no bootstrap here.
func (s *Cache) reinitRaftAdoptingInstance(adoptInstanceID string) error {
	if err := s.fsm.applyReset(); err != nil {
		return err
	}

	s.fsm.applyRaftReset()
	// Unbind the transport's heartbeat fast-path BEFORE shutting the old raft
	// down. raft's processHeartbeat returns on a closed shutdownCh WITHOUT
	// responding, so a beat landing on a torn-down instance is silently
	// dropped and the leader loops on "failed to heartbeat" while this node
	// never converges (RT-13010). Unbinding first shrinks the window in which
	// a beat can still be handed to the dying raft (RT-13042): newRaft below
	// rebinds the fast-path to the live instance, and until then (or
	// indefinitely, if newRaft fails) the transport routes beats to the live
	// consumer instead of dropping them.
	s.transport.SetHeartbeatHandler(nil)
	s.raft().Shutdown()

	s.newStores()
	if adoptInstanceID != "" {
		s.setInstanceID(adoptInstanceID)
	}

	rft, err := newRaft(s.cfg, s.fsm, s.logStore, s.stableStore, s.snapshotStore, s.transport)
	if err != nil {
		return err
	}
	s.setRaft(rft)
	s.discovery.run(s.ctx, rft)
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
	// "we last saw the peer ping this recently". Same resolution as
	// getLeaderWait so the shedder and the recovery captain agree.
	aliveCutoff := s.aliveCutoff()

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
// (this raft was replaced — e.g. by recoverQuorum or reinitRaftAdoptingInstance) is done,
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

	// See reinitRaftAdoptingInstance: unbind the heartbeat fast-path BEFORE
	// shutting the old raft down so a beat is never silently dropped by a
	// torn-down instance (RT-13010, RT-13042). newRaft below rebinds it to
	// the live raft.
	s.transport.SetHeartbeatHandler(nil)
	// IMPORTANT: do not call .Error() on the shutdown future here.
	// hashicorp/raft's shutdownFuture.Error() additionally invokes Close()
	// on the transport (raft@v1.7.3 future.go), which the InmemTransport
	// implements as DisconnectAll() and the TCPTransport implements as
	// permanently shutting the listener down. Either would brick the
	// transport for the new raft instance we're about to spin up on top of
	// the same transport. raft.Shutdown() itself is synchronous enough —
	// it sets the shutdown flag and closes the channel that all internal
	// goroutines select on. reinitRaftAdoptingInstance uses the same pattern.
	rft.Shutdown()

	// Mint a fresh instance ID — recoverQuorum forks the consensus
	// history line on purpose (RT-12862). Surviving peers compare the
	// captain's new ID against their own on the next discovery ping;
	// any mismatch triggers their reinitRaftAdoptingInstance, which wipes the stale
	// in-memory log/snapshot state that would otherwise trap them in a
	// snapshot-regression catchup loop on top of the captain's
	// (possibly older) recovered snapshot.
	s.setInstanceID(uuid.NewString())

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

// voterScan summarizes the current raft voter configuration measured
// against discovery's alive picture. configVoters counts every voter in
// the configuration; aliveVoters lists only voters confirmed alive
// within the caller's aliveCutoff window (self is always included).
// selfIsVoter records whether the local node is a voter in the
// configuration.
type voterScan struct {
	configVoters int
	aliveVoters  []raft.ServerID
	selfIsVoter  bool
}

// scanVoters walks the current raft voter configuration and queries
// discovery for per-peer liveness within aliveCutoff. Returns ok=false when
// the raft configuration is unavailable (raft not yet wired up or
// GetConfiguration errored) so callers can fall back to a safe default
// instead of acting on a partial picture.
func (s *Cache) scanVoters(now time.Time, aliveCutoff time.Duration) (voterScan, bool) {
	rft := s.raft()
	if rft == nil {
		return voterScan{}, false
	}
	cfgFut := rft.GetConfiguration()
	if err := cfgFut.Error(); err != nil {
		return voterScan{}, false
	}

	pruneBefore := now.Add(-aliveCutoff)

	var scan voterScan
	for _, srv := range cfgFut.Configuration().Servers {
		if srv.Suffrage != raft.Voter {
			continue
		}
		scan.configVoters++
		if string(srv.ID) == s.localID {
			scan.selfIsVoter = true
			scan.aliveVoters = append(scan.aliveVoters, srv.ID)
			continue
		}
		if !s.discovery.isAliveSince(srv.ID, pruneBefore) {
			continue
		}
		scan.aliveVoters = append(scan.aliveVoters, srv.ID)
	}
	return scan, true
}

// aliveCutoff resolves the discovery-side "this peer is still alive" window
// the same way runQuorumRecoveryWatch does, so the request-shedding path in
// getLeaderWait and the recovery captain operate on the same view.
func (s *Cache) aliveCutoff() time.Duration {
	if s.cfg.pruneAfter > 0 {
		return s.cfg.pruneAfter
	}
	if s.cfg.recoverQuorumAfter > 0 {
		return s.cfg.recoverQuorumAfter
	}
	return s.cfg.noLeaderTimeout
}

// quorumProbeReachable actively probes each non-self voter in the
// current raft configuration with a short-timeout LatestUID RPC and
// reports whether enough peers respond to form a quorum. Self always
// counts as alive. Reuses LatestUID because its handler responds from
// any peer regardless of leadership (see consume) — making it a free
// liveness probe without a dedicated RPC.
//
// Concurrent callers serialize on quorumProbeM and share the result
// via the cache so a burst of leaderless requests pays one probe burst,
// not one per call. The cache is invalidated on raft Peer/Leader
// observations (see invalidateQuorumProbeCache) and expires after
// quorumProbeCacheTTL as a safety net when a peer recovers without an
// accompanying raft event.
//
// Returns true ("no information" fallback to the normal leader wait)
// when raft is not ready or the configuration is unavailable; these
// transient/structural cases are not cached because re-checking is
// cheap and the cached state could mask a fast recovery.
func (s *Cache) quorumProbeReachable(ctx context.Context, timeout time.Duration) bool {
	s.quorumProbeM.Lock()
	defer s.quorumProbeM.Unlock()

	if !s.quorumProbeAt.IsZero() && time.Since(s.quorumProbeAt) < quorumProbeCacheTTL {
		return s.quorumProbeReached
	}

	rft := s.raft()
	if rft == nil {
		// local raft might be re-initialising. Wait a bit.
		return true
	}
	cfgFut := rft.GetConfiguration()
	if err := cfgFut.Error(); err != nil {
		// local raft might be re-initialising. Wait a bit.
		return true
	}

	var voters []raft.Server
	for _, srv := range cfgFut.Configuration().Servers {
		if srv.Suffrage == raft.Voter {
			voters = append(voters, srv)
		}
	}
	reached := s.probeVoters(ctx, timeout, voters)
	s.quorumProbeReached = reached
	s.quorumProbeAt = time.Now()
	return reached
}

// probeVoters fans out a short-timeout LatestUID RPC to each non-self
// voter and reports whether enough respond to form a quorum. Self
// counts as alive when present in the voter set. Returns false when
// the voter set is empty (no quorum is possible without voters).
func (s *Cache) probeVoters(ctx context.Context, timeout time.Duration, voters []raft.Server) bool {
	if len(voters) == 0 {
		return false
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	alive := 0
	pending := 0
	results := make(chan bool, len(voters))
	for _, srv := range voters {
		if string(srv.ID) == s.localID {
			alive++
			continue
		}
		pending++
		go func(srv raft.Server) {
			_, err := s.transport.LatestUID(probeCtx, srv.ID, srv.Address, &pb.LatestUid{})
			results <- err == nil
		}(srv)
	}

	quorum := len(voters)/2 + 1
	for i := 0; i < pending; i++ {
		if <-results {
			alive++
			if alive >= quorum {
				return true
			}
		}
	}
	return false
}

// invalidateQuorumProbeCache marks the cached quorumProbeReachable
// result as stale so the next call re-probes. Called from the raft
// observation goroutine when a Peer or Leader event changes the
// reachability picture (voter added/removed, leader elected or
// stepped down). May briefly contend with an in-flight probe; the
// observer's chPeerChange is non-blocking so any event dropped while
// the cache mutex is held will be corrected on the next event or by
// the TTL.
func (s *Cache) invalidateQuorumProbeCache() {
	s.quorumProbeM.Lock()
	s.quorumProbeAt = time.Time{}
	s.quorumProbeM.Unlock()
}

// recovery procedure should run. The leaderless-duration threshold is the
// caller's responsibility (the recovery watcher arms a time.Timer for
// that); shouldRecoverQuorum only re-confirms there is still no leader
// and that this node is the elected recovery captain among the alive
// voters.
//
// aliveCutoff is the discovery-side "this peer is still alive" window. It
// is independent of the recovery threshold so the alive-window can match
// the auto-prune cutoff (the canonical dead-peer signal) rather than being
// forced to equal the safety-buffer threshold.
//
// The captain is the lowest-ID alive voter — a deterministic choice
// computed from a snapshot every stranded survivor agrees on, so at
// most one runs raft.RecoverCluster. The post-recovery snapshot may
// land at a lower index than another voter's already-replicated log
// tail (lagging captain, offline voter, stale ping); the instance-ID
// watcher in noteLeaderInstanceID detects that fork on the next ping
// from the recovered leader and resets the affected survivors, so the
// captain decision no longer needs to factor in per-peer LastIndex.
func (s *Cache) shouldRecoverQuorum(now time.Time, aliveCutoff time.Duration) bool {
	if s.suffrage != raft.Voter {
		return false
	}
	if s.IsLeader() || s.hasLeader() {
		return false
	}

	scan, ok := s.scanVoters(now, aliveCutoff)
	if !ok {
		return false
	}
	if !scan.selfIsVoter || scan.configVoters == 0 {
		return false
	}
	quorum := scan.configVoters/2 + 1
	if quorum <= len(scan.aliveVoters) {
		return false
	}

	lowest := raft.ServerID(s.localID)
	for _, id := range scan.aliveVoters {
		if id < lowest {
			lowest = id
		}
	}
	return lowest == raft.ServerID(s.localID)
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

// InstanceID returns this cache's current raft instance ID — a UUID that
// identifies the consensus history line the local raft belongs to. Empty
// until the cache has either bootstrapped its own cluster, run
// recoverQuorum, or adopted a leader's ID from a discovery ping. Exposed
// for tests and operational inspection.
func (s *Cache) InstanceID() string {
	return s.getInstanceID()
}

// getInstanceID returns this cache's current raft instance ID — see
// InstanceID for the user-facing semantics.
func (s *Cache) getInstanceID() string {
	s.raftMutex.RLock()
	defer s.raftMutex.RUnlock()

	return s.instanceID
}

func (s *Cache) setInstanceID(id string) {
	s.raftMutex.Lock()
	defer s.raftMutex.Unlock()

	s.instanceID = id
}

// getLastResetID returns the ID of the most recent hard reset applied to (or
// initiated by) this node. Used to deduplicate hard resets and to drive the
// addServer resend so a peer that (re)joins after a HardReset is wiped too.
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

// noteLeaderInstanceID is called by the discovery layer for every received
// ping (RT-12862). When the ping is from the peer that this node's local
// raft currently considers leader (rft.LeaderWithID()), the local
// instance ID is reconciled with the leader's:
//
//   - empty local ID → adopt the leader's silently. A node that has
//     never bootstrapped or recovered has no consensus history of its
//     own; it joins the cluster's history via normal replication.
//   - matching local ID → already aligned, nothing to do.
//   - different local ID → dispatch adoptLeaderInstance, which resets
//     the local raft and adopts the leader's ID, discarding the forked
//     in-memory log/snapshot that would otherwise trap this node in a
//     snapshot-regression catchup loop on top of the leader's older
//     snapshot.
//
// The leaderID gate is sufficient because rft.LeaderWithID() is the
// authoritative current-leader cache from this node's own raft: a ping
// from a peer that is not our raft's leader is filtered out before any
// instance-ID comparison runs. Concurrent goroutines dispatched by
// successive pings serialize on recoveryMutex inside
// adoptLeaderInstance; the re-check under that lock turns redundant
// dispatches into no-ops, so no separate debounce is needed.
func (s *Cache) noteLeaderInstanceID(peerID raft.ServerID, peerInstanceID string) {
	if peerInstanceID == "" {
		return
	}
	if string(peerID) == s.localID {
		return
	}
	rft := s.raft()
	if rft == nil {
		return
	}
	_, leaderID := rft.LeaderWithID()
	if leaderID == "" || leaderID != peerID {
		return
	}

	localID := s.getInstanceID()
	if localID == peerInstanceID {
		return
	}
	if localID == "" {
		s.setInstanceID(peerInstanceID)
		return
	}

	go s.adoptLeaderInstance(peerID, peerInstanceID)
}

// adoptLeaderInstance wipes the local raft stores and reinitializes raft
// with the leader's instance ID, after which standard AddVoter /
// InstallSnapshot replication brings this node back in line with the
// post-recovery cluster (RT-12862). Serialized via recoveryMutex so a
// concurrent quorum recovery or a second mismatch observation cannot
// race the reset; re-checks the local instance ID under lock to skip
// no-op resets.
func (s *Cache) adoptLeaderInstance(leaderID raft.ServerID, leaderInstanceID string) {
	s.recoveryMutex.Lock()
	defer s.recoveryMutex.Unlock()

	if s.getInstanceID() == leaderInstanceID {
		return
	}
	if s.IsLeader() {
		// Defensive: a node that is itself leader can't be on a forked
		// history relative to "the leader". Skip.
		return
	}

	s.logger.Warn("forked raft history detected; resetting to adopt leader instance",
		"leader-id", leaderID,
		"leader-instance-id", leaderInstanceID,
		"local-instance-id", s.getInstanceID(),
	)

	if err := s.reinitRaftAdoptingInstance(leaderInstanceID); err != nil {
		s.logger.Error("instance-id reset failed", "error", err)
	}
}

func (s *Cache) getRaftWithCtx() (*raft.Raft, context.Context) {
	s.raftMutex.RLock()
	defer s.raftMutex.RUnlock()

	return s.raftLocker, s.ctxRaft
}
