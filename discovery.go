package quasar

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"golang.org/x/exp/maps"
)

const minPruneInterval = 500 * time.Millisecond

// Discovery defines a auto discovery for servers of the cache.
type Discovery interface {
	Inject(cache *DiscoveryInjector)
	Run(ctx context.Context) error
}

func newDiscoveryInjector(c *Cache) *DiscoveryInjector {
	return &DiscoveryInjector{
		cache:           c,
		applied:         map[raft.ServerID]struct{}{},
		servers:         map[raft.ServerID]raft.Server{},
		lastSeen:        map[raft.ServerID]time.Time{},
		peerInClusterCh: make(chan struct{}),
	}
}

// PeerStatus is a small, transport-agnostic snapshot of a node's raft state
// that Discovery implementations attach to outgoing pings. It is consumed by
// the bootstrap-gating logic on a restarting voter (RT-12775): any peer
// reporting HasLeader or NumVotersInConfig >= 2 proves an established
// cluster exists, so the restarter skips its single-node bootstrap.
//
// InstanceID carries the sender's cache.instanceID — a UUID minted whenever
// the cache creates a new raft via NewCache or recoverQuorum. A follower
// whose local instance ID differs from the *current* raft leader's
// reported InstanceID is sitting on a forked consensus history and must
// localReset before the leader's older snapshot lands on top of its stale
// log cache (RT-12862).
type PeerStatus struct {
	HasLeader         bool
	NumVotersInConfig uint32
	InstanceID        string
}

// inEstablishedCluster returns true when this status proves the reporting
// node is part of a real, multi-voter raft cluster — either via an active
// leader or via a multi-voter configuration that is currently leaderless
// (e.g. a stuck candidate after its peer crashed).
func (r PeerStatus) inEstablishedCluster() bool {
	return r.HasLeader || r.NumVotersInConfig >= 2
}

// DiscoveryInjector provides access to the underlying cache in a Discovery implementation.
//
//nolint:govet // Usually initialized once. Preferring readability to struct optimization here.
type DiscoveryInjector struct {
	cache *Cache

	serversM sync.RWMutex
	servers  map[raft.ServerID]raft.Server
	lastSeen map[raft.ServerID]time.Time
	appliedM sync.RWMutex
	applied  map[raft.ServerID]struct{}

	cancelM sync.Mutex
	cancel  context.CancelFunc

	// peerInClusterCh is closed (via peerInClusterOnce) the first time
	// ProcessServer observes a non-self peer whose PeerStatus proves it is
	// part of an established cluster (HasLeader || NumVotersInConfig >= 2).
	// Used by startup bootstrap-gating (RT-12775): seeing such a peer means
	// the restarter must skip its own single-node bootstrap. A peer that
	// pings without such status (e.g. another node still in its own
	// bootstrap-wait) does not close the channel, so two cold-starting
	// voters won't deadlock waiting for each other.
	peerInClusterOnce sync.Once
	peerInClusterCh   chan struct{}

	// voterCount tracks how many voters are in this node's raft
	// configuration. Maintained by the regObservation goroutine via raft
	// PeerObservation events (including the bootstrap configuration, which
	// raft emits as well). Read non-blocking from LocalPeerStatus so NATS
	// callbacks never have to wait on raft's main loop.
	voterCount atomic.Uint32
}

// Name returns the caches name. This name identifies a cache and separates it from other caches on the network.
func (s *DiscoveryInjector) Name() string {
	return s.cache.name
}

// ServerInfo returns the raft.Server info of this cache instance.
func (s *DiscoveryInjector) ServerInfo() raft.Server {
	return s.cache.serverInfo()
}

// ProcessServer processes the given server. It adds it to the list of known
// servers. If it is a new server, it will also be added to raft if we are
// the leader. Equivalent to ProcessServerWithStatus(srv, PeerStatus{}): no
// cluster-membership signal is carried, so it cannot abort a bootstrap-wait.
// Kept for backwards compatibility with discovery implementations that don't
// (yet) propagate raft status on the wire.
func (s *DiscoveryInjector) ProcessServer(srv raft.Server) {
	s.ProcessServerWithStatus(srv, PeerStatus{})
}

// ProcessServerWithStatus is the raft-status-aware variant of ProcessServer.
// Discovery implementations should call this when they can read raft status
// off the incoming ping. The status is consulted to decide whether the
// observation is enough to short-circuit a bootstrap-wait on a restarting
// voter (RT-12775) — see PeerStatus.inEstablishedCluster. It also feeds
// the raft instance-ID watcher (RT-12862): a ping from the current raft
// leader carrying an InstanceID different from this node's own signals
// that the local raft state belongs to a forked consensus history that
// must be reset before catch-up replication starts shipping the leader's
// (older) snapshot on top of stale logs.
func (s *DiscoveryInjector) ProcessServerWithStatus(srv raft.Server, status PeerStatus) {
	if srv.ID != raft.ServerID(s.cache.localID) && status.inEstablishedCluster() {
		s.peerInClusterOnce.Do(func() { close(s.peerInClusterCh) })
	}

	s.cache.noteLeaderInstanceID(srv.ID, status.InstanceID)

	isNew := s.setServer(srv)
	if !isNew {
		return
	}

	if err := s.addServer(srv); err != nil {
		// TODO: log?
		return
	}
}

// PeerInCluster returns a channel that is closed when ProcessServer (with
// status) has observed a non-self peer that is part of an established raft
// cluster. Used by startup bootstrap-gating to abort the wait window as
// soon as an existing cluster announces itself. Subsequent observations are
// no-ops on this channel.
func (s *DiscoveryInjector) PeerInCluster() <-chan struct{} {
	return s.peerInClusterCh
}

// LocalPeerStatus returns a snapshot of the local node's raft state for
// Discovery implementations to attach to outgoing pings. Non-blocking:
// reads atomic state already maintained by the raft-observation
// goroutine, so this is safe to call from NATS callback context without
// stalling message delivery on raft's main loop.
func (s *DiscoveryInjector) LocalPeerStatus() PeerStatus {
	rft := s.cache.raft()
	if rft == nil {
		return PeerStatus{InstanceID: s.cache.getInstanceID()}
	}
	_, leaderID := rft.LeaderWithID()
	return PeerStatus{
		HasLeader:         leaderID != "",
		NumVotersInConfig: s.voterCount.Load(),
		InstanceID:        s.cache.getInstanceID(),
	}
}

func (s *DiscoveryInjector) addServer(srv raft.Server) error {
	addServerFn, err := s.getAddServerFunc(srv.Suffrage == raft.Voter)
	if err != nil {
		return err
	}

	fut := addServerFn(srv.ID, srv.Address, 0, 0)
	if r := fut.Error(); r != nil {
		return r
	}

	if s.cache.getLastResetID() != "" {
		resp, r := s.cache.sendReset(context.Background(), srv)
		if r != nil {
			return r
		}
		if resp.GetError() != "" {
			return errors.New(resp.GetError())
		}
	}

	return nil
}

type addServerFunc func(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture

func (s *DiscoveryInjector) getAddServerFunc(voter bool) (addServerFunc, error) {
	rft := s.cache.raft()
	if rft == nil {
		return nil, errors.New("raft not set (yet)")
	}
	if voter {
		return rft.AddVoter, nil
	}
	return rft.AddNonvoter, nil
}

func (s *DiscoveryInjector) run(ctx context.Context, rft *raft.Raft) {
	if s.cache.cfg.pruneAfter != 0 {
		s.registerPruning(ctx, s.cache.cfg.pruneAfter)
	}
	s.regObservation(ctx, rft)
}

func (s *DiscoveryInjector) regObservation(ctx context.Context, rft *raft.Raft) {
	s.cancelPrevious()
	ctx, cancel := context.WithCancel(ctx)
	s.setCancel(cancel)

	// Non-blocking observer: raft's `observe` holds observersLock.RLock
	// while delivering, and a blocking send into a full channel would pin
	// that lock and stall DeregisterObserver elsewhere. The events we
	// handle (PeerObservation, LeaderObservation) are advisory hints — a
	// dropped event is corrected on the next ping / re-add cycle.
	chPeerChange := make(chan raft.Observation, observationChanSize)
	observer := raft.NewObserver(chPeerChange, false, func(o *raft.Observation) bool {
		if _, ok := o.Data.(raft.PeerObservation); ok {
			return true
		}
		if _, ok := o.Data.(raft.LeaderObservation); ok {
			return true
		}
		return false
	})
	rft.RegisterObserver(observer)

	// Reset the voter count for this raft instance; the cached value is
	// re-derived from observed configuration changes (including the
	// bootstrap configuration that raft applies via the same path).
	s.voterCount.Store(0)

	go func() {
		defer func() {
			rft.DeregisterObserver(observer)
			cancel()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case observation := <-chPeerChange:
				switch obs := observation.Data.(type) {
				case raft.PeerObservation:
					s.cache.invalidateQuorumProbeCache()
					if obs.Removed {
						if obs.Peer.Suffrage == raft.Voter {
							// Is the equivalent of decreasing by 1.
							s.voterCount.Add(^uint32(0))
						}
						s.deleteServer(obs.Peer.ID)
						s.removeApplied(obs.Peer.ID)
						continue
					}
					if obs.Peer.Suffrage == raft.Voter {
						s.voterCount.Add(1)
					}
					s.setApplied(obs.Peer.ID)
				case raft.LeaderObservation:
					s.cache.invalidateQuorumProbeCache()
					if obs.LeaderID != raft.ServerID(s.cache.localID) {
						continue
					}
					go s.addMissingServers()
				}
			}
		}
	}()
}

func (s *DiscoveryInjector) addMissingServers() {
	for _, srv := range s.getServers() {
		if s.hasApplied(srv.ID) {
			continue
		}

		//nolint:staticcheck // empty branch will be filled later
		if err := s.addServer(srv); err != nil {
			// TODO: log?
		}
	}
}

func (s *DiscoveryInjector) registerPruning(ctx context.Context, after time.Duration) {
	interval := after / 5
	if interval < minPruneInterval {
		interval = minPruneInterval
	}

	tick := time.NewTicker(interval)

	go func() {
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				s.pruneExpiredServers(now.Add(-after))
			}
		}
	}()
}

func (s *DiscoveryInjector) pruneExpiredServers(pruneBefore time.Time) {
	if !s.cache.IsLeader() {
		return
	}

	expired := s.getExpiredServers(pruneBefore)
	if len(expired) == 0 {
		return
	}

	go func(expired []raft.Server) {
		for _, srv := range expired {
			if err := s.removeServer(srv.ID); err != nil {
				continue
			}
			s.deleteServer(srv.ID)
			s.removeApplied(srv.ID)
		}
	}(expired)
}

func (s *DiscoveryInjector) getExpiredServers(pruneBefore time.Time) []raft.Server {
	s.serversM.RLock()
	defer s.serversM.RUnlock()

	var expired []raft.Server
	for id, srv := range s.servers {
		if id == raft.ServerID(s.cache.localID) {
			continue
		}
		lastSeen, ok := s.lastSeen[id]
		if !ok {
			continue
		}
		if lastSeen.After(pruneBefore) {
			continue
		}

		expired = append(expired, srv)
	}

	return expired
}

func (s *DiscoveryInjector) removeServer(id raft.ServerID) error {
	rft := s.cache.raft()
	if rft == nil {
		return errors.New("raft not set (yet)")
	}

	fut := rft.RemoveServer(id, 0, 0)
	return fut.Error()
}

func (s *DiscoveryInjector) hasApplied(srvID raft.ServerID) bool {
	s.appliedM.Lock()
	defer s.appliedM.Unlock()

	_, ok := s.applied[srvID]
	return ok
}

func (s *DiscoveryInjector) setApplied(srvID raft.ServerID) {
	s.appliedM.Lock()
	defer s.appliedM.Unlock()

	s.applied[srvID] = struct{}{}
}

func (s *DiscoveryInjector) removeApplied(srvID raft.ServerID) {
	s.appliedM.Lock()
	defer s.appliedM.Unlock()

	delete(s.applied, srvID)
}

func (s *DiscoveryInjector) setServer(srv raft.Server) bool {
	s.serversM.Lock()
	defer s.serversM.Unlock()

	s.lastSeen[srv.ID] = time.Now()

	_, ok := s.servers[srv.ID]
	if ok {
		return false
	}

	s.servers[srv.ID] = srv
	return true
}

func (s *DiscoveryInjector) deleteServer(id raft.ServerID) {
	s.serversM.Lock()
	defer s.serversM.Unlock()

	delete(s.servers, id)
	delete(s.lastSeen, id)
}

// isAliveSince reports whether the given peer has pinged discovery on or
// after `since`.
func (s *DiscoveryInjector) isAliveSince(id raft.ServerID, since time.Time) bool {
	s.serversM.RLock()
	defer s.serversM.RUnlock()

	last, ok := s.lastSeen[id]
	if !ok {
		return false
	}
	return !last.Before(since)
}

// forgetServer removes all bookkeeping for the given peer so that a fresh
// discovery ping from that ID will be treated as a brand-new server. Used by
// the quorum-recovery path after dropping a missing voter from raft so that,
// when the voter eventually returns, ProcessServer triggers an AddVoter
// instead of being suppressed as a duplicate.
func (s *DiscoveryInjector) forgetServer(id raft.ServerID) {
	s.serversM.Lock()
	delete(s.servers, id)
	delete(s.lastSeen, id)
	s.serversM.Unlock()

	s.appliedM.Lock()
	delete(s.applied, id)
	s.appliedM.Unlock()
}

func (s *DiscoveryInjector) getServers() []raft.Server {
	s.serversM.RLock()
	defer s.serversM.RUnlock()

	return maps.Values(s.servers)
}

func (s *DiscoveryInjector) setCancel(cancel context.CancelFunc) {
	s.cancelM.Lock()
	defer s.cancelM.Unlock()

	s.cancel = cancel
}

func (s *DiscoveryInjector) cancelPrevious() {
	s.cancelM.Lock()
	defer s.cancelM.Unlock()

	if s.cancel != nil {
		s.cancel()
	}
}
