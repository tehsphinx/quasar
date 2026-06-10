package quasar

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"golang.org/x/exp/maps"
)

const minPruneInterval = 500 * time.Millisecond

// addServerHardResetTimeout bounds the out-of-band hard-reset RPC sent to a
// peer that (re)joins after a HardReset. Without a bound it relied entirely on
// transport-internal deadlines and could block the discovery ping dispatcher
// indefinitely (m10, see also m17).
const addServerHardResetTimeout = 5 * time.Second

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
// reinit its raft before the leader's older snapshot lands on top of its
// stale log cache (RT-12862).
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

	// voterCount caches how many voters are in this node's raft
	// configuration, self included. Re-derived from rft.GetConfiguration()
	// (a non-blocking snapshot in raft v1.7.x) at regObservation time and
	// on every Peer/Leader observation — NOT counted from PeerObservation
	// events, which raft emits only on the leader and only for non-self
	// peers, so an event-counted value is 0 on every follower (RT-13042
	// M1). Read non-blocking from LocalPeerStatus so NATS callbacks never
	// have to wait on raft's main loop.
	voterCount atomic.Uint32

	// startedAt is the UnixNano timestamp at which discovery (re)started on
	// the current raft instance. It anchors the grace window for the
	// config-driven prune sweep (see graceElapsed): until a full prune window
	// has elapsed the lastSeen map is still being refilled by incoming pings
	// and must not be trusted to declare a configured peer dead. Reset on
	// every regObservation so a recoverQuorum-rebuilt raft gets a fresh
	// window.
	startedAt atomic.Int64
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

	// If a hard reset has happened, wipe any peer that (re)joins afterwards so a
	// node that was unreachable during the HardReset fan-out — and is therefore
	// still ahead — cannot re-wedge the cluster on rejoin (RT-13034).
	if resetID := s.cache.getLastResetID(); resetID != "" {
		ctx, cancel := context.WithTimeout(s.cache.ctx, addServerHardResetTimeout)
		defer cancel()
		if r := s.cache.sendHardReset(ctx, srv, resetID); r != nil {
			return r
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
	s.regObservation(ctx, rft)
}

func (s *DiscoveryInjector) regObservation(ctx context.Context, rft *raft.Raft) {
	s.cancelPrevious()
	ctx, cancel := context.WithCancel(ctx)
	s.setCancel(cancel)

	// Pruning shares the per-raft cancel with the observer goroutine: run is
	// invoked on every raft (re)creation (newCache, reinitRaftAdoptingInstance,
	// recoverQuorum), and a ticker started on the cache-lifetime context would
	// leak one goroutine per rebuild — duplicated sweeps and duplicated
	// RemoveServer calls on flapping clusters (RT-13042 M9). cancelPrevious
	// above stops the predecessor's ticker along with its observer.
	if s.cache.cfg.pruneAfter != 0 {
		s.registerPruning(ctx, s.cache.cfg.pruneAfter)
	}

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

	// Seed the voter count from the new raft instance's configuration so
	// LocalPeerStatus is correct from the first ping, before any
	// observation arrives.
	s.refreshVoterCount(rft)

	// Anchor a fresh grace window for the config-driven prune sweep: lastSeen
	// starts cold on a (re)started raft and needs a full prune window of
	// incoming pings before it can be trusted to declare a configured peer
	// dead.
	s.startedAt.Store(time.Now().UnixNano())

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
					s.refreshVoterCount(rft)
					if obs.Removed {
						s.deleteServer(obs.Peer.ID)
						s.removeApplied(obs.Peer.ID)
						continue
					}
					s.setApplied(obs.Peer.ID)
				case raft.LeaderObservation:
					s.cache.invalidateQuorumProbeCache()
					s.refreshVoterCount(rft)
					if obs.LeaderID != raft.ServerID(s.cache.localID) {
						continue
					}
					go s.addMissingServers()
				}
			}
		}
	}()
}

// refreshVoterCount re-derives the cached voter count from the raft
// configuration snapshot (self included). GetConfiguration is non-blocking
// in raft v1.7.x — it returns an already-resolved future over the latest
// known configuration — so this is safe to call from the observation
// goroutine. On a transient configuration error the previous value is kept;
// the next observation or regObservation re-derives it.
func (s *DiscoveryInjector) refreshVoterCount(rft *raft.Raft) {
	cfgFut := rft.GetConfiguration()
	if err := cfgFut.Error(); err != nil {
		return
	}

	var count uint32
	for _, srv := range cfgFut.Configuration().Servers {
		if srv.Suffrage == raft.Voter {
			count++
		}
	}
	s.voterCount.Store(count)
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
	expired := s.getExpiredServers(pruneBefore)

	// Leader only: also reconcile the committed raft configuration. A peer
	// that is a configured member but never pings discovery is absent from
	// the ping-driven servers map, so the sweep above can never see it.
	//
	// Gated on graceElapsed so a node that has just (re)started does not
	// mistake a still-alive peer that simply has not pinged us yet for a dead
	// one.
	if s.cache.IsLeader() && s.graceElapsed(pruneBefore) {
		expired = s.appendExpiredConfigServers(expired, pruneBefore)
	}

	if len(expired) == 0 {
		return
	}

	go func(expired []raft.Server) {
		for _, srv := range expired {
			if s.cache.IsLeader() {
				// only remove from raft when leader
				if err := s.removeServer(srv.ID); err != nil {
					continue
				}
			}
			// always remove from discovery -- unless we are the leader and removal failed
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

// graceElapsed reports whether discovery has been running on the current raft
// instance for at least a full prune window.
func (s *DiscoveryInjector) graceElapsed(pruneBefore time.Time) bool {
	started := s.startedAt.Load()
	if started == 0 {
		return false
	}
	return time.Unix(0, started).Before(pruneBefore)
}

// appendExpiredConfigServers returns the peers in the committed raft configuration
// -- excluding self and anything already in skip -- that have not pinged
// discovery within the prune window.
func (s *DiscoveryInjector) appendExpiredConfigServers(expired []raft.Server, pruneBefore time.Time) []raft.Server {
	rft := s.cache.raft()
	if rft == nil {
		return nil
	}
	cfgFut := rft.GetConfiguration()
	if err := cfgFut.Error(); err != nil {
		return nil
	}

	for _, srv := range cfgFut.Configuration().Servers {
		if srv.ID == raft.ServerID(s.cache.localID) {
			continue
		}
		if s.isAliveSince(srv.ID, pruneBefore) {
			continue
		}
		if slices.ContainsFunc(expired, func(s raft.Server) bool { return s.ID == srv.ID }) {
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
