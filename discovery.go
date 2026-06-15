package quasar

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"golang.org/x/exp/maps"
)

const minPruneInterval = 500 * time.Millisecond

// addServerHardResetTimeout bounds the out-of-band hard-reset RPC sent to a
// peer that (re)joins after a HardReset. Without a bound it relied entirely on
// transport-internal deadlines and could block the discovery ping dispatcher
// indefinitely (m10, see also m17).
const addServerHardResetTimeout = 5 * time.Second

// addServerCommitTimeout bounds how long addServer waits for an AddVoter /
// AddNonvoter configuration change to commit. raft's own timeout argument
// only bounds the enqueue onto the configuration-change channel, not the
// commit; the future then blocks until the entry commits or the leader steps
// down (~lease timeout). On a leader that just lost quorum that parks the
// caller for the whole lease, so bound the commit-wait explicitly (L8).
const addServerCommitTimeout = 5 * time.Second

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
		instanceIDs:     map[raft.ServerID]string{},
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
	// instanceIDs holds each peer's last-pinged raft instance ID (RT-13067).
	// addServer's admission gate compares it against the local instance ID: a
	// peer that does not provably follow this node's consensus history is
	// hard-reset before being added to the configuration. Empty means the peer
	// never reported one (fresh node, freshly wiped node, or a discovery
	// implementation that does not propagate status) and is treated as a
	// mismatch — the wipe is a no-op on a genuinely fresh peer.
	instanceIDs map[raft.ServerID]string
	appliedM    sync.RWMutex
	applied     map[raft.ServerID]struct{}

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

	// sawLeaderPingAt is the UnixNano timestamp of the most recent ping from a
	// non-self peer reporting HasLeader=true (0 until the first such ping). It
	// distinguishes the two ways PeerInCluster can fire: a genuinely live cluster
	// (some peer sees a leader) versus a stale, leaderless config still
	// advertising NumVotersInConfig>=2. The RT-13067 bootstrap-gating liveness
	// fallback reads it (via observedLeaderPing) to avoid forking a competing
	// cluster when a real leader exists and will (re)admit this node. It is a
	// timestamp rather than a sticky bool so the signal ages out: a leader that
	// stops pinging (e.g. the surviving node was itself restarting and is now
	// gone) no longer pins this voter for the whole process lifetime, which would
	// otherwise force a restart to clear it.
	sawLeaderPingAt atomic.Int64

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

// Logger exposes the cache's logger to Discovery implementations so delivery
// failures can be surfaced instead of silently swallowed (RT-13067): a
// discovery plane that stops delivering pings disarms instance-ID re-seeding
// and peer liveness, and used to be invisible in the logs.
func (s *DiscoveryInjector) Logger() hclog.Logger {
	return s.cache.logger
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
	if srv.ID != raft.ServerID(s.cache.localID) {
		if status.inEstablishedCluster() {
			s.peerInClusterOnce.Do(func() { close(s.peerInClusterCh) })
		}
		if status.HasLeader {
			s.sawLeaderPingAt.Store(time.Now().UnixNano())
		}
	}

	s.cache.noteLeaderInstanceID(srv.ID, status.InstanceID)

	isNew := s.setServerWithInstanceID(srv, status.InstanceID)
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

// observedLeaderPing reports whether a non-self peer has reported HasLeader=true
// within the given window. Used by the RT-13067 bootstrap-gating liveness
// fallback to tell a live cluster (keep waiting to be re-admitted) from a stale,
// leaderless one (bootstrap to break the wedge). The window ages the signal out:
// a live leader pings every discovery cadence, so a recent leader ping means a
// real leader still exists; once leader pings stop for a full window the cluster
// is treated as leaderless. A zero/negative window only matches a ping at the
// current instant, i.e. effectively reports "no live leader".
func (s *DiscoveryInjector) observedLeaderPing(window time.Duration) bool {
	at := s.sawLeaderPingAt.Load()
	if at == 0 {
		return false
	}
	return time.Since(time.Unix(0, at)) <= window
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

	// Admission gate (RT-13067): a (re)joining peer is added directly only when
	// its last-pinged raft instance ID proves it already follows this node's
	// consensus history. Any other peer — a different instance ID, or none
	// reported — may have survived this node's restart on a higher raft term:
	// adding it first starts replication immediately and its higher term
	// demotes a freshly bootstrapped leader within a heartbeat, before any
	// post-add wipe could run, trapping the cluster in an election loop. So the
	// mismatched peer is hard-reset to fresh stores FIRST (the RPC is confirmed:
	// the receiver applies the wipe before responding), then added, and catch-up
	// replication brings it back. The gate is armed from the instant of
	// bootstrap (the instance ID is minted there), so the pre-startup-reset
	// window is covered without any admission hold. Empty peer instance IDs are
	// treated as mismatch: a genuinely fresh peer wipes as a no-op, while a peer
	// that joined the previous incarnation moments before the restart (never
	// adopted an ID, still ahead) is defused. Gated on IsLeader (a non-leader's
	// AddVoter/AddNonvoter would fail anyway) and on having a local instance ID:
	// a leader without one (joined, elected before adopting) cannot claim a
	// history and must not wipe healthy followers.
	//
	// The wipe is keyed on the local instance ID — the leader's current
	// consensus incarnation — NEVER on getLastResetID(): retries for the same
	// incarnation dedup on the peer (markResetID), while a peer wiped under an
	// older key is wiped again for the new incarnation. The last reset ID can
	// be inherited from a previous incarnation's sweep (e.g. across a quorum
	// recovery on a node that was itself wiped earlier), a key the peers have
	// already recorded — keying on it made their dedup silently skip the
	// admission wipe, readmitting a potentially forked peer unwiped.
	localInst := s.cache.getInstanceID()
	if s.cache.IsLeader() && localInst != "" && s.peerInstanceID(srv.ID) != localInst {
		ctx, cancel := context.WithTimeout(s.cache.ctx, addServerHardResetTimeout)
		r := s.cache.sendHardReset(ctx, srv, localInst)
		cancel()
		if r != nil {
			return r
		}
	}

	fut := addServerFn(srv.ID, srv.Address, 0, addServerCommitTimeout)
	// fut.Error() blocks until the config change commits or leadership is
	// lost; bound that wait so a quorum-less leader does not park this caller
	// (run per discovery ping since m17) for the full lease timeout. The
	// future resolves on its own afterward, so the buffered receiver does not
	// leak (L8).
	chErr := make(chan error, 1)
	go func() { chErr <- fut.Error() }()

	select {
	case r := <-chErr:
		if r != nil {
			return r
		}
	case <-time.After(addServerCommitTimeout):
		return fmt.Errorf("add server %q: timed out waiting for configuration change to commit", srv.ID)
	case <-s.cache.ctx.Done():
		return s.cache.ctx.Err()
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
	return s.setServerWithInstanceID(srv, "")
}

// setServerWithInstanceID is setServer plus bookkeeping of the peer's
// last-pinged raft instance ID, which addServer's admission gate consults
// (RT-13067). Last ping wins: a peer that wipes (instance ID cleared) or
// adopts a new instance ID is re-evaluated on its next ping.
func (s *DiscoveryInjector) setServerWithInstanceID(srv raft.Server, instanceID string) bool {
	s.serversM.Lock()
	defer s.serversM.Unlock()

	s.lastSeen[srv.ID] = time.Now()
	s.instanceIDs[srv.ID] = instanceID

	known, ok := s.servers[srv.ID]
	if ok && known.Address == srv.Address {
		return false
	}

	// New ID, or a known ID that came back on a different address (a peer
	// restarted with a dynamic port/IP). Treat an address change as new so
	// ProcessServerWithStatus re-runs addServer: AddVoter with a changed
	// address updates the raft configuration entry, otherwise the leader
	// keeps the dead address forever and the peer is unreachable (M3).
	s.servers[srv.ID] = srv
	return true
}

func (s *DiscoveryInjector) deleteServer(id raft.ServerID) {
	s.serversM.Lock()
	defer s.serversM.Unlock()

	delete(s.servers, id)
	delete(s.lastSeen, id)
	delete(s.instanceIDs, id)
}

// peerInstanceID returns the raft instance ID the given peer reported on its
// last discovery ping, or "" if it never reported one.
func (s *DiscoveryInjector) peerInstanceID(id raft.ServerID) string {
	s.serversM.RLock()
	defer s.serversM.RUnlock()

	return s.instanceIDs[id]
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
	delete(s.instanceIDs, id)
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
