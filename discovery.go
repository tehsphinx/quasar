package quasar

import (
	"context"
	"errors"
	"sync"
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
		cache:    c,
		applied:  map[raft.ServerID]struct{}{},
		servers:  map[raft.ServerID]raft.Server{},
		lastSeen: map[raft.ServerID]time.Time{},
	}
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
}

// Name returns the caches name. This name identifies a cache and separates it from other caches on the network.
func (s *DiscoveryInjector) Name() string {
	return s.cache.name
}

// ServerInfo returns the raft.Server info of this cache instance.
func (s *DiscoveryInjector) ServerInfo() raft.Server {
	return s.cache.serverInfo()
}

// ProcessServer processes the given server. It adds it to the list of known servers.
// If it is a new server, it will also be added to raft if we are the leader.
func (s *DiscoveryInjector) ProcessServer(srv raft.Server) {
	isNew := s.setServer(srv)
	if !isNew {
		return
	}

	if err := s.addServer(srv); err != nil {
		// TODO: log?
		return
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

	chPeerChange := make(chan raft.Observation, observationChanSize)
	observer := raft.NewObserver(chPeerChange, true, func(o *raft.Observation) bool {
		if _, ok := o.Data.(raft.PeerObservation); ok {
			return true
		}
		if _, ok := o.Data.(raft.LeaderObservation); ok {
			return true
		}
		return false
	})
	rft.RegisterObserver(observer)

	go func() {
		defer func() {
			cancel()
			rft.DeregisterObserver(observer)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case observation := <-chPeerChange:
				switch obs := observation.Data.(type) {
				case raft.PeerObservation:
					if obs.Removed {
						s.deleteServer(obs.Peer.ID)
						s.removeApplied(obs.Peer.ID)
						continue
					}
					s.setApplied(obs.Peer.ID)
				case raft.LeaderObservation:
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
