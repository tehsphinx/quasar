package quasar

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"golang.org/x/exp/maps"
)

// Discovery defines a auto discovery for servers of the cache.
type Discovery interface {
	Inject(cache *DiscoveryInjector)
	Run(ctx context.Context) error
}

func newDiscoveryInjector(c *Cache) *DiscoveryInjector {
	return &DiscoveryInjector{
		cache:   c,
		applied: map[raft.ServerID]struct{}{},
		servers: map[raft.ServerID]raft.Server{},
	}
}

// DiscoveryInjector provides access to the underlying cache in a Discovery implementation.
//
//nolint:govet // Usually initialized once. Preferring readability to struct optimization here.
type DiscoveryInjector struct {
	cache *Cache

	serversM sync.RWMutex
	servers  map[raft.ServerID]raft.Server
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
	if err := fut.Error(); err != nil {
		return err
	}
	return nil
}

type addServerFunc func(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture

func (s *DiscoveryInjector) getAddServerFunc(voter bool) (addServerFunc, error) {
	if s.cache.raft == nil {
		return nil, errors.New("raft not set (yet)")
	}
	if voter {
		return s.cache.raft.AddVoter, nil
	}
	return s.cache.raft.AddNonvoter, nil
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

	_, ok := s.servers[srv.ID]
	if ok {
		return false
	}

	s.servers[srv.ID] = srv
	return true
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
