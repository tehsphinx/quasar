package quasar

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"golang.org/x/exp/maps"
)

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

type DiscoveryInjector struct {
	cache *Cache

	serversM sync.RWMutex
	servers  map[raft.ServerID]raft.Server
	appliedM sync.RWMutex
	applied  map[raft.ServerID]struct{}
}

func (s *DiscoveryInjector) Name() string {
	return s.cache.name
}

func (s *DiscoveryInjector) ServerInfo() raft.Server {
	return s.cache.serverInfo()
}

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
	addServerFn := s.getAddServerFunc(srv.Suffrage == raft.Voter)

	fut := addServerFn(srv.ID, srv.Address, 0, 0)
	if err := fut.Error(); err != nil {
		return err
	}
	return nil
}

type addServerFunc func(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture

func (s *DiscoveryInjector) getAddServerFunc(voter bool) addServerFunc {
	if voter {
		return s.cache.raft.AddVoter
	}
	return s.cache.raft.AddNonvoter
}

func (s *DiscoveryInjector) regObservation(rft *raft.Raft) {
	chPeerChange := make(chan raft.Observation, 5)
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
		// TODO: handle shutdown
		for observation := range chPeerChange {
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
	}()
}

func (s *DiscoveryInjector) addMissingServers() {
	for _, srv := range s.servers {
		if s.hasApplied(srv.ID) {
			continue
		}

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
