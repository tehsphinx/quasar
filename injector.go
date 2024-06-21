package quasar

import (
	"errors"
	"time"

	"github.com/hashicorp/raft"
)

type FSMInjector struct {
	cache *Cache
}

// TODO: add a context and apply a possible timeout being set.
func (s *FSMInjector) WaitFor(uid uint64) {
	s.cache.fsm.WaitFor(uid)
}

// TODO: add a context and apply a possible timeout being set.
func (s *FSMInjector) WaitForMasterLatest() error {
	uid, err := s.cache.masterLastIndex()
	if err != nil {
		return err
	}
	s.cache.fsm.WaitFor(uid)
	return nil
}

// TODO: add a context and apply a possible timeout being set.
func (s *FSMInjector) WaitForKnownLatest() {
	uid := s.cache.raft.LastIndex()
	s.cache.fsm.WaitFor(uid)
}

func (s *FSMInjector) Store(bts []byte) (uint64, error) {
	return s.cache.store("", bts)
}

type DiscoveryInjector struct {
	cache *Cache
}

func (s *DiscoveryInjector) GetServers() ([]raft.Server, error) {
	if s.cache.raft == nil {
		return nil, errors.New("raft not ready")
	}

	fut := s.cache.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return nil, err
	}

	return fut.Configuration().Servers, nil
}

func (s *DiscoveryInjector) AddServer(id, addr string, voter bool) error {
	addServerFn := s.getAddServerFunc(voter)

	fut := addServerFn(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
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

func (s *DiscoveryInjector) Name() string {
	return s.cache.name
}
