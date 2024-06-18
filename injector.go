package quasar

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
