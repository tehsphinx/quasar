package quasar

// ReinitForTest rebuilds the cache's raft onto fresh stores adopting
// adoptInstanceID, the way a hard reset or a fork adoption does.
func (s *Cache) ReinitForTest(adoptInstanceID string) error {
	s.recoveryMutex.Lock()
	defer s.recoveryMutex.Unlock()

	return s.reinitRaftAdoptingInstance(adoptInstanceID, false)
}
