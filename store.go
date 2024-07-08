package quasar

import (
	"github.com/hashicorp/raft"
)

func wrapStore(s raft.LogStore, fsm *fsmWrapper) *store {
	return &store{
		LogStore: s,
		fsm:      fsm,
	}
}

// store is a wrapper struct for raft.LogStore
type store struct {
	raft.LogStore

	fsm *fsmWrapper
}

// StoreLog stores a log entry.
func (s *store) StoreLog(log *raft.Log) error {
	if log.Type != raft.LogCommand {
		s.fsm.regSystemUID(log.Index)
	}
	return s.LogStore.StoreLog(log)
}

// StoreLogs stores multiple log entries. By default, the logs stored may not be contiguous with previous logs
// (i.e. may have a gap in Index since the last log written). If an implementation can't tolerate this it may
// optionally implement `MonotonicLogStore` to indicate that this is not allowed. This changes Raft's behaviour
// after restoring a user snapshot to remove all previous logs instead of relying on a "gap" to signal the
// discontinuity between logs before the snapshot and logs after.
func (s *store) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		if log.Type != raft.LogCommand {
			s.fsm.regSystemUID(log.Index)
		}
	}
	return s.LogStore.StoreLogs(logs)
}
