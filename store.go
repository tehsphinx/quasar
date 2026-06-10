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

// store is a wrapper struct for raft.LogStore.
type store struct {
	raft.LogStore

	fsm *fsmWrapper
}

// regSystemUID registers the index of a non-command log entry so
// fsmWrapper.lastApplied keeps moving across entries that never reach
// FSM.Apply — without it, WaitFor would hang on e.g. the noop every newly
// elected leader appends.
//
// Configuration entries are deliberately NOT registered here: logs are
// stored BEFORE they are committed, and an index registered at store time
// can advance lastApplied past an entry a new leader later truncates and
// replaces — releasing WaitFor callers before the real entry at that index
// is applied (RT-13042 M10). Committed configuration entries reach the FSM
// via StoreConfiguration (raft.ConfigurationStore) instead, which registers
// them at commit time. Noop entries never reach the FSM on any path, so
// store time remains the only hook for them; that residual window
// self-corrects on the next applied entry and is accepted.
func (s *store) regSystemUID(log *raft.Log) {
	if log.Type == raft.LogCommand || log.Type == raft.LogConfiguration {
		return
	}
	s.fsm.regSystemUID(log.Index)
}

// StoreLog stores a log entry.
func (s *store) StoreLog(log *raft.Log) error {
	s.regSystemUID(log)
	return s.LogStore.StoreLog(log)
}

// StoreLogs stores multiple log entries. By default, the logs stored may not be contiguous with previous logs
// (i.e. may have a gap in Index since the last log written). If an implementation can't tolerate this it may
// optionally implement `MonotonicLogStore` to indicate that this is not allowed. This changes Raft's behaviour
// after restoring a user snapshot to remove all previous logs instead of relying on a "gap" to signal the
// discontinuity between logs before the snapshot and logs after.
func (s *store) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		s.regSystemUID(log)
	}
	return s.LogStore.StoreLogs(logs)
}
