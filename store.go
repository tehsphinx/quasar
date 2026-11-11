package quasar

import (
	"sync/atomic"

	"github.com/armon/go-metrics"
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

	// The log budget, see snapshotOver. maxBytes is zero without one.
	maxBytes     int64
	snapshot     func()
	labels       []metrics.Label
	trigger      atomic.Int64 // log size above which the next snapshot is asked for
	snapshotting atomic.Bool
}

// logSizer is what the budget needs from the wrapped store: what the entries it
// holds weigh. stores.LogRing implements it.
type logSizer interface {
	Bytes() int
}

// snapshotOver arms the log budget: once the log outgrows maxBytes, snapshot is
// called to have raft take a snapshot, whose compaction is what trims the log.
// raft itself only snapshots on an entry count, and an entry weighs anything
// from a few bytes to tens of kilobytes, so on a cluster of large entries the
// count is reached after gigabytes of log or never (RT-15905).
//
// snapshot has to return only when raft is done with the snapshot. cache names
// the cache on the quasar_log_bytes gauge, which reports what the log weighs.
func (s *store) snapshotOver(cache string, maxBytes int, snapshot func()) {
	s.maxBytes = int64(maxBytes)
	s.snapshot = snapshot
	s.labels = []metrics.Label{{Name: "cache", Value: cache}}
	s.trigger.Store(s.maxBytes)
}

// checkBudget asks for a snapshot when the log has outgrown its budget. It runs
// on raft's goroutines after every append and must not block them, so the
// snapshot is taken on its own goroutine, one at a time.
func (s *store) checkBudget() {
	if s.maxBytes <= 0 {
		return
	}
	logs, ok := s.LogStore.(logSizer)
	if !ok {
		return
	}
	size := int64(logs.Bytes())
	// The only number that shows the log growing before the memory runs out.
	metrics.SetGaugeWithLabels([]string{"quasar", "log", "bytes"}, float32(size), s.labels)
	if size <= s.trigger.Load() || !s.snapshotting.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer s.snapshotting.Store(false)

		s.snapshot()
		// The compaction keeps what raft's TrailingLogs and the entries not yet
		// snapshotted demand, which can be more than the budget, and a snapshot
		// can fail. Ask again only after the log has grown by another half
		// budget, or every append from here on would ask for one.
		s.trigger.Store(max(s.maxBytes, int64(logs.Bytes())+s.maxBytes/2))
	}()
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
	if err := s.LogStore.StoreLog(log); err != nil {
		return err
	}
	s.checkBudget()
	return nil
}

// IsMonotonic implements the raft.MonotonicLogStore interface by forwarding to
// the wrapped store, the shim raft itself uses for LogCache. Embedding
// raft.LogStore promotes only that interface's method set, so without this
// raft's `logs.(MonotonicLogStore)` assertion fails whatever the wrapped store
// says, and the wrapper decides the snapshot-install and user-restore paths on
// its own. stores.LogRing reports false and has its reasons (RT-14463); the
// forward is what lets it.
func (s *store) IsMonotonic() bool {
	if monotonic, ok := s.LogStore.(raft.MonotonicLogStore); ok {
		return monotonic.IsMonotonic()
	}
	return false
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
	if err := s.LogStore.StoreLogs(logs); err != nil {
		return err
	}
	s.checkBudget()
	return nil
}
