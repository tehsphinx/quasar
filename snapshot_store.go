package quasar

import (
	"io"

	"github.com/hashicorp/raft"
)

func wrapSnapshotStore(s raft.SnapshotStore, fsm *fsmWrapper) *snapshotStore {
	return &snapshotStore{
		SnapshotStore: s,
		fsm:           fsm,
	}
}

// snapshotStore wraps a raft.SnapshotStore to record the meta index of every
// snapshot raft opens. raft's runFSM opens a snapshot and immediately calls
// FSM.Restore with its reader on the same goroutine, so the recorded index is
// exactly the raft index the restore lands at — the only reliable source for
// fsmWrapper.lastApplied after a restore, since the value embedded in the
// snapshot data predates the (possibly burned / leader-side) restore index
// (RT-13042 M7).
//
// The leader also opens its latest snapshot when streaming an InstallSnapshot
// to a follower; that may overwrite the recorded index concurrently, but the
// in-memory snapshot store retains only the latest snapshot, so any
// concurrent Open records the same index.
type snapshotStore struct {
	raft.SnapshotStore

	fsm *fsmWrapper
}

// Open opens the given snapshot and records its meta index for the restore
// that may follow.
func (s *snapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	meta, rc, err := s.SnapshotStore.Open(id)
	if err != nil {
		return meta, rc, err
	}
	s.fsm.noteRestoreIndex(meta.Index)
	return meta, rc, nil
}
