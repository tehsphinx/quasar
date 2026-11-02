package quasar

import (
	"bytes"
	"fmt"
	"io"
	"sync"

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

	// closeMu makes the newer-snapshot check in pendingSink.Close and the
	// create that follows it one step.
	closeMu sync.Mutex
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

// Create defers the wrapped store's Create to the sink's Close (RT-15590).
// raft.InmemSnapshotStore makes a sink its latest, and only, snapshot as soon
// as Create returns, and its Cancel does nothing. A Persist that failed
// part-way therefore replaced the last good snapshot with a truncated one: a
// retried quorum recovery restored it, and an InstallSnapshot shipped it to a
// lagging follower. Buffering keeps the previous snapshot until the new one is
// complete, at the cost of a second copy of the snapshot while it is written.
func (s *snapshotStore) Create(version raft.SnapshotVersion, index, term uint64,
	configuration raft.Configuration, configurationIndex uint64, trans raft.Transport,
) (raft.SnapshotSink, error) {
	return &pendingSink{store: s, index: index, create: func() (raft.SnapshotSink, error) {
		return s.SnapshotStore.Create(version, index, term, configuration, configurationIndex, trans)
	}}, nil
}

// pendingSink buffers a snapshot and hands it to the wrapped store on Close;
// Cancel drops it.
type pendingSink struct {
	bytes.Buffer
	store    *snapshotStore
	index    uint64
	create   func() (raft.SnapshotSink, error)
	sink     raft.SnapshotSink
	canceled bool
}

// Close creates the snapshot in the wrapped store. It is idempotent: an FSM's
// Persist may close the sink, and raft closes it again. After Cancel it does
// nothing, like raft's FileSnapshotSink.
//
// Close fails if the wrapped store already holds a snapshot at a higher index.
// With the create deferred to Close, the sink closed last becomes the latest
// snapshot. On a follower, raft's takeSnapshot persists a local snapshot while
// the main loop installs and restores a newer one from the leader. Had the
// local snapshot won, the store would fall back to an index whose following
// logs were compacted: a quorum recovery fails at GetLog on it until a
// restart, and a leader ships it to a follower that then needs those logs. On
// the error, takeSnapshot keeps lastSnapshot and the logs. An equal index is
// stored: raft.RecoverCluster writes its snapshot at the index it restored
// when no log follows it.
func (p *pendingSink) Close() error {
	if p.sink != nil || p.canceled {
		return nil
	}
	p.store.closeMu.Lock()
	defer p.store.closeMu.Unlock()
	snaps, err := p.store.List()
	if err != nil {
		return err
	}
	if len(snaps) > 0 && snaps[0].Index > p.index {
		return fmt.Errorf("snapshot at index %d is older than the stored one at index %d", p.index, snaps[0].Index)
	}
	sink, err := p.create()
	if err != nil {
		return err
	}
	if _, err := p.WriteTo(sink); err != nil {
		_ = sink.Cancel()
		return err
	}
	p.sink = sink
	return sink.Close()
}

// Cancel drops the buffered snapshot. The wrapped store never sees it, and a
// later Close does not create it.
func (p *pendingSink) Cancel() error {
	p.canceled = true
	p.Reset()
	return nil
}

// ID is the created snapshot's ID. raft reads it only after Close.
func (p *pendingSink) ID() string {
	if p.sink == nil {
		return ""
	}
	return p.sink.ID()
}
