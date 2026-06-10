package quasar

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/cond"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

const uint64Bytes = 8

// FSM defines the fsm to be implemented for the quasar cache.
//
// Restore must fully replace the FSM's state with the snapshot contents
// (not merge into it): quorum recovery restores the newest snapshot and
// then replays the log tail on top, which is only exact if Restore starts
// from a clean slate.
//
// When WithQuorumRecovery is enabled, ApplyCmd should be idempotent.
// Recovery before the first auto-snapshot replays the whole log; the
// library resets the FSM first (see recoverQuorum, M2) so a single replay
// is exact, but a command whose effect depends on prior state (counters,
// appends, toggles) is still safest written idempotently.
type FSM interface {
	Inject(fsm *FSMInjector)
	ApplyCmd(cmd []byte) error
	Snapshot() (raft.FSMSnapshot, error)
	Restore(snapshot io.ReadCloser) error
	Reset() error
}

type logApplier interface {
	Apply(log *raft.Log) interface{}
}

func wrapFSM(fsm FSM) *fsmWrapper {
	mutex := &sync.Mutex{}
	s := &fsmWrapper{
		fsm:   fsm,
		condM: mutex,
		cond:  cond.New(mutex),
	}
	// The logApplier fast path returns the unexported applyResponse, which
	// applyLocal type-asserts. Only the internal kvFSM can satisfy that
	// contract, so detect it concretely: a user FSM that happens to
	// implement Apply(*raft.Log) would otherwise take the fast path and
	// panic the leader on the first apply (L1).
	if _, ok := fsm.(*kvFSM); ok {
		s.isLogApplier = true
	}
	return s
}

//nolint:govet // struct optimization not worth it. Is not created often. Optimized for readability.
type fsmWrapper struct {
	fsm FSM

	lastApplied uint64
	// restoreIndex is the meta index of the snapshot raft most recently
	// opened from the wrapped snapshot store (see wrapSnapshotStore). raft's
	// runFSM opens a snapshot and immediately calls Restore on the same
	// goroutine, so the value read inside Restore is the index the restore
	// lands at — for both the follower InstallSnapshot path and the
	// burned-index user-restore path (RT-13042 M7). Accessed atomically;
	// consumed (reset to 0) by takeRestoreIndex.
	restoreIndex uint64
	sysUIDsM     sync.Mutex
	sysUIDs      []uint64
	condM        *sync.Mutex
	cond         *cond.Cond

	// hasLeader reports whether the cache currently sees a raft leader.
	// Wired up by newCache after the Cache is constructed; used by WaitFor
	// to enrich the returned error when the wait context expires while
	// there is no leader to make progress.
	hasLeader func() bool

	isLogApplier bool
}

type applyResponse struct {
	resp *pb.CommandResponse
	err  error
}

func (s *fsmWrapper) Apply(log *raft.Log) interface{} {
	defer s.uidApplied(log.Index)

	if s.isLogApplier {
		if fsm, ok := s.fsm.(logApplier); ok {
			return fsm.Apply(log)
		}
	}

	var cmd pb.Command
	if err := proto.Unmarshal(log.Data, &cmd); err != nil {
		return applyResponse{err: err}
	}

	resp, respErr := s.apply(log, &cmd)

	return applyResponse{
		resp: resp,
		err:  respErr,
	}
}

func (s *fsmWrapper) apply(log *raft.Log, command *pb.Command) (*pb.CommandResponse, error) {
	switch cmd := command.GetCmd().(type) {
	case *pb.Command_Store:
		return s.store(log, cmd.Store)
	case *pb.Command_ResetCache:
		return nil, s.applyReset()
	default:
		// fmt.Printf("%+v\n", command)
		return nil, errors.New("fsmWrapper.apply: command not implemented")
	}
}

func (s *fsmWrapper) Snapshot() (raft.FSMSnapshot, error) {
	snapshot, err := s.fsm.Snapshot()
	if err != nil {
		return nil, err
	}

	return &snapshotWrapper{
		snapshot:    snapshot,
		lastApplied: s.getLastApplied(),
	}, nil
}

func (s *fsmWrapper) Restore(snapshot io.ReadCloser) error {
	s.setLastApplied(0)

	// io.ReadFull: a plain Read may legally return fewer than 8 bytes with a
	// nil error, and an error returned from Restore on the user-restore path
	// makes raft v1.7.x panic the whole process (RT-13042 M7).
	bts := make([]byte, uint64Bytes)
	if _, err := io.ReadFull(snapshot, bts); err != nil {
		return fmt.Errorf("failed to parse lastApplied: %w", err)
	}

	// The embedded value is the writer's lastApplied at Snapshot() time. It
	// travels with the data but does NOT identify the raft index this restore
	// lands at: a user restore (raft.Restore) re-snapshots the data at a
	// burned index max(lastIndex, snapshotIndex)+1, and a follower
	// InstallSnapshot applies the leader's snapshot at the leader's meta
	// index. The old `embedded + 1` heuristic matched only the
	// nothing-written-since user-restore case and left followers one index
	// AHEAD after InstallSnapshot — WaitFor returned before the entry was
	// applied, briefly serving stale reads (RT-13042 M7). Use the meta index
	// of the snapshot raft just opened; the embedded value remains the
	// fallback for direct Restore calls that bypass the snapshot store.
	uid := uint64FromBytes(bts)
	if metaIndex := s.takeRestoreIndex(); metaIndex != 0 {
		uid = metaIndex
	}

	if r := s.fsm.Restore(snapshot); r != nil {
		return r
	}

	s.uidApplied(uid)
	return nil
}

// noteRestoreIndex records the meta index of the snapshot raft just opened
// from the wrapped snapshot store. See the restoreIndex field doc.
func (s *fsmWrapper) noteRestoreIndex(index uint64) {
	atomic.StoreUint64(&s.restoreIndex, index)
}

// takeRestoreIndex consumes the recorded snapshot meta index, returning 0
// when no snapshot was opened since the last consumption.
func (s *fsmWrapper) takeRestoreIndex() uint64 {
	return atomic.SwapUint64(&s.restoreIndex, 0)
}

var _ raft.ConfigurationStore = (*fsmWrapper)(nil)

// StoreConfiguration is invoked by raft once a configuration-change log
// entry is committed and handed to the FSM goroutine. It registers the
// entry's index at COMMIT time — the store wrapper skips configuration
// entries at store time precisely so an uncommitted (and possibly later
// truncated) config change cannot advance lastApplied early (RT-13042 M10).
//
// Implements raft.ConfigurationStore.
func (s *fsmWrapper) StoreConfiguration(index uint64, _ raft.Configuration) {
	s.regSystemUID(index)
}

func (s *fsmWrapper) applyRaftReset() {
	s.setLastApplied(0)
	s.resetSysUIDs()
}

func (s *fsmWrapper) applyReset() error {
	return s.fsm.Reset()
}

func (s *fsmWrapper) store(log *raft.Log, cmd *pb.Store) (*pb.CommandResponse, error) {
	err := s.fsm.ApplyCmd(cmd.Data)
	return respStore(&pb.StoreResponse{Uid: log.Index}), err
}

func (s *fsmWrapper) WaitFor(ctx context.Context, uid uint64) error {
	s.condM.Lock()
	defer s.condM.Unlock()

	for s.getLastApplied() < uid {
		if err := s.cond.WaitContext(ctx); err != nil {
			// The wait channel may have been closed by a Broadcast at the same
			// instant the context expired; select then picks ctx.Done() at
			// random even though the condition was actually met. Re-check once
			// (the lock is held again here) so a wait that succeeded exactly at
			// the deadline is not reported as a timeout (m25).
			if s.getLastApplied() >= uid {
				return nil
			}
			if s.hasLeader != nil && !s.hasLeader() {
				return errors.Join(err, ErrWaitFor, ErrNoLeader)
			}
			return errors.Join(err, ErrWaitFor)
		}
	}
	return nil
}

func (s *fsmWrapper) uidApplied(uid uint64) {
	s.setLastApplied(uid)

	s.applySysUIDs()

	s.condM.Lock()
	defer s.condM.Unlock()

	s.cond.Broadcast()
}

func (s *fsmWrapper) regSystemUID(uid uint64) {
	if applied := s.applySysUID(uid); applied {
		// Drain queued successors: with configuration entries registering at
		// commit time (RT-13042 M10), a freshly elected leader's noop is
		// stored — and queued here — BEFORE the configuration entry below it
		// commits, so the commit-time registration must pop the queue or
		// lastApplied stalls until the first command applies.
		s.applySysUIDs()

		s.condM.Lock()
		defer s.condM.Unlock()

		s.cond.Broadcast()
		return
	}

	s.queueSysUID(uid)
}

func (s *fsmWrapper) queueSysUID(uid uint64) {
	s.sysUIDsM.Lock()
	defer s.sysUIDsM.Unlock()

	s.sysUIDs = append(s.sysUIDs, uid)
}

func (s *fsmWrapper) popSysUID(apply func(uid uint64) bool) bool {
	s.sysUIDsM.Lock()
	defer s.sysUIDsM.Unlock()

	if len(s.sysUIDs) == 0 {
		return false
	}

	uid := s.sysUIDs[0]
	if !apply(uid) {
		return false
	}
	s.sysUIDs = s.sysUIDs[1:]

	return true
}

func (s *fsmWrapper) resetSysUIDs() {
	s.sysUIDsM.Lock()
	defer s.sysUIDsM.Unlock()

	s.sysUIDs = s.sysUIDs[:0]
}

func (s *fsmWrapper) applySysUID(uid uint64) bool {
	if applied := s.incrLastAppliedTo(uid); applied {
		return true
	}
	if uid <= s.getLastApplied() {
		return true
	}
	return false
}

func (s *fsmWrapper) applySysUIDs() {
	for s.popSysUID(s.applySysUID) {
	}
}

func (s *fsmWrapper) getLastApplied() uint64 {
	return atomic.LoadUint64(&s.lastApplied)
}

func (s *fsmWrapper) setLastApplied(uid uint64) {
	atomic.StoreUint64(&s.lastApplied, uid)
}

func (s *fsmWrapper) incrLastAppliedTo(uid uint64) bool {
	return atomic.CompareAndSwapUint64(&s.lastApplied, uid-1, uid)
}

type snapshotWrapper struct {
	snapshot    raft.FSMSnapshot
	lastApplied uint64
}

func (s *snapshotWrapper) Persist(sink raft.SnapshotSink) error {
	// write lastApplied to sink before writing other data
	lastAppliedBts := uint64ToBytes(s.lastApplied)
	if _, err := sink.Write(lastAppliedBts); err != nil {
		return err
	}

	return s.snapshot.Persist(sink)
}

func (s *snapshotWrapper) Release() {}

func uint64ToBytes(val uint64) []byte {
	bts := make([]byte, uint64Bytes)
	binary.LittleEndian.PutUint64(bts, val)
	return bts
}

func uint64FromBytes(val []byte) uint64 {
	return binary.LittleEndian.Uint64(val)
}
