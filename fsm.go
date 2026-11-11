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
	"github.com/tehsphinx/quasar/internal/cond"
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
//
// Reset (and ResetFull, see FullResetter) may be called CONCURRENTLY with
// ApplyCmd and must serialize against it internally. A hard reset applied on
// the raft leader clears the FSM from an RPC goroutine while raft keeps
// applying, by design — the leader keeps its raft so its heartbeats can catch
// the wiped followers up (see Cache.HardReset). The follower rebuild paths do
// join raft's goroutines before resetting, but they are not the only callers
// (RT-14147).
type FSM interface {
	Inject(fsm *FSMInjector)
	ApplyCmd(cmd []byte) error
	Snapshot() (raft.FSMSnapshot, error)
	Restore(snapshot io.ReadCloser) error
	Reset() error
}

// Replayer is an optional extension of FSM. ReplayCmd applies a command like
// ApplyCmd does, for a log entry that is not news to anyone the node serves:
//
//   - an entry this process had applied before raft was rebuilt on the same
//     history: quorum recovery re-applies the log behind the newest snapshot,
//     and a follower rebuilt by a hard reset fetches the leader's log again;
//   - an entry the cluster had when a fresh process joined it, applied while
//     that process was catching up.
//
// An FSM that notifies others of the changes it applies implements it to bring
// its state up to date without notifying them a second time, days late. One that
// does not gets every entry through ApplyCmd.
//
// After the node adopted another history, the catch-up goes through ApplyCmd:
// what the other branch applied is news to this node, and an index cannot tell
// it from what both branches share (RT-15906).
type Replayer interface {
	ReplayCmd(cmd []byte) error
}

// FullResetter is an optional extension of FSM. When the FSM implements it,
// quasar calls ResetFull(full) instead of Reset() when applying a reset,
// passing whether stores that normally survive a reset must also be cleared (a
// full reset; see ResetCache.full). Implementing it is optional and
// non-breaking: an FSM that only implements Reset keeps working — quasar then
// never performs a full reset.
type FullResetter interface {
	ResetFull(full bool) error
}

type logApplier interface {
	Apply(log *raft.Log) interface{}
}

func wrapFSM(fsm FSM) *fsmWrapper {
	s := &fsmWrapper{
		fsm:  fsm,
		cond: cond.New(),
	}
	// The logApplier fast path returns the unexported applyResponse, which
	// applyLocal type-asserts. Only the internal kvFSM can satisfy that
	// contract, so detect it concretely: a user FSM that happens to
	// implement Apply(*raft.Log) would otherwise take the fast path and
	// panic the leader on the first apply (L1).
	if _, ok := fsm.(*kvFSM); ok {
		s.isLogApplier = true
	}
	// A fresh process starts in replay: what it applies until it knows where
	// the cluster stands is the cluster's past (see Cache.endStartupReplay).
	if _, ok := fsm.(Replayer); ok {
		s.starting.Store(true)
	}
	return s
}

//nolint:govet // struct optimization not worth it. Is not created often. Optimized for readability.
type fsmWrapper struct {
	fsm FSM

	lastApplied uint64
	// replayUntil, replayTerm and starting decide which entries a Replayer gets
	// as replays, see isReplay. replayMu orders their changes; replayEpoch
	// counts the times the log changed under the boundary, see endStartup.
	replayMu    sync.Mutex
	replayEpoch uint64
	replayUntil atomic.Uint64
	replayTerm  atomic.Uint64
	starting    atomic.Bool
	// lastCmd and lastCmdTerm are the index and the term of the last command
	// the FSM applied. Unlike lastApplied they never run ahead of it: that one
	// also takes the index of a noop when it is stored, which a later leader
	// can still replace with a command.
	lastCmd     atomic.Uint64
	lastCmdTerm atomic.Uint64
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
	defer func() {
		s.lastCmd.Store(log.Index)
		s.lastCmdTerm.Store(log.Term)
	}()

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
		return nil, s.applyReset(cmd.ResetCache.GetFull())
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
	s.lastCmd.Store(0)
	s.lastCmdTerm.Store(0)
	s.resetSysUIDs()
}

// applyReset clears the FSM. full selects a full reset: when true and the FSM
// implements FullResetter, stores that normally survive a reset are also
// cleared. An FSM that does not implement FullResetter is never reset fully
// (full is ignored).
func (s *fsmWrapper) applyReset(full bool) error {
	if r, ok := s.fsm.(FullResetter); ok {
		return r.ResetFull(full)
	}
	return s.fsm.Reset()
}

func (s *fsmWrapper) store(log *raft.Log, cmd *pb.Store) (*pb.CommandResponse, error) {
	var err error
	if replayer, ok := s.fsm.(Replayer); ok && s.isReplay(log) {
		err = replayer.ReplayCmd(cmd.Data)
	} else {
		err = s.fsm.ApplyCmd(cmd.Data)
	}
	return respStore(&pb.StoreResponse{Uid: log.Index}), err
}

// isReplay reports whether log is a replay, see Replayer: any entry while the
// process is starting, and afterwards an entry within the boundary. The
// boundary is an index and a term. Whoever writes an entry under an index that
// was taken before does it as a leader elected later, in a higher term: a node
// that wins an election after a hard reset wiped the voters ahead of it, or the
// node that recovered the quorum alone. Its entries are news whatever their
// index.
func (s *fsmWrapper) isReplay(log *raft.Log) bool {
	return s.starting.Load() ||
		log.Index <= s.replayUntil.Load() && log.Term <= s.replayTerm.Load()
}

// replayUpTo sets the boundary: every entry up to index, written in a term up
// to term, is a replay. It is called when the log changes under the FSM:
// before raft is rebuilt, with replayedSoFar when the rebuilt raft has the same
// history and with nothing when it adopts another one, and with nothing when a
// history starts on this node.
func (s *fsmWrapper) replayUpTo(index, term uint64) {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()

	s.replayEpoch++
	s.replayUntil.Store(index)
	s.replayTerm.Store(term)
}

// replayedSoFar is the boundary to keep across a rebuild of raft on the same
// history: the last command this process applied, or the boundary it has when
// that reaches further, as it does on a process that is still catching up.
// Only raft's goroutines move it, so it is read once they are gone.
func (s *fsmWrapper) replayedSoFar() (index, term uint64) {
	if until := s.replayUntil.Load(); until > s.lastCmd.Load() {
		return until, s.replayTerm.Load()
	}
	return s.lastCmd.Load(), s.lastCmdTerm.Load()
}

// startupEpoch is what endStartup tells an index of the current log by. It is
// taken before the leader is asked for the index.
func (s *fsmWrapper) startupEpoch() uint64 {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()

	return s.replayEpoch
}

// endStartup ends the replay a fresh process starts in. index is where the
// cluster stood when the process learned it, and term the term the process was
// in when it asked: entries up to both stay replays, others are news. It
// reports false and changes nothing when the log changed since epoch was
// taken: index is then one of the log before, and as a boundary on the new
// log, which starts over, it would turn news into replays for as long as that
// log needs to reach it.
func (s *fsmWrapper) endStartup(index, term, epoch uint64) bool {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()

	if !s.starting.Load() {
		return true
	}
	if s.replayEpoch != epoch {
		return false
	}
	if index > s.replayUntil.Load() {
		s.replayUntil.Store(index)
		s.replayTerm.Store(term)
	}
	s.starting.Store(false)
	return true
}

// WaitFor blocks until the FSM has applied uid, or until ctx expires.
//
// The wait takes no lock on purpose. The broadcast sites run on raft's own
// goroutines — uidApplied on the FSM goroutine, regSystemUID on the main
// goroutine via the store wrapper — and sharing a mutex with the waiters made
// their per-broadcast cost grow with the number of in-flight waiters, enough to
// stall either one under a client retry storm (RT-13896). What replaces the
// mutex is an ordering: the applier publishes lastApplied BEFORE swapping and
// closing the wait channel, and a waiter installs the wait channel BEFORE
// re-reading lastApplied. Go's atomics are sequentially consistent, so whichever
// side goes first is observed by the other and no wakeup is lost.
//
// This makes the WAKE path lock-free, not the whole apply path: applySysUIDs
// still takes sysUIDsM, shared with queueSysUID on the main goroutine. That
// contention is bounded by two goroutines and a slice reslice, not by the
// waiter count, which is why it stays.
func (s *fsmWrapper) WaitFor(ctx context.Context, uid uint64) error {
	for {
		if s.getLastApplied() >= uid {
			return nil
		}

		// Install first, then re-check: see the ordering note above.
		ch := s.cond.WaitChan()
		if s.getLastApplied() >= uid {
			return nil
		}

		select {
		case <-ch:
			// Broadcast wakes every waiter on every applied entry, so loop to
			// find out whether the entry that landed is this waiter's.
		case <-ctx.Done():
			// The wait channel may have been closed by a Broadcast at the same
			// instant the context expired; select then picks ctx.Done() at
			// random even though the condition was actually met. Re-check once
			// so a wait that succeeded exactly at the deadline is not reported
			// as a timeout (m25).
			if s.getLastApplied() >= uid {
				return nil
			}
			if s.hasLeader != nil && !s.hasLeader() {
				return errors.Join(ctx.Err(), ErrWaitFor, ErrNoLeader)
			}
			return errors.Join(ctx.Err(), ErrWaitFor)
		}
	}
}

func (s *fsmWrapper) uidApplied(uid uint64) {
	s.setLastApplied(uid)

	s.applySysUIDs()

	// No lock: lastApplied is published above, before Broadcast swaps and closes
	// the wait channel. See WaitFor.
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
