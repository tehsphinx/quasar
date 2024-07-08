package quasar

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

type FSM interface {
	Inject(fsm *FSMInjector)
	ApplyCmd(cmd []byte) error
	Snapshot() (raft.FSMSnapshot, error)
	Restore(snapshot io.ReadCloser) error
}

type logApplier interface {
	Apply(log *raft.Log) interface{}
}

func wrapFSM(fsm FSM) *fsmWrapper {
	mutex := &sync.Mutex{}
	s := &fsmWrapper{
		fsm:   fsm,
		condM: mutex,
		cond:  sync.NewCond(mutex),
	}
	if _, ok := fsm.(logApplier); ok {
		s.isLogApplier = true
	}
	return s
}

type fsmWrapper struct {
	fsm  FSM
	raft *raft.Raft

	lastApplied uint64
	sysUIDsM    sync.Mutex
	sysUIDs     []uint64
	condM       *sync.Mutex
	cond        *sync.Cond

	isLogApplier bool
}

func (s *fsmWrapper) SetRaft(rft *raft.Raft) {
	s.raft = rft
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

	bts := make([]byte, 8)
	n, err := snapshot.Read(bts)
	if err != nil {
		return err
	}
	if n != len(bts) {
		return errors.New("failed to parse lastApplied: not enough bytes found")
	}
	uid := uint64FromBytes(bts)

	if r := s.fsm.Restore(snapshot); r != nil {
		return r
	}

	s.setLastApplied(uid)
	s.cond.Broadcast()
	return nil
}

func (s *fsmWrapper) store(log *raft.Log, cmd *pb.Store) (*pb.CommandResponse, error) {
	err := s.fsm.ApplyCmd(cmd.Data)
	return respStore(&pb.StoreResponse{Uid: log.Index}), err
}

func (s *fsmWrapper) WaitFor(uid uint64) {
	s.condM.Lock()
	defer s.condM.Unlock()

	for s.getLastApplied() < uid {
		s.cond.Wait()
	}
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
	bts := make([]byte, 8)
	binary.LittleEndian.PutUint64(bts, val)
	return bts
}

func uint64FromBytes(val []byte) uint64 {
	return binary.LittleEndian.Uint64(val)
}
