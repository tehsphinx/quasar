package quasar

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"

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

type cacheFSM interface {
	raft.FSM
	WaitFor(uid uint64)
}

func wrapFSM(fsm FSM) *fsmWrapper {
	mutex := &sync.Mutex{}
	return &fsmWrapper{
		fsm:   fsm,
		condM: mutex,
		cond:  sync.NewCond(mutex),
	}
}

type fsmWrapper struct {
	fsm FSM

	lastApplied uint64
	condM       *sync.Mutex
	cond        *sync.Cond
}

type applyResponse struct {
	resp *pb.CommandResponse
	err  error
}

func (s *fsmWrapper) Apply(log *raft.Log) interface{} {
	var cmd pb.Command
	if err := proto.Unmarshal(log.Data, &cmd); err != nil {
		return applyResponse{err: err}
	}

	resp, respErr := s.apply(log, &cmd)

	s.uidApplied(log.Index)

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
		lastApplied: s.lastApplied,
	}, nil
}

func (s *fsmWrapper) Restore(snapshot io.ReadCloser) error {
	s.lastApplied = 0

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

	s.lastApplied = uid
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

	for s.lastApplied < uid {
		s.cond.Wait()
	}
}

func (s *fsmWrapper) uidApplied(uid uint64) {
	s.condM.Lock()
	defer s.condM.Unlock()

	s.lastApplied = uid
	s.cond.Broadcast()
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
