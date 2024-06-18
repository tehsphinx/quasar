package quasar

import (
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
	// Snapshot() (raft.FSMSnapshot, error)
	// Restore(snapshot io.ReadCloser) error
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
	// TODO implement me
	panic("implement me")
}

func (s *fsmWrapper) Restore(snapshot io.ReadCloser) error {
	// TODO implement me
	panic("implement me")
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
