package quasar

import (
	"errors"
	"io"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/stores"
	"google.golang.org/protobuf/proto"
)

func newKeyValueFSM(kv stores.KVStore) *kvFSM {
	return &kvFSM{
		KVStore: kv,
	}
}

var (
	_ FSM        = (*kvFSM)(nil)
	_ logApplier = (*kvFSM)(nil)
)

type kvFSM struct {
	stores.KVStore
	fsm *FSMInjector
}

func (s *kvFSM) Inject(fsm *FSMInjector) {
	s.fsm = fsm
}

func (s *kvFSM) ApplyCmd([]byte) error {
	// should never be called as we implement Apply function.
	return nil
}

func (s *kvFSM) Apply(log *raft.Log) interface{} {
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

func (s *kvFSM) apply(log *raft.Log, command *pb.Command) (*pb.CommandResponse, error) {
	switch cmd := command.GetCmd().(type) {
	case *pb.Command_Store:
		return s.store(log.Index, cmd.Store)
	default:
		// fmt.Printf("%+v\n", command)
		return nil, errors.New("kvFSM.apply: command not implemented")
	}
}

func (s *kvFSM) Snapshot() (raft.FSMSnapshot, error) {
	// TODO implement me
	panic("implement me")
}

func (s *kvFSM) Restore(snapshot io.ReadCloser) error {
	// TODO implement me
	panic("implement me")
}

func (s *kvFSM) Reset() error {
	// TODO implement me
	panic("implement me")
}

func (s *kvFSM) store(uid uint64, cmd *pb.Store) (*pb.CommandResponse, error) {
	err := s.Store(cmd.Key, cmd.Data)
	return respStore(&pb.StoreResponse{Uid: uid}), err
}
