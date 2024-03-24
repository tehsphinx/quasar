package quasar

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/stores"
	"google.golang.org/protobuf/proto"
)

type FSM interface {
	Apply(cmd *pb.Command) (*pb.CommandResponse, error)
	Snapshot() (raft.FSMSnapshot, error)
	Restore(snapshot io.ReadCloser) error
}

func NewFSM(kv stores.KVStore) FSM {
	return &cacheFSM{
		store: kv,
	}
}

type cacheFSM struct {
	store stores.KVStore
}

func (s *cacheFSM) Apply(command *pb.Command) (*pb.CommandResponse, error) {
	switch cmd := command.GetCmd().(type) {
	case *pb.Command_StoreValue:
		return s.Store(cmd.StoreValue)
	case *pb.Command_LoadValue:
		return s.Load(cmd.LoadValue)
	}

	fmt.Printf("%+v\n", command)
	return nil, nil
}

func (s *cacheFSM) Store(cmd *pb.StoreValue) (*pb.CommandResponse, error) {
	err := s.store.Store(cmd.Key, cmd.Data)
	return respStore(&pb.StoreValueResponse{}), err
}

func (s *cacheFSM) Load(cmd *pb.LoadValue) (*pb.CommandResponse, error) {
	data, err := s.store.Load(cmd.Key)
	return respLoad(&pb.LoadValueResponse{Data: data}), err
}

func (s *cacheFSM) Snapshot() (raft.FSMSnapshot, error) {
	// TODO implement me
	panic("implement me")
}

func (s *cacheFSM) Restore(snapshot io.ReadCloser) error {
	// TODO implement me
	panic("implement me")
}

func wrapFSM(raftFSM FSM) raft.FSM {
	return &fsmWrapper{
		fsm: raftFSM,
	}
}

type applyResponse struct {
	resp *pb.CommandResponse
	err  error
}

type fsmWrapper struct {
	fsm FSM
}

func (s *fsmWrapper) Apply(log *raft.Log) interface{} {
	var cmd pb.Command
	if err := proto.Unmarshal(log.Data, &cmd); err != nil {
		return applyResponse{err: err}
	}

	resp, err := s.fsm.Apply(&cmd)
	return applyResponse{
		resp: resp,
		err:  err,
	}
}

func (s *fsmWrapper) Snapshot() (raft.FSMSnapshot, error) {
	return s.fsm.Snapshot()
}

func (s *fsmWrapper) Restore(snapshot io.ReadCloser) error {
	return s.fsm.Restore(snapshot)
}
