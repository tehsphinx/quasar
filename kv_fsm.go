package quasar

import (
	"errors"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/stores"
	"google.golang.org/protobuf/proto"
)

func newKeyValueFSM(kv stores.KVStore) *kvFSM {
	mutex := &sync.Mutex{}
	return &kvFSM{
		KVStore: kv,
		condM:   mutex,
		cond:    sync.NewCond(mutex),
	}
}

type kvFSM struct {
	stores.KVStore

	lastApplied uint64
	condM       *sync.Mutex
	cond        *sync.Cond
}

func (s *kvFSM) Apply(log *raft.Log) interface{} {
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

func (s *kvFSM) store(uid uint64, cmd *pb.Store) (*pb.CommandResponse, error) {
	err := s.Store(cmd.Key, cmd.Data)
	return respStore(&pb.StoreResponse{Uid: uid}), err
}

func (s *kvFSM) WaitFor(uid uint64) {
	s.condM.Lock()
	defer s.condM.Unlock()

	for s.lastApplied < uid {
		s.cond.Wait()
	}
}

func (s *kvFSM) uidApplied(uid uint64) {
	s.condM.Lock()
	defer s.condM.Unlock()

	s.lastApplied = uid
	s.cond.Broadcast()
}
