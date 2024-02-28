package quasar

import (
	"io"

	"github.com/hashicorp/raft"
)

func NewFSM() *FSM {
	return &FSM{}
}

type FSM struct {
}

func (F *FSM) Apply(log *raft.Log) interface{} {
	// TODO implement me
	panic("implement me")
}

func (F *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// TODO implement me
	panic("implement me")
}

func (F *FSM) Restore(snapshot io.ReadCloser) error {
	// TODO implement me
	panic("implement me")
}
