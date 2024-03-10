package transports

import (
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
)

type Transport interface {
	raft.Transport

	CacheConsumer() <-chan raft.RPC

	Store(id raft.ServerID, target raft.ServerAddress, command *pb.StoreValue) (*pb.StoreValueResponse, error)
	Load(id raft.ServerID, target raft.ServerAddress, command *pb.LoadValue) (*pb.LoadValueResponse, error)
}
