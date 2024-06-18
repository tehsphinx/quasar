package transports

import (
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
)

type Transport interface {
	raft.Transport

	CacheConsumer() <-chan raft.RPC

	Store(id raft.ServerID, target raft.ServerAddress, command *pb.Store) (*pb.StoreResponse, error)
	LatestUID(id raft.ServerID, target raft.ServerAddress, command *pb.LatestUid) (*pb.LatestUidResponse, error)
	// Load(id raft.ServerID, target raft.ServerAddress, command *pb.LoadValue) (*pb.LoadValueResponse, error)
}
