// Package transports contains different transport implementations for the quasar cache.
package transports

import (
	"context"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
)

type Transport interface {
	raft.Transport

	CacheConsumer() <-chan raft.RPC

	Store(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.Store) (*pb.StoreResponse, error)
	LatestUID(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.LatestUid) (*pb.LatestUidResponse, error)
}
