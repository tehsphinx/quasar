// Package transports contains different transport implementations for the quasar cache.
package transports

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
)

const (
	defaultTimout    = 5 * time.Second
	consumerChanSize = 16
)

// Transport defines an extended raft.Transport with additional commands for the cache.
type Transport interface {
	raft.Transport

	// CacheConsumer returns the cache consumer channel to which all incoming cache commands are sent.
	CacheConsumer() <-chan raft.RPC

	// Store asks the master to apply a change command to the raft cluster.
	Store(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.Store) (*pb.StoreResponse, error)

	// ResetCache asks the master to reset the cache.
	ResetCache(ctx context.Context, _ raft.ServerID, address raft.ServerAddress, request *pb.ResetCache) (*pb.ResetCacheResponse, error)

	// LatestUID asks the master to return its latest known / generated uid.
	LatestUID(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.LatestUid) (*pb.LatestUidResponse, error)
}

func snapshotTimeout(origTimeout time.Duration, size int64) time.Duration {
	timeout := origTimeout * time.Duration(size/int64(raft.DefaultTimeoutScale))
	if timeout < origTimeout {
		return origTimeout
	}
	return timeout
}
