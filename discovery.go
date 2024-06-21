package quasar

import (
	"context"

	"github.com/hashicorp/raft"
)

type Discovery interface {
	Run(ctx context.Context) error
	Inject(cache *DiscoveryInjector)
	PingCluster(ctx context.Context, id raft.ServerID, addr raft.ServerAddress, voter bool) error
}
