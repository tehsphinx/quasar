package quasar

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// newRaft creates a fresh *raft.Raft from the given stores without bootstrapping
// or applying any discovery-driven configuration. Bootstrap of the initial
// cluster configuration is intentionally deferred to the caller: newCache drives
// the bootstrap decision after discovery has had a chance to learn whether an
// existing cluster is already out there (see Cache.bootstrap), localReset
// rejoins as a follower and relies on the leader's heartbeats, and the quorum
// recovery path has already populated the stores via raft.RecoverCluster.
func newRaft(cfg options, fsm raft.FSM, logStore raft.LogStore, stableStore raft.StableStore,
	snapshotStore raft.SnapshotStore, transport transports.Transport,
) (*raft.Raft, error) {
	conf := raftConfig(cfg)
	rft, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft layer: %w", err)
	}
	return rft, nil
}

// raftConfig returns the raft.Config to use for this cache, applying the
// quasar-level overrides on top of the user-supplied or default raft config.
func raftConfig(cfg options) *raft.Config {
	conf := cfg.raftConfig
	if conf == nil {
		conf = raft.DefaultConfig()
	}
	conf.LocalID = raft.ServerID(cfg.localID)
	conf.Logger = cfg.getLogger()
	return conf
}

func getTransport(ctx context.Context, cfg options) (transports.Transport, error) {
	if cfg.transport != nil {
		return cfg.transport, nil
	}
	if cfg.nc != nil {
		return transports.NewNATSTransport(ctx, cfg.nc, cfg.cacheName, cfg.localID)
	}

	if cfg.bindAddr != "" {
		return transports.NewTCPTransport(ctx, cfg.bindAddr, cfg.extAddr)
	}

	_, inMemTransport := transports.NewInmemTransport("")
	return inMemTransport, nil
}
