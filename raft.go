package quasar

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

func getRaft(cfg options, fsm raft.FSM, logStore raft.LogStore, transport transports.Transport, discovery *DiscoveryInjector) (*raft.Raft, error) {
	snapshotStore := raft.NewInmemSnapshotStore()
	stableStore := stores.NewStableInMemory()

	conf := cfg.raftConfig
	if conf == nil {
		conf = raft.DefaultConfig()
	}
	conf.LocalID = raft.ServerID(cfg.localID)

	rft, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft layer: %w", err)
	}

	if cfg.discovery != nil {
		cfg.servers = discovery.getServers()
		cfg.bootstrap = true
	}

	if cfg.bootstrap && len(cfg.servers) == 0 {
		cfg.servers = []raft.Server{
			{
				ID:      conf.LocalID,
				Address: transport.LocalAddr(),
			},
		}
	}
	if len(cfg.servers) != 0 {
		rft.BootstrapCluster(raft.Configuration{
			Servers: cfg.servers,
		})
	}

	return rft, nil
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
