package quasar

import (
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

func getRaft(cfg options, fsm raft.FSM, transport transports.Transport) (*raft.Raft, error) {
	if cfg.raft != nil {
		return cfg.raft, nil
	}

	snapshotStore := raft.NewDiscardSnapshotStore()
	logStore := raft.NewInmemStore()
	stableStore := stores.NewStableInMemory()

	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(cfg.localID)

	rft, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft layer: %w", err)
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

func getTransport(cfg options) (transports.Transport, error) {
	if cfg.transport != nil {
		return cfg.transport, nil
	}

	return transports.NewTCPTransport(cfg.bindAddr, cfg.extAddr)
}
