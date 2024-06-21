package quasar

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

func getRaft(ctx context.Context, cfg options, fsm raft.FSM, transport transports.Transport) (*raft.Raft, error) {
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

	if cfg.discovery != nil {
		exists, e := clusterExists(ctx, cfg, transport)
		fmt.Println("clusterExists", exists, e)
		if e != nil {
			return nil, e
		}
		if exists {
			// No need to bootstrap. We will get invited to cluster.
			return rft, nil
		}
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

func clusterExists(ctx context.Context, cfg options, transport transports.Transport) (bool, error) {
	err := cfg.discovery.PingCluster(ctx, raft.ServerID(cfg.localID), transport.LocalAddr(), cfg.isVoter)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			// no responders -> no other servers
			return false, nil
		}
		if errors.Is(err, context.DeadlineExceeded) {
			// no response -> no other servers
			return false, nil
		}
		// other failure
		return false, fmt.Errorf("failed to ping cache cluster: %w", err)
	}
	return true, nil
}

func getTransport(cfg options) (transports.Transport, error) {
	// TODO: set name on transport so it doesn't need to be added in NewTransport.
	if cfg.transport != nil {
		return cfg.transport, nil
	}

	return transports.NewTCPTransport(cfg.bindAddr, cfg.extAddr)
}
