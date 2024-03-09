package quasar

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/store"
)

func getRaft(cfg options) (*raft.Raft, error) {
	if cfg.raft != nil {
		return cfg.raft, nil
	}

	return getTCPRaft(cfg.tcpPort)
}

// TODO: generalize
func getTCPRaft(port int) (*raft.Raft, error) {
	extAddr := &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: port,
	}
	bindAddr := ":" + strconv.Itoa(extAddr.Port)

	conf := raft.DefaultConfig()
	conf.LocalID = "main"

	transport, err := raft.NewTCPTransport(bindAddr, extAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate raft transport (%v): %w", extAddr, err)
	}

	fsm := NewFSM()
	snapshotStore := raft.NewDiscardSnapshotStore()
	logStore := raft.NewInmemStore()
	stableStore := store.NewStableInMemory()

	rft, err := raft.NewRaft(conf, wrapFSM(fsm), logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft layer: %w", err)
	}
	rft.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      conf.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	})

	return rft, nil
}
