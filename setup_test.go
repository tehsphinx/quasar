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

func getRaft(port int) (*raft.Raft, error) {
	extAddr := &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 28224,
	}
	bindAddr := ":" + strconv.Itoa(extAddr.Port)

	conf := &raft.Config{
		ProtocolVersion:    3,
		LocalID:            "main",
		HeartbeatTimeout:   10 * time.Second,
		ElectionTimeout:    10 * time.Second,
		CommitTimeout:      10 * time.Second,
		MaxAppendEntries:   5,
		SnapshotInterval:   30 * time.Minute,
		LeaderLeaseTimeout: 10 * time.Second,
	}

	transport, err := raft.NewTCPTransport(bindAddr, extAddr, 10, 5*time.Second, os.Stdout)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate raft transport: %w", err)
	}

	fsm := NewFSM()
	logStore := raft.NewInmemStore()
	snapshotStore := raft.NewDiscardSnapshotStore()
	stableStore := store.NewStableInMemory()

	rft, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft layer: %w", err)
	}

	return rft, nil
}
