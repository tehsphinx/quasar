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
// existing cluster is already out there (see Cache.bootstrap),
// reinitRaftAdoptingInstance rejoins as a follower and relies on the leader's
// heartbeats, and the quorum recovery path has already populated the stores
// via raft.RecoverCluster.
func newRaft(cfg options, fsm raft.FSM, logStore raft.LogStore, stableStore raft.StableStore,
	snapshotStore raft.SnapshotStore, transport transports.Transport,
) (*raft.Raft, error) {
	conf := raftConfig(cfg)
	rft, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapshotStore, hideClose(transport))
	if err != nil {
		return nil, fmt.Errorf("failed to create raft layer: %w", err)
	}
	return rft, nil
}

// hideClose wraps transport so that raft cannot close it, keeping pre-vote
// support when the transport has it.
//
// raft's shutdownFuture.Error() is the only way to join the run / runFSM /
// runSnapshots goroutines, and it also calls Close() on a transport that
// satisfies raft.WithClose (raft@v1.7.3 future.go) — InmemTransport.Close
// disconnects every peer, TCPTransport.Close shuts the listener down for good.
// The rebuild paths (reinitRaftAdoptingInstance, recoverQuorum) put a new raft
// on the SAME transport, so they must be able to await the old instance without
// losing it: hidden from raft, Close() is never called and the wait is safe
// (RT-14147). The transport is closed by Cache.shutdown instead.
func hideClose(transport transports.Transport) raft.Transport {
	// transports.Transport does not declare RequestPreVote, so a wrapper that
	// only embeds it would silently drop the raft.WithPreVote capability of the
	// TCP, NATS and Inmem transports and raft would disable pre-vote
	// (raft@v1.7.3 api.go). Satisfying WithPreVote unconditionally is worse: a
	// stub that errors is recorded as a denied vote and stalls pre-vote quorum.
	if preVote, ok := transport.(raft.WithPreVote); ok {
		return noClosePreVoteTransport{Transport: transport, WithPreVote: preVote}
	}
	return noCloseTransport{Transport: transport}
}

// noCloseTransport is a transport whose Close(), if it has one, is invisible to
// raft. See hideClose.
type noCloseTransport struct {
	transports.Transport
}

// noClosePreVoteTransport is noCloseTransport for a transport that supports
// pre-vote. See hideClose.
type noClosePreVoteTransport struct {
	transports.Transport
	raft.WithPreVote
}

// raftConfig returns the raft.Config to use for this cache, applying the
// quasar-level overrides on top of the user-supplied or default raft config.
//
// When the caller did not supply a *raft.Config, PreVote is forced off to keep
// behaviour stable on mixed-version clusters (peers without the prevote subject
// would respond with nats.ErrNoResponders, which raft v1.7.x records as
// Granted=false, preventing pre-vote quorum). Operators that want PreVote on
// pass a *raft.Config via WithRaftConfig with PreVoteDisabled left at its
// zero value (false) — and do so only once every voter is on a build that
// implements the WithPreVote transport.
func raftConfig(cfg options) *raft.Config {
	conf := cfg.raftConfig
	if conf == nil {
		conf = raft.DefaultConfig()
		conf.PreVoteDisabled = true
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
