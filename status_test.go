package quasar_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/matryer/is"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/examples/generic/exampleFSM"
	"github.com/tehsphinx/quasar/transports"
)

// TestRaftStatsCarriesLivenessKeys covers RT-13896: the keys gatex scrapes as
// raft liveness gauges must be present in the raw stats map.
func TestRaftStatsCarriesLivenessKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	asrt := is.New(t)

	addr, transport := transports.NewInmemTransport("")
	cache, err := quasar.NewCache(ctx, exampleFSM.NewInMemoryFSM(),
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport),
		quasar.WithServers([]raft.Server{{ID: "cache1", Address: addr, Suffrage: raft.Voter}}),
	)
	asrt.NoErr(err)
	defer cache.Shutdown()

	asrt.NoErr(cache.WaitReady(ctx))

	stats := cache.RaftStats()
	for _, key := range []string{
		"last_log_index", "last_snapshot_index", "applied_index", "commit_index", "fsm_pending",
	} {
		if _, ok := stats[key]; !ok {
			t.Errorf("RaftStats is missing %q, got %v", key, stats)
		}
	}
}

// TestGetRaftStatusHealthyOnFollower is the RT-13042 H3 regression test.
// GetRaftStatus used VerifyLeader on every node, but followers answer the
// verify future with ErrNotLeader (raft v1.7.3), so a healthy follower always
// reported LeaderHealthy == false and therefore Healthy == false. Followers
// must derive leader health from leader-contact recency instead.
func TestGetRaftStatusHealthyOnFollower(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	asrt := is.New(t)

	addr1, transport1 := transports.NewInmemTransport("")
	addr2, transport2 := transports.NewInmemTransport("")
	transport1.Connect(addr2, transport2)
	transport2.Connect(addr1, transport1)

	servers := []raft.Server{
		{ID: "cache1", Address: addr1, Suffrage: raft.Voter},
		{ID: "cache2", Address: addr2, Suffrage: raft.Voter},
	}

	cache1, err := quasar.NewCache(ctx, exampleFSM.NewInMemoryFSM(),
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctx, exampleFSM.NewInMemoryFSM(),
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer cache2.Shutdown()

	asrt.NoErr(cache1.WaitReady(ctx))
	asrt.NoErr(cache2.WaitReady(ctx))

	// Both nodes — the leader and the follower — must report a healthy
	// cluster. Polled because a follower's LastContact only fills in with
	// the first post-election heartbeat.
	err = waitForCondition(ctx, func() error {
		for _, c := range []*quasar.Cache{cache1, cache2} {
			st := c.GetRaftStatus()
			if !st.HasLeader {
				return fmt.Errorf("node %s sees no leader", st.LocalID)
			}
			if !st.LeaderHealthy {
				return fmt.Errorf("node %s (leader=%v) reports LeaderHealthy=false", st.LocalID, st.IsLeader)
			}
			if !st.Healthy {
				return fmt.Errorf("node %s (leader=%v) reports Healthy=false", st.LocalID, st.IsLeader)
			}
		}
		return nil
	})
	asrt.NoErr(err)
}
