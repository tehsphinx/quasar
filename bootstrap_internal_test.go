package quasar

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// syncBuffer is a goroutine-safe io.Writer so raft's background logging
// goroutines and the test goroutine can share one log sink without racing.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// noopDiscovery is a do-nothing Discovery that just enables the
// discovery-voter bootstrap branch.
type noopDiscovery struct{}

func (noopDiscovery) Inject(*DiscoveryInjector) {}
func (noopDiscovery) Run(context.Context) error { return nil }

// announcingDiscovery enables the discovery-voter bootstrap branch and, when
// Run is called (inside NewCache, before bootstrap), synchronously announces a
// single peer with the given status. With NumVotersInConfig>=2 or HasLeader it
// closes peerInClusterCh, so the local voter skips its single-node bootstrap —
// the precondition for the RT-13067 liveness fallback.
type announcingDiscovery struct {
	inj    *DiscoveryInjector
	peerID raft.ServerID
	status PeerStatus
}

func (d *announcingDiscovery) Inject(inj *DiscoveryInjector) { d.inj = inj }

func (d *announcingDiscovery) Run(context.Context) error {
	// The peer is not in our (empty) config and we are not leader, so the
	// addServer attempt inside ProcessServerWithStatus fails fast and harmlessly
	// — but peerInClusterCh / sawLeaderPing are updated first, which is all this
	// test needs.
	d.inj.ProcessServerWithStatus(
		raft.Server{ID: d.peerID, Address: raft.ServerAddress(d.peerID), Suffrage: raft.Voter},
		d.status,
	)
	return nil
}

// TestBootstrapFallbackRecoversFromLeaderlessEstablishedCluster is the RT-13067
// regression test for the bootstrap-gating liveness fallback. A restarting
// voter is told (via a stale-config peer reporting NumVotersInConfig=2, no
// leader) that an established cluster exists, so it skips its own bootstrap.
// Pre-fix this is terminal: configless, leaderless, crash-looping. The fallback
// must detect that no real leader ever formed and bootstrap a single-voter
// cluster to break the wedge.
func TestBootstrapFallbackRecoversFromLeaderlessEstablishedCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out := &syncBuffer{}
	logger := hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})

	disc := &announcingDiscovery{
		peerID: "stale-peer",
		status: PeerStatus{HasLeader: false, NumVotersInConfig: 2},
	}

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, newSnapFSM(),
		WithLocalID("voter"),
		WithTransport(tr),
		WithDiscovery(disc),
		WithBootstrapFallbackWait(300*time.Millisecond),
		WithHclogLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	// Bootstrap was skipped: the node starts configless and leaderless.
	if !c.hasEmptyConfig() {
		t.Fatalf("expected an empty config right after a skipped bootstrap")
	}

	// The fallback must bootstrap a single-voter cluster and win the election.
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady after fallback: %v\nlog:\n%s", err, out.String())
	}
	if !c.IsLeader() {
		t.Fatalf("expected the node to be leader of its fallback single-voter cluster")
	}
	if !strings.Contains(out.String(), "bootstrap fallback") {
		t.Fatalf("expected a bootstrap-fallback log, got:\n%s", out.String())
	}
}

// TestBootstrapFallbackSkippedWhenLeaderObserved is the RT-13067 safety check:
// the fallback must NOT fire — and must not fork a competing cluster — when a
// peer reports a live leader (HasLeader=true). The real leader is expected to
// re-admit this node; the local voter stays configless, waiting, until that
// happens.
func TestBootstrapFallbackSkippedWhenLeaderObserved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out := &syncBuffer{}
	logger := hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})

	disc := &announcingDiscovery{
		peerID: "live-peer",
		status: PeerStatus{HasLeader: true, NumVotersInConfig: 2},
	}

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, newSnapFSM(),
		WithLocalID("voter"),
		WithTransport(tr),
		WithDiscovery(disc),
		WithBootstrapFallbackWait(300*time.Millisecond),
		WithHclogLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	// Wait well past the fallback window, then confirm it did NOT fire.
	time.Sleep(1 * time.Second)

	if c.IsLeader() {
		t.Fatalf("fallback forked a competing cluster despite an observed leader")
	}
	if !c.hasEmptyConfig() {
		t.Fatalf("node left its empty config without being admitted by a real leader")
	}
	if strings.Contains(out.String(), "bootstrap fallback") {
		t.Fatalf("fallback fired despite an observed live leader:\n%s", out.String())
	}
}

// TestBootstrapWarnsWhenServersIgnoredForDiscoveryVoter is the L5 regression
// test. A discovery-enabled voter decides bootstrapping on its own, silently
// ignoring an explicit WithServers/WithBootstrap. The misconfiguration must
// now be logged rather than producing unexplained behavior.
func TestBootstrapWarnsWhenServersIgnoredForDiscoveryVoter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out := &syncBuffer{}
	logger := hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, newSnapFSM(),
		WithLocalID("voter"),
		WithTransport(tr),
		WithDiscovery(noopDiscovery{}),
		WithBootstrap(true),
		WithBootstrapWait(0),
		WithHclogLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if !strings.Contains(out.String(), "WithServers/WithBootstrap ignored") {
		t.Fatalf("expected an ignored-options warning, got:\n%s", out.String())
	}
}

// TestBootstrapClusterLogsFutureError is the RT-13042 m2 regression test.
// BootstrapCluster futures used to be discarded, silently swallowing failures.
// A second bootstrap of an already-bootstrapped raft fails with
// raft.ErrCantBootstrap and must now be logged.
func TestBootstrapClusterLogsFutureError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out := &syncBuffer{}
	logger := hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, newSnapFSM(),
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
		WithHclogLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	// Already bootstrapped: this second attempt fails and must be logged.
	c.bootstrapCluster(c.raft(), raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID(c.localID), Address: tr.LocalAddr()}},
	})

	if !strings.Contains(out.String(), "bootstrap cluster failed") {
		t.Fatalf("expected a bootstrap-failure log, got:\n%s", out.String())
	}
}
