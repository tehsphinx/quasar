package transports

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/internal/inflight"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// fillConsumer fills ch to capacity, so the next hand-off onto it blocks
// until something drains it.
func fillConsumer(ch chan raft.RPC) {
	for len(ch) < cap(ch) {
		ch <- raft.RPC{}
	}
}

// serveCounted runs h for subj on a test connection of its own and reports
// every callback that returns. The transport's own subscriptions are torn
// down with its ctx, so they cannot show whether a callback came back.
func serveCounted(t *testing.T, subj string, h nats.MsgHandler) (*nats.Conn, <-chan struct{}) {
	t.Helper()

	nc, err := nats.Connect(natsURL, natsTestOptions()...)
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	t.Cleanup(nc.Close)

	returned := make(chan struct{}, 1)
	if _, err := nc.Subscribe(subj, func(msg *nats.Msg) {
		h(msg)
		returned <- struct{}{}
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return nc, returned
}

// awaitReturnAfterCancel asserts that the callback stays parked on the full
// consumer until cancel and comes back promptly after it.
func awaitReturnAfterCancel(t *testing.T, returned <-chan struct{}, cancel context.CancelFunc) {
	t.Helper()

	select {
	case <-returned:
		t.Fatal("callback returned before the cancel: it never blocked on the full consumer")
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("callback still blocked on the full consumer after the transport ctx was cancelled")
	}
}

// TestNATSTransport_HandoffReturnsOnShutdown is the RT-14146 regression test
// for rpcHandler: a raft or cache RPC that met a full consumer channel parked
// its subscription's dispatcher for good, even after the transport's ctx was
// cancelled.
func TestNATSTransport_HandoffReturnsOnShutdown(t *testing.T) {
	tests := []struct {
		name    string
		bounded bool
		handler func(ctx context.Context, trans *NATSTransport, sem *inflight.Sem) (chan raft.RPC, nats.MsgHandler)
		req     proto.Message
	}{
		{
			name: "raft-rpc",
			handler: func(ctx context.Context, trans *NATSTransport, _ *inflight.Sem) (chan raft.RPC, nats.MsgHandler) {
				return trans.chConsume, trans.handleVote(ctx)
			},
			req: pb.ToRequestVoteRequest(&raft.RequestVoteRequest{Term: 1}),
		},
		{
			name:    "cache-rpc",
			bounded: true,
			handler: func(ctx context.Context, trans *NATSTransport, sem *inflight.Sem) (chan raft.RPC, nats.MsgHandler) {
				return trans.chConsumeCache, trans.handleStore(ctx, sem)
			},
			req: &pb.Store{Key: "k", Data: []byte("x")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			transCtx, shutdown := context.WithCancel(ctx)
			defer shutdown()

			trans := newInflightTestTransport(transCtx, t, "handoff-"+tt.name)

			subj := "quasar.test.handoff." + tt.name
			var sem *inflight.Sem
			if tt.bounded {
				sem = inflight.NewRPC(newTestLogger(t), subj, 1)
			}
			ch, h := tt.handler(transCtx, trans, sem)
			fillConsumer(ch)
			nc, returned := serveCounted(t, subj, h)

			bts, err := proto.Marshal(tt.req)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if err := nc.PublishRequest(subj, nats.NewInbox(), bts); err != nil {
				t.Fatalf("publish: %v", err)
			}
			awaitReturnAfterCancel(t, returned, shutdown)

			if sem != nil && !sem.Acquire() {
				t.Fatal("the inflight slot taken by the abandoned RPC was never released")
			}
		})
	}
}

// TestNATSTransport_InstallSnapshotHandoffReturnsOnShutdown is the same
// RT-14146 regression test for handleInstallSnapshot, which hands the RPC off
// only after it has answered the request with the stream subject.
func TestNATSTransport_InstallSnapshotHandoffReturnsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	transCtx, shutdown := context.WithCancel(ctx)
	defer shutdown()

	trans := newInflightTestTransport(transCtx, t, "handoff-snapshot")
	fillConsumer(trans.chConsume)

	subj := "quasar.test.handoff.snapshot"
	nc, returned := serveCounted(t, subj, trans.handleInstallSnapshot(transCtx))

	bts, err := proto.Marshal(pb.ToInstallSnapshotRequest(&raft.InstallSnapshotRequest{Term: 1, Size: 1}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// The answer shows the callback got past opening the stream, on to the
	// hand-off.
	if _, err := nc.Request(subj, bts, natsTimeout); err != nil {
		t.Fatalf("open snapshot channel: %v", err)
	}
	awaitReturnAfterCancel(t, returned, shutdown)
}

// TestNATSTransport_HeartbeatFallbackBoundedWhenConsumerFull is the RT-14146
// regression test for the heartbeat fallback: with no raft bound and nobody
// draining chConsume, a beat parked the heartbeat dispatcher until something
// drained it, long after the sender had fallen back to entries.append. It is
// now answered with an error within the heartbeat window, with the transport
// still live.
func TestNATSTransport_HeartbeatFallbackBoundedWhenConsumerFull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	nc, err := nats.Connect(natsURL, natsTestOptions()...)
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()

	const hbTimeout = 300 * time.Millisecond
	trans, err := NewNATSTransport(ctx, nc, "test-cache", "handoff-hb",
		WithNATSLogger(newTestLogger(t)),
		WithNATSTimeout(natsTimeout),
		WithNATSHeartbeatTimeout(hbTimeout),
	)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	trans.SetHeartbeatHandler(nil)
	fillConsumer(trans.chConsume)

	bts, err := proto.Marshal(pb.ToAppendEntriesRequest(&raft.AppendEntriesRequest{
		Term:      10,
		RPCHeader: raft.RPCHeader{Addr: []byte("kenny")},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// A raw request: AppendEntries would fall back to entries.append, which
	// meets the same full channel.
	start := time.Now()
	resp, err := nc.Request("quasar.test-cache.handoff-hb.entries.heartbeat", bts, natsTimeout)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("heartbeat not answered: %v", err)
	}
	var protoResp pb.CommandResponse
	if err := proto.Unmarshal(resp.Data, &protoResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.Contains(protoResp.GetError(), "no raft bound") {
		t.Fatalf("expected a no-raft-bound error, got %q", protoResp.GetError())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("answered after %s with a heartbeat timeout of %s", elapsed, hbTimeout)
	}
}
