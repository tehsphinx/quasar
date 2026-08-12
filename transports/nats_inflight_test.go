package transports

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/pb/v1"
)

// newInflightTestTransport builds a transport against the test NATS server and
// closes its connections with the test.
func newInflightTestTransport(ctx context.Context, tb testing.TB, name string) *NATSTransport {
	tb.Helper()

	nc, err := nats.Connect(natsURL, nats.Timeout(natsTimeout))
	if err != nil {
		tb.Skipf("NATS not available: %v", err)
	}
	trans, err := NewNATSTransport(ctx, nc, "test-cache", name,
		WithNATSLogger(newTestLogger(tb)),
		WithNATSTimeout(natsTimeout),
	)
	if err != nil {
		tb.Fatalf("transport %s: %v", name, err)
	}
	tb.Cleanup(nc.Close)
	return trans
}

// drainCacheConsumer forwards the transport's cache RPCs to the returned
// channel, so a test can hold them without answering.
func drainCacheConsumer(ctx context.Context, trans *NATSTransport, buf int) <-chan raft.RPC {
	out := make(chan raft.RPC, buf)
	go func() {
		for {
			select {
			case rpc := <-trans.CacheConsumer():
				out <- rpc
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// TestNATSTransport_ForwardedStoresServedConcurrently is the RT-13899
// regression test. The subscription callback used to await the full raft
// commit and apply before returning, and nats.go serves one async
// subscription from a single goroutine — so a leader served every forwarded
// write in the cluster strictly one at a time. The barrier below cannot be
// reached that way: the second Store never arrives until the first is
// answered.
func TestNATSTransport_ForwardedStoresServedConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := newInflightTestTransport(ctx, t, "cs-leader")
	sender := newInflightTestTransport(ctx, t, "cs-sender")

	const concurrent = 8

	arrived := drainCacheConsumer(ctx, leader, concurrent)

	errs := make(chan error, concurrent)
	for i := 0; i < concurrent; i++ {
		go func(i int) {
			_, err := sender.Store(ctx, "leader", leader.LocalAddr(),
				&pb.Store{Key: strconv.Itoa(i), Data: []byte("payload")})
			errs <- err
		}(i)
	}

	held := make([]raft.RPC, 0, concurrent)
	for len(held) < concurrent {
		select {
		case rpc := <-arrived:
			held = append(held, rpc)
		case <-time.After(4 * time.Second):
			t.Fatalf("only %d of %d forwarded stores reached the leader: the subject is served serially",
				len(held), concurrent)
		}
	}

	for i, rpc := range held {
		rpc.Respond(&pb.StoreResponse{Uid: uint64(i + 1)}, nil)
	}
	for i := 0; i < concurrent; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("forwarded store failed: %v", err)
		}
	}
}

// TestNATSTransport_OverloadShedsWithExplicitError checks that a saturated
// subject answers instead of buffering: buffering is what filled the NATS
// client's per-subscription queue until it dropped silently, leaving callers
// to die on their request timeout with no signal on the leader.
func TestNATSTransport_OverloadShedsWithExplicitError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := newInflightTestTransport(ctx, t, "shed-leader")
	sender := newInflightTestTransport(ctx, t, "shed-sender")

	// A subject of its own with a single slot, so one held Store saturates it.
	const boundAddr = "shed-bound"
	subj := "quasar.test-cache." + boundAddr + ".cache.store"
	sub, err := leader.connRepl.Subscribe(subj,
		leader.handleStore(ctx, newInflightSem(newTestLogger(t), subj, 1)))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Subscribe does not wait for the server to register the SUB; without the
	// flush the first request can beat it and be delivered to nobody.
	if err = leader.connRepl.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	arrived := drainCacheConsumer(ctx, leader, 2)

	first := make(chan error, 1)
	go func() {
		_, storeErr := sender.Store(ctx, "leader", boundAddr, &pb.Store{Key: "held", Data: []byte("x")})
		first <- storeErr
	}()

	var held raft.RPC
	select {
	case held = <-arrived:
	case <-time.After(4 * time.Second):
		t.Fatal("first store never reached the leader")
	}

	start := time.Now()
	_, err = sender.Store(ctx, "leader", boundAddr, &pb.Store{Key: "shed", Data: []byte("x")})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the saturated subject to shed the second store")
	}
	if !strings.Contains(err.Error(), "leader overloaded") {
		t.Fatalf("expected an overload error, got: %v", err)
	}
	if elapsed > natsTimeout/2 {
		t.Fatalf("shed took %s: the request timed out instead of being answered", elapsed)
	}

	held.Respond(&pb.StoreResponse{Uid: 1}, nil)
	if err := <-first; err != nil {
		t.Fatalf("held store failed: %v", err)
	}
}

// TestNATSTransport_RaftRPCsStayOneAtATime pins the other half of the change:
// raft RPCs keep blocking their callback. raft drains its consumer serially
// and depends on the arrival order per peer, so entries.append must not gain
// the concurrency the cache subjects did.
func TestNATSTransport_RaftRPCsStayOneAtATime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := newInflightTestTransport(ctx, t, "serial-leader")

	var (
		inFlight atomic.Int32
		overlap  atomic.Bool
	)
	go func() {
		for {
			select {
			case rpc := <-leader.Consumer():
				if inFlight.Add(1) > 1 {
					overlap.Store(true)
				}
				time.Sleep(20 * time.Millisecond)
				inFlight.Add(-1)
				rpc.Respond(&raft.AppendEntriesResponse{Success: true}, nil)
			case <-ctx.Done():
				return
			}
		}
	}()

	const senders = 3
	errs := make(chan error, senders)
	for i := 0; i < senders; i++ {
		sender := newInflightTestTransport(ctx, t, fmt.Sprintf("serial-sender-%d", i))
		go func() {
			args := makeAppendRPC()
			var out raft.AppendEntriesResponse
			errs <- sender.AppendEntries("leader", leader.LocalAddr(), &args, &out)
		}()
	}
	for i := 0; i < senders; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("append entries failed: %v", err)
		}
	}

	if overlap.Load() {
		t.Fatal("raft RPCs were served concurrently: entries.append lost its arrival ordering")
	}
}

// BenchmarkNATSForwardedStores measures forwarded-write throughput against a
// fixed synthetic apply latency, with the subject served one at a time (the
// pre-RT-13899 behaviour, sem == nil) and served concurrently. Run it with an
// explicit iteration count, e.g.
//
//	go test ./transports -run '^$' -bench BenchmarkNATSForwardedStores -benchtime 500x
func BenchmarkNATSForwardedStores(b *testing.B) {
	const (
		callers      = 32
		applyLatency = 2 * time.Millisecond
	)

	for _, bc := range []struct {
		name     string
		inflight int // 0 keeps the callback synchronous
	}{
		{name: "serial", inflight: 0},
		{name: "concurrent", inflight: maxInflightCacheRPCs},
	} {
		b.Run(bc.name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			leader := newInflightTestTransport(ctx, b, "bench-leader-"+bc.name)
			sender := newInflightTestTransport(ctx, b, "bench-sender-"+bc.name)

			addr := raft.ServerAddress("bench-" + bc.name)
			subj := "quasar.test-cache." + string(addr) + ".cache.store"
			var sem *inflightSem
			if bc.inflight > 0 {
				sem = newInflightSem(newTestLogger(b), subj, bc.inflight)
			}
			sub, err := leader.connRepl.Subscribe(subj, leader.handleStore(ctx, sem))
			if err != nil {
				b.Fatalf("subscribe: %v", err)
			}
			if err = leader.connRepl.Flush(); err != nil {
				b.Fatalf("flush: %v", err)
			}
			defer func() { _ = sub.Unsubscribe() }()

			// Stands in for Cache.consume, which dispatches every cache RPC on
			// its own goroutine, plus a fixed commit-and-apply cost.
			go func() {
				var uid atomic.Uint64
				for {
					select {
					case rpc := <-leader.CacheConsumer():
						go func(rpc raft.RPC) {
							time.Sleep(applyLatency)
							rpc.Respond(&pb.StoreResponse{Uid: uid.Add(1)}, nil)
						}(rpc)
					case <-ctx.Done():
						return
					}
				}
			}()

			work := make(chan int, callers)
			errs := make(chan error, callers)
			for i := 0; i < callers; i++ {
				go func() {
					for n := range work {
						if _, err := sender.Store(ctx, "leader", addr,
							&pb.Store{Key: strconv.Itoa(n), Data: []byte("payload")}); err != nil {
							errs <- err
							return
						}
					}
					errs <- nil
				}()
			}

			b.ResetTimer()
			start := time.Now()
			for n := 0; n < b.N; n++ {
				work <- n
			}
			close(work)
			for i := 0; i < callers; i++ {
				if err := <-errs; err != nil {
					b.Fatalf("store failed: %v", err)
				}
			}
			elapsed := time.Since(start)
			b.StopTimer()

			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "stores/s")
		})
	}
}
