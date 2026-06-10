// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// TestMultipartAssembler_HeaderlessIgnoresStaleAssembly is part of the
// RT-13042 M2 regression. A sender that dies mid-multipart leaves a partial
// assembly behind. The next standalone (headerless) message used to be
// appended onto that stale partial payload, producing a corrupt frame. A
// headerless message must be delivered untouched, regardless of any
// assembly in progress.
func TestMultipartAssembler_HeaderlessIgnoresStaleAssembly(t *testing.T) {
	assembler := newMultipartAssembler()

	// Part 1 of a multipart request whose sender dies before the final part.
	part := nats.NewMsg("test.subject")
	part.Header.Set("request_id", "sender-a/42")
	part.Header.Set("pkg_part", "1")
	part.Data = []byte("stale-partial-")

	data, complete, err := assembler.handle(part)
	if err != nil {
		t.Fatalf("part 1: %v", err)
	}
	if complete {
		t.Fatalf("part 1 unexpectedly reported complete with data %q", data)
	}

	// A standalone single-part request from another sender arrives next.
	standalone := nats.NewMsg("test.subject")
	standalone.Reply = "reply.inbox"
	standalone.Data = []byte("standalone-payload")

	data, complete, err = assembler.handle(standalone)
	if err != nil {
		t.Fatalf("standalone: %v", err)
	}
	if !complete {
		t.Fatal("standalone message not reported complete")
	}
	if !bytes.Equal(data, standalone.Data) {
		t.Fatalf("stale partial assembly glued onto standalone message: got %q, want %q", data, standalone.Data)
	}
}

// TestMultipartAssembler_InterleavedSenders is the RT-13042 M3 unit
// regression: parts of two requests arriving interleaved (as concurrent
// forwarders to one leader subject produce) must reassemble independently.
func TestMultipartAssembler_InterleavedSenders(t *testing.T) {
	assembler := newMultipartAssembler()

	mkPart := func(id, part, data string, final bool) *nats.Msg {
		msg := nats.NewMsg("test.subject")
		msg.Header.Set("request_id", id)
		msg.Header.Set("pkg_part", part)
		msg.Data = []byte(data)
		if final {
			msg.Reply = "reply.inbox"
		}
		return msg
	}

	steps := []struct {
		msg  *nats.Msg
		want string // expected payload when complete; "" = not complete
	}{
		{mkPart("a/1", "1", "aaa-", false), ""},
		{mkPart("b/1", "1", "bbb-", false), ""},
		{mkPart("a/1", "2", "AAA", true), "aaa-AAA"},
		{mkPart("b/1", "2", "BBB", true), "bbb-BBB"},
	}
	for i, step := range steps {
		data, complete, err := assembler.handle(step.msg)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if (step.want != "") != complete {
			t.Fatalf("step %d: complete = %v, want %v", i, complete, step.want != "")
		}
		if step.want != "" && string(data) != step.want {
			t.Fatalf("step %d: assembled %q, want %q", i, data, step.want)
		}
	}
}

// TestNATSTransport_ConcurrentMultipartStores is the RT-13042 M3 regression
// test. The Store handler used to share a single reassembly buffer across all
// senders — justified for AppendEntries (one leader, sequential) but wrong
// for Store: any node, and multiple goroutines per node, forwards large
// Stores to the same leader subject. Worse, the multipart request_id was a
// bare per-transport counter starting at 0, so two senders simultaneously
// used the same ID. Interleaved parts then corrupted each other's assembly
// and concurrent large Stores failed persistently. With reassembly keyed by
// the sender-qualified request ID, all concurrent large Stores must arrive
// intact.
func TestNATSTransport_ConcurrentMultipartStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	newTrans := func(name string) *NATSTransport {
		nc, err := nats.Connect(natsURL, nats.Timeout(natsTimeout))
		if err != nil {
			t.Skipf("NATS not available: %v", err)
		}
		trans, err := NewNATSTransport(ctx, nc, "test-cache", name,
			WithNATSLogger(newTestLogger(t)),
			WithNATSTimeout(natsTimeout),
			WithNATSMaxMsgSize(1024),
		)
		if err != nil {
			t.Fatalf("transport %s: %v", name, err)
		}
		t.Cleanup(nc.Close)
		return trans
	}

	leader := newTrans("cms-leader")
	senderA := newTrans("cms-sender-a")
	senderB := newTrans("cms-sender-b")

	// Leader side: drain the cache consumer, verify each Store payload is
	// uniform (uncorrupted) and respond.
	go func() {
		var uid uint64
		for {
			select {
			case rpc := <-leader.CacheConsumer():
				store, ok := rpc.Command.(*pb.Store)
				if !ok {
					rpc.Respond(nil, errors.New("unexpected command type"))
					continue
				}
				if err := verifyUniformPayload(store); err != nil {
					rpc.Respond(nil, err)
					continue
				}
				uid++
				rpc.Respond(&pb.StoreResponse{Uid: uid}, nil)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Two senders push large (multipart) Stores to the same leader subject
	// concurrently so their parts interleave on the wire.
	const perSender = 10
	run := func(sender *NATSTransport, fill byte) error {
		for i := 0; i < perSender; i++ {
			store := &pb.Store{
				Key:  string(fill),
				Data: bytes.Repeat([]byte{fill}, 4*1024),
			}
			if _, err := sender.Store(ctx, "leader", leader.LocalAddr(), store); err != nil {
				return fmt.Errorf("sender %c store %d: %w", fill, i, err)
			}
		}
		return nil
	}

	errs := make(chan error, 2)
	go func() { errs <- run(senderA, 'a') }()
	go func() { errs <- run(senderB, 'b') }()

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent multipart stores failed: %v", err)
		}
	}
}

// verifyUniformPayload checks that the Store payload consists exclusively of
// the byte named by its key — any foreign byte means parts of another
// sender's request were glued into this assembly.
func verifyUniformPayload(store *pb.Store) error {
	if len(store.Key) != 1 || len(store.Data) != 4*1024 {
		return fmt.Errorf("unexpected store shape: key %q, %d bytes", store.Key, len(store.Data))
	}
	fill := store.Key[0]
	for i, b := range store.Data {
		if b != fill {
			return fmt.Errorf("corrupt payload for key %q: byte %d is %q", store.Key, i, b)
		}
	}
	return nil
}

// TestNATSTransport_MultipartFinalPartCarriesHeaders is the RT-13042 M2
// regression test. request() built the final part of a multipart request with
// request_id / pkg_part headers but then sent the raw payload bytes without
// them — so the receiver appended the final part unconditionally, with no way
// to validate the assembly or detect a lost middle part. The final part must
// arrive carrying the same headers as the parts published before it.
func TestNATSTransport_MultipartFinalPartCarriesHeaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, err := nats.Connect(natsURL, nats.Timeout(natsTimeout))
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	trans, err := NewNATSTransport(ctx, nc, "test-cache", "mp-server1",
		WithNATSLogger(newTestLogger(t)),
		WithNATSTimeout(natsTimeout),
		WithNATSMaxMsgSize(1024),
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans.conn.Close()

	const subj = "test.multipart.headers"

	var (
		mu       sync.Mutex
		received []*nats.Msg
	)
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
		if msg.Reply != "" {
			bts, _ := proto.Marshal(&pb.CommandResponse{})
			_ = msg.Respond(bts)
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	// Payload well above maxMsgSize so request() takes the multipart path.
	req := &pb.Store{Key: "big", Data: bytes.Repeat([]byte("x"), 4*1024)}
	var resp pb.CommandResponse
	if _, err := trans.request(ctx, subj, req, &resp); err != nil {
		t.Fatalf("request: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) < 2 {
		t.Fatalf("expected a multipart send, got %d message(s)", len(received))
	}

	final := received[len(received)-1]
	if final.Reply == "" {
		t.Fatalf("last received message is not the request part")
	}
	if final.Header.Get("request_id") == "" || final.Header.Get("pkg_part") == "" {
		t.Fatalf("final multipart part arrived without request_id/pkg_part headers: %v", final.Header)
	}
	wantID := received[0].Header.Get("request_id")
	if got := final.Header.Get("request_id"); got != wantID {
		t.Fatalf("final part request_id = %q, want %q (same assembly as earlier parts)", got, wantID)
	}
}
