// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// TestHandleMultiPart_HeaderlessResetsStaleAssembly is part of the RT-13042 M2
// regression. A sender that dies mid-multipart leaves a partial assembly in
// the receiver's buffer. The next standalone (headerless) message used to be
// appended onto that stale partial payload, producing a corrupt frame. A
// headerless arrival while an assembly is in progress must drop the stale
// assembly and deliver only the standalone message.
func TestHandleMultiPart_HeaderlessResetsStaleAssembly(t *testing.T) {
	var message Message

	// Part 1 of a multipart request whose sender dies before the final part.
	part := nats.NewMsg("test.subject")
	part.Header = nats.Header{}
	part.Header.Set("request_id", "42")
	part.Header.Set("pkg_part", "1")
	part.Data = []byte("stale-partial-")

	complete, err := handleMultiPart(part, &message)
	if err != nil {
		t.Fatalf("part 1: %v", err)
	}
	if complete {
		t.Fatal("part 1 unexpectedly reported complete")
	}

	// A standalone single-part request from another sender arrives next.
	standalone := nats.NewMsg("test.subject")
	standalone.Reply = "reply.inbox"
	standalone.Data = []byte("standalone-payload")

	complete, err = handleMultiPart(standalone, &message)
	if err != nil {
		t.Fatalf("standalone: %v", err)
	}
	if !complete {
		t.Fatal("standalone message not reported complete")
	}

	got := message.GetDataAndReset()
	if !bytes.Equal(got, standalone.Data) {
		t.Fatalf("stale partial assembly glued onto standalone message: got %q, want %q", got, standalone.Data)
	}
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
