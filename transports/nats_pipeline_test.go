// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// stubFollower serves entries.append for one address on its own connection, so
// a test can drop, delay or reorder replies the way a lost message or a
// reconnect would — things a real receiving transport never does on purpose.
type stubFollower struct {
	address string
	chReq   chan stubRequest
}

// stubRequest is one request with the moment it arrived, so a test can delay
// its reply by a fixed span measured from arrival rather than from the moment
// the test got around to it.
type stubRequest struct {
	msg *nats.Msg
	at  time.Time
}

func newStubFollower(t *testing.T, address string) *stubFollower {
	t.Helper()

	conn, err := nats.Connect(natsURL, natsTestOptions()...)
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	t.Cleanup(conn.Close)

	stub := &stubFollower{address: address, chReq: make(chan stubRequest, 256)}
	sub, err := conn.Subscribe("quasar.test-cache."+address+".entries.append", func(msg *nats.Msg) {
		stub.chReq <- stubRequest{msg: msg, at: time.Now()}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return stub
}

// next returns the next request the stub received.
func (s *stubFollower) next(t *testing.T) *nats.Msg {
	t.Helper()

	select {
	case req := <-s.chReq:
		return req.msg
	case <-time.After(2 * time.Second):
		t.Fatal("no request reached the follower")
		return nil
	}
}

// respond answers one request with lastLog, which the test uses to tell
// responses apart.
func (s *stubFollower) respond(t *testing.T, msg *nats.Msg, lastLog uint64) {
	t.Helper()

	resp := &raft.AppendEntriesResponse{Term: 4, LastLog: lastLog, Success: true}
	bts, err := proto.Marshal(&pb.CommandResponse{
		Resp: &pb.CommandResponse_AppendEntries{AppendEntries: pb.ToAppendEntriesResponse(resp)},
	})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	if err := msg.Respond(bts); err != nil {
		t.Fatalf("respond: %v", err)
	}
}

// makePipeline builds a transport and a pipeline aimed at address.
func makePipeline(ctx context.Context, t *testing.T, serverName, address string) raft.AppendPipeline {
	t.Helper()

	trans, err := makeNATSTransport(ctx, t, "test-cache", serverName)
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	t.Cleanup(trans.conn.Close)

	pipe, err := trans.AppendEntriesPipeline("id1", raft.ServerAddress(address))
	if err != nil {
		t.Fatalf("AppendEntriesPipeline: %v", err)
	}
	t.Cleanup(func() { _ = pipe.Close() })
	return pipe
}

// appendAt sends one pipelined request whose PrevLogEntry identifies it.
func appendAt(t *testing.T, pipe raft.AppendPipeline, index uint64) {
	t.Helper()

	args := makeAppendRPC()
	args.PrevLogEntry = index
	if _, err := pipe.AppendEntries(&args, new(raft.AppendEntriesResponse)); err != nil {
		t.Fatalf("AppendEntries(%d): %v", index, err)
	}
}

// TestNATSPipeline_ResponsesInSendOrder is the property raft depends on:
// pipelineDecode pairs each response with the request carried on the same
// future and advances matchIndex from it, so a response delivered against
// another request marks a follower as holding entries it never acknowledged.
func TestNATSPipeline_ResponsesInSendOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const count = 8

	stub := newStubFollower(t, "pipeline-order-follower")
	pipe := makePipeline(ctx, t, "pipeline-order-leader", stub.address)

	for i := uint64(1); i <= count; i++ {
		appendAt(t, pipe, i)
	}
	// Answer every request with its own PrevLogEntry, so a shifted pairing is
	// visible in the response and not only in the arrival order.
	for i := uint64(1); i <= count; i++ {
		msg := stub.next(t)
		stub.respond(t, msg, i)
	}

	for i := uint64(1); i <= count; i++ {
		select {
		case future := <-pipe.Consumer():
			if err := future.Error(); err != nil {
				t.Fatalf("future %d: %v", i, err)
			}
			if got := future.Request().PrevLogEntry; got != i {
				t.Fatalf("future %d carries request for index %d: futures left send order", i, got)
			}
			if got := future.Response().LastLog; got != i {
				t.Fatalf("request %d got the response of request %d", i, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("future %d never completed", i)
		}
	}
}

// TestNATSPipeline_LostReplyAborts drops one reply. Without the per-request
// reply subject every later response would answer the previous request, which
// raft cannot detect; with it the pipeline fails the future instead, and raft
// falls back to synchronous replication on the unsuccessful response.
func TestNATSPipeline_LostReplyAborts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stub := newStubFollower(t, "pipeline-lost-follower")
	pipe := makePipeline(ctx, t, "pipeline-lost-leader", stub.address)

	for i := uint64(1); i <= 3; i++ {
		appendAt(t, pipe, i)
	}
	stub.respond(t, stub.next(t), 1)
	stub.next(t) // request 2: no reply, as if the message had been dropped
	stub.respond(t, stub.next(t), 3)

	first := <-pipe.Consumer()
	if err := first.Error(); err != nil {
		t.Fatalf("first future: %v", err)
	}

	select {
	case future := <-pipe.Consumer():
		if future.Error() == nil {
			t.Fatal("the future whose reply was lost completed successfully")
		}
		if future.Request().PrevLogEntry != 2 {
			t.Fatalf("second future carries request %d, want 2", future.Request().PrevLogEntry)
		}
		// What raft reads: an unsuccessful response aborts the pipeline.
		if future.Response().Success {
			t.Fatal("failed future reports Success; raft would keep pipelining")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a lost reply stalled the pipeline instead of failing the future")
	}
}

// TestNATSPipeline_BoundsInflight pins the window: a follower that stops
// answering must block the sender rather than let the leader keep publishing
// into the follower's pending buffer until the NATS client drops silently.
func TestNATSPipeline_BoundsInflight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stub := newStubFollower(t, "pipeline-window-follower")
	pipe := makePipeline(ctx, t, "pipeline-window-leader", stub.address)

	chSent := make(chan struct{}, 4*maxPipelineInflight)
	chErr := make(chan error, 1)
	go func() {
		for i := uint64(1); i <= 4*maxPipelineInflight; i++ {
			args := makeAppendRPC()
			args.PrevLogEntry = i
			if _, err := pipe.AppendEntries(&args, new(raft.AppendEntriesResponse)); err != nil {
				chErr <- err
				return
			}
			chSent <- struct{}{}
		}
		chErr <- errors.New("sender never blocked on the in-flight bound")
	}()

	sent := 0
wait:
	for {
		select {
		case <-chSent:
			sent++
		case <-time.After(500 * time.Millisecond):
			break wait
		}
	}
	// The queue holds maxPipelineInflight, the decoder holds the one it is
	// waiting on: a couple more than the window, never a multiple of it.
	if sent < maxPipelineInflight || sent > maxPipelineInflight+2 {
		t.Fatalf("sent %d requests before blocking, want between %d and %d",
			sent, maxPipelineInflight, maxPipelineInflight+2)
	}

	if err := pipe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-chErr:
		if !errors.Is(err, raft.ErrPipelineShutdown) {
			t.Fatalf("blocked sender returned %v, want ErrPipelineShutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not release the blocked sender")
	}
	if err := pipe.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestNATSPipeline_BeatsSynchronousThroughput is the acceptance criterion in
// its unit-test form. The follower answers every request one delay after it
// arrives, so the delay stands in for the round trip: the synchronous path
// pays it once per batch, the pipeline pays it once in total.
func TestNATSPipeline_BeatsSynchronousThroughput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		count = 10
		delay = 20 * time.Millisecond
	)

	stub := newStubFollower(t, "pipeline-rate-follower")
	trans, err := makeNATSTransport(ctx, t, "test-cache", "pipeline-rate-leader")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans.conn.Close()

	// One replier: requests are served in arrival order, each after the delay
	// its own arrival started. Replies stay ordered, as a real follower's do.
	go func() {
		for {
			select {
			case req := <-stub.chReq:
				time.Sleep(time.Until(req.at.Add(delay)))
				stub.respond(t, req.msg, 1)
			case <-ctx.Done():
				return
			}
		}
	}()

	start := time.Now()
	for i := 0; i < count; i++ {
		args := makeAppendRPC()
		if err := trans.AppendEntries("id1", raft.ServerAddress(stub.address), &args,
			new(raft.AppendEntriesResponse)); err != nil {
			t.Fatalf("synchronous AppendEntries: %v", err)
		}
	}
	synchronous := time.Since(start)

	pipe, err := trans.AppendEntriesPipeline("id1", raft.ServerAddress(stub.address))
	if err != nil {
		t.Fatalf("AppendEntriesPipeline: %v", err)
	}
	defer func() { _ = pipe.Close() }()

	start = time.Now()
	for i := uint64(1); i <= count; i++ {
		appendAt(t, pipe, i)
	}
	for i := 0; i < count; i++ {
		select {
		case future := <-pipe.Consumer():
			if err := future.Error(); err != nil {
				t.Fatalf("pipelined future: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("pipelined future never completed")
		}
	}
	pipelined := time.Since(start)

	// Comparative, not absolute: the machine's own latency lands on both runs.
	if pipelined*4 > synchronous {
		t.Fatalf("pipelined %s vs synchronous %s: pipelining did not lift the per-round-trip cap",
			pipelined, synchronous)
	}
	t.Logf("%d requests: synchronous %s, pipelined %s", count, synchronous, pipelined)
}

// TestNATSPipeline_MultiPartRequest sends a payload past maxMsgSize through the
// pipeline. Its parts go on connRepl, not connBulk as the synchronous path
// sends them: parts on one connection and the next request on another are two
// streams with no ordering between them.
func TestNATSPipeline_MultiPartRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	receiver, err := makeNATSTransport(ctx, t, "test-cache", "pipeline-multipart-follower")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer receiver.conn.Close()

	go func() {
		for {
			select {
			case rpc := <-receiver.Consumer():
				req, _ := rpc.Command.(*raft.AppendEntriesRequest)
				rpc.Respond(&raft.AppendEntriesResponse{Term: 4, LastLog: req.PrevLogEntry, Success: true}, nil)
			case <-ctx.Done():
				return
			}
		}
	}()

	pipe := makePipeline(ctx, t, "pipeline-multipart-leader", string(receiver.LocalAddr()))

	large := makeAppendRPCLarge()
	large.PrevLogEntry = 42
	if _, err := pipe.AppendEntries(&large, new(raft.AppendEntriesResponse)); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	appendAt(t, pipe, 43)

	for _, want := range []uint64{42, 43} {
		select {
		case future := <-pipe.Consumer():
			if err := future.Error(); err != nil {
				t.Fatalf("future for index %d: %v", want, err)
			}
			if got := future.Response().LastLog; got != want {
				t.Fatalf("got the response of request %d while awaiting %d", got, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("future for index %d never completed", want)
		}
	}
}
