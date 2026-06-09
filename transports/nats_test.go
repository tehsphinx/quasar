// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
)

// NATS connection settings - adjust these for your test environment
const (
	natsURL     = "nats://localhost:4222"
	natsTimeout = 5 * time.Second
)

func makeNATSTransport(ctx context.Context, t *testing.T, cacheName, serverName string) (*NATSTransport, error) {
	t.Helper()

	// Connect to NATS
	nc, err := nats.Connect(natsURL, nats.Timeout(natsTimeout))
	if err != nil {
		return nil, err
	}

	// Create NATS transport
	return NewNATSTransport(
		ctx,
		nc,
		cacheName,
		serverName,
		WithNATSLogger(newTestLogger(t)),
		WithNATSTimeout(natsTimeout),
	)
}

func TestNATSTransport_AppendEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := makeNATSTransport(ctx, t, "test-cache", "server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer func() {
		if trans1 != nil {
			trans1.conn.Close()
		}
	}()

	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := makeAppendRPC()
	resp := makeAppendRPCResponse()

	// Listen for a request
	go func() {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*raft.AppendEntriesRequest)
			if !reflect.DeepEqual(req, &args) {
				t.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(200 * time.Millisecond):
			t.Errorf("timeout")
		}
	}()

	// Transport 2 makes outbound request
	trans2, err := makeNATSTransport(ctx, t, "test-cache", "server2")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() {
		if trans2 != nil {
			trans2.conn.Close()
		}
	}()

	var out raft.AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}

func TestNATSTransport_AppendEntries_LargeMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := makeNATSTransport(ctx, t, "test-cache", "server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer func() {
		if trans1 != nil {
			trans1.conn.Close()
		}
	}()

	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := makeAppendRPCLarge()
	resp := makeAppendRPCResponse()

	// Listen for a request
	go func() {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*raft.AppendEntriesRequest)
			if !reflect.DeepEqual(req, &args) {
				t.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(2000 * time.Millisecond):
			t.Errorf("timeout")
		}
	}()

	// Transport 2 makes outbound request
	trans2, err := makeNATSTransport(ctx, t, "test-cache", "server2")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() {
		if trans2 != nil {
			trans2.conn.Close()
		}
	}()

	var out raft.AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}

func TestNATSTransport_RequestVote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := makeNATSTransport(ctx, t, "test-cache", "server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer func() {
		if trans1 != nil {
			trans1.conn.Close()
		}
	}()

	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.RequestVoteRequest{
		Term:         20,
		LastLogIndex: 100,
		LastLogTerm:  19,
		RPCHeader:    raft.RPCHeader{Addr: []byte("butters")},
	}

	resp := raft.RequestVoteResponse{
		Term:    100,
		Granted: false,
	}

	// Listen for a request
	go func() {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*raft.RequestVoteRequest)
			if !reflect.DeepEqual(req, &args) {
				t.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(200 * time.Millisecond):
			t.Errorf("timeout")
			return
		}
	}()

	// Transport 2 makes outbound request
	trans2, err := makeNATSTransport(ctx, t, "test-cache", "server2")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() {
		if trans2 != nil {
			trans2.conn.Close()
		}
	}()

	var out raft.RequestVoteResponse
	if err := trans2.RequestVote("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}

func TestNATSTransport_RequestPreVote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	trans1, err := makeNATSTransport(ctx, t, "test-cache", "server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer func() {
		if trans1 != nil {
			trans1.conn.Close()
		}
	}()

	rpcCh := trans1.Consumer()

	args := raft.RequestPreVoteRequest{
		Term:         42,
		LastLogIndex: 200,
		LastLogTerm:  41,
		RPCHeader:    raft.RPCHeader{Addr: []byte("butters")},
	}

	resp := raft.RequestPreVoteResponse{
		Term:      42,
		Granted:   true,
		RPCHeader: raft.RPCHeader{Addr: []byte("stan")},
	}

	go func() {
		select {
		case rpc := <-rpcCh:
			req, ok := rpc.Command.(*raft.RequestPreVoteRequest)
			if !ok {
				t.Errorf("unexpected command type %T", rpc.Command)
				return
			}
			if !reflect.DeepEqual(req, &args) {
				t.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}
			rpc.Respond(&resp, nil)
		case <-time.After(200 * time.Millisecond):
			t.Errorf("timeout")
			return
		}
	}()

	trans2, err := makeNATSTransport(ctx, t, "test-cache", "server2")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() {
		if trans2 != nil {
			trans2.conn.Close()
		}
	}()

	var out raft.RequestPreVoteResponse
	if err := trans2.RequestPreVote("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}

func TestNATSTransport_TimeoutNow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := makeNATSTransport(ctx, t, "test-cache", "server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer func() {
		if trans1 != nil {
			trans1.conn.Close()
		}
	}()

	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.TimeoutNowRequest{
		RPCHeader: raft.RPCHeader{Addr: []byte("kyle")},
	}

	resp := raft.TimeoutNowResponse{
		RPCHeader: raft.RPCHeader{},
	}

	// Listen for a request
	go func() {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*raft.TimeoutNowRequest)
			if !reflect.DeepEqual(req, &args) {
				t.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(200 * time.Millisecond):
			t.Errorf("timeout")
		}
	}()

	// Transport 2 makes outbound request
	trans2, err := makeNATSTransport(ctx, t, "test-cache", "server2")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() {
		if trans2 != nil {
			trans2.conn.Close()
		}
	}()

	var out raft.TimeoutNowResponse
	if err := trans2.TimeoutNow("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}

// TestNATSTransport_Heartbeat_FastPath verifies that a heartbeat AppendEntries
// (zero entries, no probe-back state) is dispatched via the heartbeat handler
// instead of the regular consumer channel. Mirrors hashicorp/raft's
// TestNetworkTransport_Heartbeat_FastPath.
func TestNATSTransport_Heartbeat_FastPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	trans1, err := makeNATSTransport(ctx, t, "test-cache", "hb-server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans1.conn.Close()

	args := raft.AppendEntriesRequest{
		Term:      10,
		RPCHeader: raft.RPCHeader{Addr: []byte("cartman")},
	}
	resp := raft.AppendEntriesResponse{
		Term:    10,
		LastLog: 90,
		Success: true,
	}

	var invoked atomic.Bool
	trans1.SetHeartbeatHandler(func(rpc raft.RPC) {
		req, ok := rpc.Command.(*raft.AppendEntriesRequest)
		if !ok {
			t.Errorf("unexpected command type %T", rpc.Command)
			rpc.Respond(nil, nil)
			return
		}
		if req.Term != args.Term || !reflect.DeepEqual(req.RPCHeader.Addr, args.RPCHeader.Addr) ||
			len(req.Entries) != 0 || req.PrevLogEntry != 0 || req.PrevLogTerm != 0 {
			t.Errorf("heartbeat mismatch: %#v", *req)
			rpc.Respond(nil, nil)
			return
		}
		invoked.Store(true)
		rpc.Respond(&resp, nil)
	})

	// Fail the test loudly if the request lands on the consumer channel.
	go func() {
		select {
		case rpc := <-trans1.Consumer():
			t.Errorf("heartbeat unexpectedly delivered to chConsume: %#v", rpc.Command)
			rpc.Respond(nil, nil)
		case <-ctx.Done():
		}
	}()

	trans2, err := makeNATSTransport(ctx, t, "test-cache", "hb-server2")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.conn.Close()

	var out raft.AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("response mismatch: %#v %#v", resp, out)
	}
	if !invoked.Load() {
		t.Fatalf("heartbeat fast-path not invoked")
	}
}

// TestNATSTransport_Heartbeat_BypassesStuckEntriesPath verifies the bug the
// dedicated heartbeat subscription fixes: even when the entries.append
// subscription's callback goroutine is wedged on a chConsume push, a
// heartbeat still completes its round-trip via entries.heartbeat.
func TestNATSTransport_Heartbeat_BypassesStuckEntriesPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	trans1, err := makeNATSTransport(ctx, t, "test-cache", "hbblock-server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans1.conn.Close()

	hbResp := raft.AppendEntriesResponse{Term: 10, Success: true}
	trans1.SetHeartbeatHandler(func(rpc raft.RPC) {
		rpc.Respond(&hbResp, nil)
	})

	trans2, err := makeNATSTransport(ctx, t, "test-cache", "hbblock-server2")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.conn.Close()

	// Deliberately do NOT drain trans1.Consumer(). Fire off enough regular
	// AppendEntries to fill chConsume and then wedge the entries.append
	// callback goroutine on a blocking send. Each request hangs waiting for
	// a response that will never come, so launch them in goroutines.
	heavyArgs := makeAppendRPC()
	stuck := consumerChanSize + 8
	for i := 0; i < stuck; i++ {
		go func() {
			var out raft.AppendEntriesResponse
			_ = trans2.AppendEntries("id1", trans1.LocalAddr(), &heavyArgs, &out)
		}()
	}

	// Give NATS a moment to deliver and the receiver to fill chConsume.
	time.Sleep(200 * time.Millisecond)

	// A heartbeat should still round-trip quickly via the dedicated
	// entries.heartbeat subscription.
	hbArgs := raft.AppendEntriesRequest{
		Term:      10,
		RPCHeader: raft.RPCHeader{Addr: []byte("kenny")},
	}
	done := make(chan error, 1)
	go func() {
		var out raft.AppendEntriesResponse
		done <- trans2.AppendEntries("id1", trans1.LocalAddr(), &hbArgs, &out)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("heartbeat failed while entries.append was wedged: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("heartbeat did not complete while entries.append was wedged — dedicated heartbeat subscription likely not engaged")
	}
}

// TestNATSTransport_Heartbeat_FallsBackToEntriesAppendWhenUnanswered is the
// RT-13010 regression test. It reproduces a peer that is subscribed to
// entries.heartbeat but never answers it -- e.g. its heartbeat fast-path
// (raft SetHeartbeatHandler) is wired to a raft instance torn down during a
// reinit, so processHeartbeat returns on a closed shutdownCh without
// responding. The leader must not loop on "context deadline exceeded"
// forever: after heartbeatTimeout it falls back to entries.append, and the
// receiver routes that fallback to the live raft's consumer loop (chConsume)
// -- not back through the dead heartbeatFn -- so the round-trip completes.
func TestNATSTransport_Heartbeat_FallsBackToEntriesAppendWhenUnanswered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	trans1, err := makeNATSTransport(ctx, t, "test-cache", "hbfb-server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans1.conn.Close()

	// Subscribed to entries.heartbeat but deliberately never responds:
	// models a heartbeat fast-path bound to a shut-down raft.
	trans1.SetHeartbeatHandler(func(_ raft.RPC) {})

	resp := raft.AppendEntriesResponse{Term: 10, LastLog: 90, Success: true}

	// The live raft: drain the regular consumer and answer. The fallback
	// heartbeat arrives here once entries.heartbeat times out.
	var servedViaConsumer atomic.Bool
	go func() {
		for {
			select {
			case rpc := <-trans1.Consumer():
				servedViaConsumer.Store(true)
				rpc.Respond(&resp, nil)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Sender with a short heartbeat timeout so the fallback kicks in quickly.
	nc, err := nats.Connect(natsURL, nats.Timeout(natsTimeout))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	trans2, err := NewNATSTransport(ctx, nc, "test-cache", "hbfb-server2",
		WithNATSLogger(newTestLogger(t)),
		WithNATSTimeout(natsTimeout),
		WithNATSHeartbeatTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.conn.Close()

	hbArgs := raft.AppendEntriesRequest{
		Term:      10,
		RPCHeader: raft.RPCHeader{Addr: []byte("kenny")},
	}

	done := make(chan error, 1)
	var out raft.AppendEntriesResponse
	go func() {
		done <- trans2.AppendEntries("id1", trans1.LocalAddr(), &hbArgs, &out)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("heartbeat did not fall back to entries.append: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("heartbeat never completed — fallback to entries.append not engaged")
	}

	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("response mismatch: got %#v want %#v", out, resp)
	}
	if !servedViaConsumer.Load() {
		t.Fatalf("fallback heartbeat was not served via the regular consumer (chConsume)")
	}
}

// TestNATSTransport_Heartbeat_NilHandlerRoutesToConsumer covers the transport
// contract the RT-13010 lifecycle fix relies on. quasar clears the heartbeat
// fast-path (SetHeartbeatHandler(nil)) the instant it shuts a raft down for a
// reinit / quorum-recovery, so during the window before the new raft rebinds
// it the handler is nil. A beat arriving in that window must not be dropped: it
// is routed to the consumer, which the live raft drains and answers.
func TestNATSTransport_Heartbeat_NilHandlerRoutesToConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	trans1, err := makeNATSTransport(ctx, t, "test-cache", "hbnil-server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans1.conn.Close()

	// No live raft bound — models the reinit Shutdown→rebind window.
	trans1.SetHeartbeatHandler(nil)

	resp := raft.AppendEntriesResponse{Term: 10, LastLog: 90, Success: true}

	// The live raft drains the consumer; the beat is routed here.
	var servedViaConsumer atomic.Bool
	go func() {
		for {
			select {
			case rpc := <-trans1.Consumer():
				servedViaConsumer.Store(true)
				rpc.Respond(&resp, nil)
			case <-ctx.Done():
				return
			}
		}
	}()

	trans2, err := makeNATSTransport(ctx, t, "test-cache", "hbnil-server2")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.conn.Close()

	hbArgs := raft.AppendEntriesRequest{
		Term:      10,
		RPCHeader: raft.RPCHeader{Addr: []byte("kenny")},
	}
	var out raft.AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &hbArgs, &out); err != nil {
		t.Fatalf("heartbeat with nil handler not served: %v", err)
	}

	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("response mismatch: got %#v want %#v", out, resp)
	}
	if !servedViaConsumer.Load() {
		t.Fatalf("nil-handler heartbeat was not served via the consumer (chConsume)")
	}
}
