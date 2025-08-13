// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"reflect"
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
