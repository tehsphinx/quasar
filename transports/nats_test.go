// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
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
	args := makeAppendRPCNats()
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

func makeAppendRPCNats() raft.AppendEntriesRequest {
	largeData := bytes.Repeat([]byte("a"), 150*1024)

	var entries []*raft.Log
	for i := 0; i < 25; i++ {
		entries = append(entries, &raft.Log{
			Index: 101,
			Term:  4,
			Type:  raft.LogNoop,
			Data:  largeData,
		})
	}

	return raft.AppendEntriesRequest{
		Term:              10,
		PrevLogEntry:      100,
		PrevLogTerm:       4,
		Entries:           entries,
		LeaderCommitIndex: 90,
		RPCHeader:         raft.RPCHeader{Addr: []byte("cartman")},
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

// Helper functions are defined in tcp_test.go and shared across test files

func TestNATSTransport_AppendEntries_LargeMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	// Create a large message that will exceed maxPkgSize (1MB)
	largeData := make([]byte, maxPkgSize+1000) // Slightly larger than 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Make the RPC request with large entries
	args := raft.AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*raft.Log{
			{
				Index: 101,
				Term:  4,
				Type:  raft.LogCommand,
				Data:  largeData,
			},
		},
		LeaderCommitIndex: 90,
		RPCHeader:         raft.RPCHeader{Addr: []byte("cartman")},
	}

	resp := raft.AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	// Listen for a request
	go func() {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*raft.AppendEntriesRequest)
			if !reflect.DeepEqual(req, &args) {
				t.Errorf("command mismatch: entries length %d vs %d", len(req.Entries), len(args.Entries))
				if len(req.Entries) > 0 && len(args.Entries) > 0 {
					t.Errorf("data length %d vs %d", len(req.Entries[0].Data), len(args.Entries[0].Data))
				}
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(2 * time.Second):
			t.Errorf("timeout waiting for large message")
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
