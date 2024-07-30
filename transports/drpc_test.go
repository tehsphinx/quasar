package transports

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/matryer/is"
)

func TestDRPC_CloseStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := NewDRPCTransport(ctx, "localhost:0", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*raft.Log{
			{
				Index: 101,
				Term:  4,
				Type:  raft.LogNoop,
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

	// errCh is used to report errors from any of the goroutines
	// created in this test.
	// It is buffered as to not block.
	errCh := make(chan error, 100)

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*raft.AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					errCh <- fmt.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	// Transport 2 makes outbound request, 3 conn pool
	trans2, err := NewDRPCTransport(ctx, "localhost:0", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	for i := 0; i < 2; i++ {
		// Create wait group
		wg := &sync.WaitGroup{}

		// Try to do parallel appends, should stress the conn pool
		for i = 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var out raft.AppendEntriesResponse
				if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
					errCh <- err
					return
				}

				// Verify the response
				if !reflect.DeepEqual(resp, out) {
					errCh <- fmt.Errorf("command mismatch: %#v %#v", resp, out)
					return
				}
			}()
		}

		// Wait for the routines to finish
		wg.Wait()

		// Check if we received any errors from the above goroutines.
		if len(errCh) > 0 {
			t.Fatal(<-errCh)
		}

		// // Check the conn pool size
		// addr := trans1.LocalAddr()
		// if len(trans2.connPool[addr]) != 3 {
		// 	t.Fatalf("Expected 3 pooled conns!")
		// }
		//
		// if i == 0 {
		// 	trans2.CloseStreams()
		// 	if len(trans2.connPool[addr]) != 0 {
		// 		t.Fatalf("Expected no pooled conns after closing streams!")
		// 	}
		// }
	}
}

func TestDRPC_StartStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	trans, err := NewTCPTransport(ctx, "localhost:0", nil,
		WithTCPMaxPool(2),
		WithTCPTimeout(time.Second),
		WithTCPLogger(newTestLogger(t)))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	trans.Close()
}

func TestDRPC_Heartbeat_FastPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := NewTCPTransport(ctx, "localhost:0", nil,
		WithTCPMaxPool(2),
		WithTCPTimeout(time.Second),
		WithTCPLogger(newTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	// Make the RPC request
	args := raft.AppendEntriesRequest{
		Term:      10,
		RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, Addr: []byte("cartman")},
		Leader:    []byte("cartman"),
	}

	resp := raft.AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	invoked := false
	fastpath := func(rpc raft.RPC) {
		// Verify the command
		req := rpc.Command.(*raft.AppendEntriesRequest)
		if !reflect.DeepEqual(req, &args) {
			t.Fatalf("command mismatch: %#v %#v", *req, args)
		}

		rpc.Respond(&resp, nil)
		invoked = true
	}
	trans1.SetHeartbeatHandler(fastpath)

	// Transport 2 makes outbound request
	trans2, err := NewTCPTransport(ctx, "localhost:0", nil,
		WithTCPMaxPool(2),
		WithTCPTimeout(time.Second),
		WithTCPLogger(newTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	var out raft.AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}

	// Ensure fast-path is used
	if !invoked {
		t.Fatalf("fast-path not used")
	}
}

func TestDRPC_AppendEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(ctx, t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
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
		trans2, err := makeTransport(ctx, t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()

		var out raft.AppendEntriesResponse
		if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}
	}
}

func TestDRPC_AppendEntriesPipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(ctx, t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := makeAppendRPC()
		resp := makeAppendRPCResponse()

		// Listen for a request
		go func() {
			for i := 0; i < 10; i++ {
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
					return
				}
			}
		}()

		// Transport 2 makes outbound request
		trans2, err := makeTransport(ctx, t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()
		pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		for i := 0; i < 10; i++ {
			out := new(raft.AppendEntriesResponse)
			if _, err := pipeline.AppendEntries(&args, out); err != nil {
				t.Fatalf("err: %v", err)
			}
		}

		respCh := pipeline.Consumer()
		for i := 0; i < 10; i++ {
			select {
			case ready := <-respCh:
				// Verify the response
				if !reflect.DeepEqual(&resp, ready.Response()) {
					t.Fatalf("command mismatch: %#v %#v", &resp, ready.Response())
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("timeout")
			}
		}

		_ = pipeline.Close()
	}
}

func TestDRPC_AppendEntriesPipeline_CloseStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := makeTransport(ctx, t, true, "localhost:0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := makeAppendRPC()
	resp := makeAppendRPCResponse()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*raft.AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-shutdownCh:
				return
			}
		}
	}()

	// Transport 2 makes outbound request
	trans2, err := makeTransport(ctx, t, true, string(trans1.LocalAddr()))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	for _, cancelStreams := range []bool{true, false} {
		pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		for i := 0; i < 100; i++ {
			// On the last one, close the streams on the transport one.
			if cancelStreams && i == 10 {
				trans1.CloseStreams()
				time.Sleep(10 * time.Millisecond)
			}

			out := new(raft.AppendEntriesResponse)
			if _, err := pipeline.AppendEntries(&args, out); err != nil {
				break
			}
		}

		var futureErr error
		respCh := pipeline.Consumer()
	OUTER:
		for i := 0; i < 100; i++ {
			select {
			case ready := <-respCh:
				if err := ready.Error(); err != nil {
					futureErr = err
					break OUTER
				}

				// Verify the response
				if !reflect.DeepEqual(&resp, ready.Response()) {
					t.Fatalf("command mismatch: %#v %#v %v", &resp, ready.Response(), ready.Error())
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("timeout when cancel streams is %v", cancelStreams)
			}
		}

		if cancelStreams && futureErr == nil {
			t.Fatalf("expected an error due to the streams being closed")
		} else if !cancelStreams && futureErr != nil {
			t.Fatalf("unexpected error: %v", futureErr)
		}

		pipeline.Close()
	}
}

func TestDRPC_AppendEntriesPipeline_MaxRPCsInFlight(t *testing.T) {
	// Test the important cases 0 (default to 2), 1 (disabled), 2 and "some"
	for _, max := range []int{0, 1, 2, 10} {
		t.Run(fmt.Sprintf("max=%d", max), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			asrt := is.New(t)
			config := &raft.NetworkTransportConfig{
				MaxPool:         2,
				MaxRPCsInFlight: max,
				Timeout:         time.Second,
				// Don't use test logger as the transport has multiple goroutines and
				// causes panics.
				ServerAddressProvider: &testAddrProvider{"localhost:0"},
			}

			// Transport 1 is consumer
			trans1, err := NewTCPTransport(ctx, "localhost:0", nil, WithTCPConfig(config))
			asrt.NoErr(err)
			defer trans1.Close()

			// Make the RPC request
			args := makeAppendRPC()
			resp := makeAppendRPCResponse()

			// Transport 2 makes outbound request
			config.ServerAddressProvider = &testAddrProvider{string(trans1.LocalAddr())}
			trans2, err := NewTCPTransport(ctx, "localhost:0", nil, WithTCPConfig(config))
			asrt.NoErr(err)
			defer trans2.Close()

			// Kill the transports on the timeout to unblock. That means things that
			// shouldn't have blocked did block.
			go func() {
				<-ctx.Done()
				trans2.Close()
				trans1.Close()
			}()

			// Attempt to pipeline
			pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
			if max == 1 {
				// Max == 1 implies no pipelining
				asrt.Equal(err, raft.ErrPipelineReplicationNotSupported)
				return
			}
			asrt.NoErr(err)

			expectedMax := max
			if max == 0 {
				// Should have defaulted to 2
				expectedMax = 2
			}

			for i := 0; i < expectedMax-1; i++ {
				// We should be able to send `max - 1` rpcs before `AppendEntries`
				// blocks. It blocks on the `max` one because it it sends before pushing
				// to the chan. It will block forever when it does because nothing is
				// responding yet.
				out := new(raft.AppendEntriesResponse)
				_, err := pipeline.AppendEntries(&args, out)
				asrt.NoErr(err)
			}

			// Verify the next send blocks without blocking test forever
			errCh := make(chan error, 1)
			go func() {
				out := new(raft.AppendEntriesResponse)
				_, err := pipeline.AppendEntries(&args, out)
				errCh <- err
			}()

			select {
			case err := <-errCh:
				asrt.NoErr(err)
				t.Fatalf("AppendEntries didn't block with %d in flight", max)
			case <-time.After(50 * time.Millisecond):
				// OK it's probably blocked or we got _really_ unlucky with scheduling!
			}

			// Verify that once we receive/respond another one can be sent.
			rpc := <-trans1.Consumer()
			rpc.Respond(resp, nil)

			// We also need to consume the response from the pipeline in case chan is
			// unbuffered (inflight is 2 or 1)
			<-pipeline.Consumer()

			// The last append should unblock once the response is received.
			select {
			case <-errCh:
				// OK
			case <-time.After(50 * time.Millisecond):
				t.Fatalf("last append didn't unblock")
			}
		})
	}
}

func TestDRPC_RequestVote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(ctx, t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
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
		trans2, err := makeTransport(ctx, t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()
		var out raft.RequestVoteResponse
		if err := trans2.RequestVote("id1", trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}
	}
}

func TestDRPC_InstallSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(ctx, t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := raft.InstallSnapshotRequest{
			Term:         10,
			LastLogIndex: 100,
			LastLogTerm:  9,
			Peers:        []byte("blah blah"),
			Size:         10,
			RPCHeader:    raft.RPCHeader{Addr: []byte("kyle")},
		}

		resp := raft.InstallSnapshotResponse{
			Term:    10,
			Success: true,
		}

		// Listen for a request
		go func() {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*raft.InstallSnapshotRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}

				// Try to read the bytes
				buf := make([]byte, 10)
				rpc.Reader.Read(buf)

				// Compare
				if !bytes.Equal(buf, []byte("0123456789")) {
					t.Errorf("bad buf %v", buf)
					return
				}

				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				t.Errorf("timeout")
			}
		}()

		// Transport 2 makes outbound request
		trans2, err := makeTransport(ctx, t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()
		// Create a buffer
		buf := bytes.NewBufferString("0123456789")

		var out raft.InstallSnapshotResponse
		if err := trans2.InstallSnapshot("id1", trans1.LocalAddr(), &args, &out, buf); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}
	}
}

func TestDRPC_EncodeDecode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := NewTCPTransport(ctx, "localhost:0", nil,
		WithTCPMaxPool(2),
		WithTCPTimeout(time.Second),
		WithTCPLogger(newTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	local := trans1.LocalAddr()
	enc := trans1.EncodePeer("id1", local)
	dec := trans1.DecodePeer(enc)

	if dec != local {
		t.Fatalf("enc/dec fail: %v %v", dec, local)
	}
}

func TestDRPC_EncodeDecode_AddressProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addressOverride := "localhost:11111"
	config := &raft.NetworkTransportConfig{
		MaxPool: 2, Timeout: time.Second, Logger: newTestLogger(t),
		ServerAddressProvider: &testAddrProvider{addressOverride},
	}
	trans1, err := NewTCPTransport(ctx, "localhost:0", nil, WithTCPConfig(config))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	local := trans1.LocalAddr()
	enc := trans1.EncodePeer("id1", local)
	dec := trans1.DecodePeer(enc)

	if dec != raft.ServerAddress(addressOverride) {
		t.Fatalf("enc/dec fail: %v %v", dec, addressOverride)
	}
}

func TestDRPC_PooledConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := NewTCPTransport(ctx, "localhost:0", nil,
		WithTCPMaxPool(2),
		WithTCPTimeout(time.Second),
		WithTCPLogger(newTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*raft.Log{
			{
				Index: 101,
				Term:  4,
				Type:  raft.LogNoop,
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

	// errCh is used to report errors from any of the goroutines
	// created in this test.
	// It is buffered as to not block.
	errCh := make(chan error, 100)

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*raft.AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					errCh <- fmt.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	// Transport 2 makes outbound request, 3 conn pool
	trans2, err := NewTCPTransport(ctx, "localhost:0", nil,
		WithTCPMaxPool(3),
		WithTCPTimeout(time.Second),
		WithTCPLogger(newTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	// Create wait group
	wg := &sync.WaitGroup{}

	// Try to do parallel appends, should stress the conn pool
	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			var out raft.AppendEntriesResponse
			if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
				errCh <- err
				return
			}

			// Verify the response
			if !reflect.DeepEqual(resp, out) {
				errCh <- fmt.Errorf("command mismatch: %#v %#v", resp, out)
				return
			}
		}()
	}

	// Wait for the routines to finish
	wg.Wait()

	// Check if we received any errors from the above goroutines.
	if len(errCh) > 0 {
		t.Fatal(<-errCh)
	}

	// Check the conn pool size
	addr := trans1.LocalAddr()
	if len(trans2.connPool[addr]) != 3 {
		t.Fatalf("Expected 3 pooled conns!")
	}
}
