package transports

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/matryer/is"
)

func TestNetworkTransport_CloseStreams(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransport("localhost:0", nil,
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
	trans2, err := NewTCPTransport("localhost:0", nil,
		WithTCPMaxPool(3),
		WithTCPTimeout(time.Second),
		WithTCPLogger(newTestLogger(t)),
	)
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

		// Check the conn pool size
		addr := trans1.LocalAddr()
		if len(trans2.connPool[addr]) != 3 {
			t.Fatalf("Expected 3 pooled conns!")
		}

		if i == 0 {
			trans2.CloseStreams()
			if len(trans2.connPool[addr]) != 0 {
				t.Fatalf("Expected no pooled conns after closing streams!")
			}
		}
	}
}

func TestNetworkTransport_StartStop(t *testing.T) {
	trans, err := NewTCPTransport("localhost:0", nil,
		WithTCPMaxPool(2),
		WithTCPTimeout(time.Second),
		WithTCPLogger(newTestLogger(t)))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	trans.Close()
}

func TestNetworkTransport_Heartbeat_FastPath(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransport("localhost:0", nil,
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
	trans2, err := NewTCPTransport("localhost:0", nil,
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

func makeAppendRPC() raft.AppendEntriesRequest {
	return raft.AppendEntriesRequest{
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
}

func makeAppendRPCResponse() raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}
}

func TestNetworkTransport_AppendEntries(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
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
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
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

func TestNetworkTransport_AppendEntriesPipeline(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
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
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
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
		pipeline.Close()

	}
}

func TestNetworkTransport_AppendEntriesPipeline_CloseStreams(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := makeTransport(t, true, "localhost:0")
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
	trans2, err := makeTransport(t, true, string(trans1.LocalAddr()))
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

func TestNetworkTransport_AppendEntriesPipeline_MaxRPCsInFlight(t *testing.T) {
	// Test the important cases 0 (default to 2), 1 (disabled), 2 and "some"
	for _, max := range []int{0, 1, 2, 10} {
		t.Run(fmt.Sprintf("max=%d", max), func(t *testing.T) {
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
			trans1, err := NewTCPTransport("localhost:0", nil, WithTCPConfig(config))
			asrt.NoErr(err)
			defer trans1.Close()

			// Make the RPC request
			args := makeAppendRPC()
			resp := makeAppendRPCResponse()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Transport 2 makes outbound request
			config.ServerAddressProvider = &testAddrProvider{string(trans1.LocalAddr())}
			trans2, err := NewTCPTransport("localhost:0", nil, WithTCPConfig(config))
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

func TestNetworkTransport_RequestVote(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
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
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
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

func TestNetworkTransport_InstallSnapshot(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
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
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()
		// Create a buffer
		buf := bytes.NewBuffer([]byte("0123456789"))

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

func TestNetworkTransport_EncodeDecode(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransport("localhost:0", nil,
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

func TestNetworkTransport_EncodeDecode_AddressProvider(t *testing.T) {
	addressOverride := "localhost:11111"
	config := &raft.NetworkTransportConfig{
		MaxPool: 2, Timeout: time.Second, Logger: newTestLogger(t),
		ServerAddressProvider: &testAddrProvider{addressOverride},
	}
	trans1, err := NewTCPTransport("localhost:0", nil, WithTCPConfig(config))
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

func TestNetworkTransport_PooledConn(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransport("localhost:0", nil,
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
	trans2, err := NewTCPTransport("localhost:0", nil,
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

func makeTransport(t *testing.T, useAddrProvider bool, addressOverride string) (*TCPTransport, error) {
	config := &raft.NetworkTransportConfig{
		MaxPool: 2,
		// Setting this because older tests for pipelining were written when this
		// was a constant and block forever if it's not large enough.
		MaxRPCsInFlight: 130,
		Timeout:         time.Second,
		Logger:          newTestLogger(t),
	}
	if useAddrProvider {
		config.ServerAddressProvider = &testAddrProvider{addressOverride}
	}
	return NewTCPTransport("localhost:0", nil, WithTCPConfig(config))
}

type testAddrProvider struct {
	addr string
}

func (t *testAddrProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	return raft.ServerAddress(t.addr), nil
}

// This can be used as the destination for a logger and it'll
// map them into calls to testing.T.Log, so that you only see
// the logging for failed tests.
type testLoggerAdapter struct {
	tb     testing.TB
	prefix string
}

func (a *testLoggerAdapter) Write(d []byte) (int, error) {
	if d[len(d)-1] == '\n' {
		d = d[:len(d)-1]
	}
	if a.prefix != "" {
		l := a.prefix + ": " + string(d)
		a.tb.Log(l)
		return len(l), nil
	}

	a.tb.Log(string(d))
	return len(d), nil
}

func newTestLogger(tb testing.TB) hclog.Logger {
	return newTestLoggerWithPrefix(tb, "")
}

// newTestLoggerWithPrefix returns a Logger that can be used in tests. prefix
// will be added as the name of the logger.
//
// If tests are run with -v (verbose mode, or -json which implies verbose) the
// log output will go to stderr directly. If tests are run in regular "quiet"
// mode, logs will be sent to t.Log so that the logs only appear when a test
// fails.
//
// Be careful where this is used though - calling t.Log after the test completes
// causes a panic. This is common if you use it for a NetworkTransport for
// example and then close the transport at the end of the test because an error
// is logged after the test is complete.
func newTestLoggerWithPrefix(tb testing.TB, prefix string) hclog.Logger {
	if testing.Verbose() {
		return hclog.New(&hclog.LoggerOptions{Name: prefix})
	}

	return hclog.New(&hclog.LoggerOptions{
		Name:   prefix,
		Output: &testLoggerAdapter{tb: tb, prefix: prefix},
	})
}
