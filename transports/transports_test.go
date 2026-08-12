// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

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

func makeAppendRPCLarge() raft.AppendEntriesRequest {
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

func makeAppendRPCResponse() raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}
}

func makeTransport(ctx context.Context, t *testing.T, useAddrProvider bool, addressOverride string) (*TCPTransport, error) {
	t.Helper()

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
	return NewTCPTransport(ctx, "localhost:0", nil, WithTCPConfig(config))
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
	// mu guards the t.Log call itself, not just done: without it a write that
	// has already passed the done check can still land after the cleanup ran.
	mu     sync.Mutex
	done   bool
	tb     testing.TB
	prefix string
}

func (a *testLoggerAdapter) Write(d []byte) (int, error) {
	if d[len(d)-1] == '\n' {
		d = d[:len(d)-1]
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.done {
		return len(d), nil
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
	tb.Helper()
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
	tb.Helper()
	if testing.Verbose() {
		return hclog.New(&hclog.LoggerOptions{Name: prefix})
	}

	adapter := &testLoggerAdapter{tb: tb, prefix: prefix}
	// NATS runs the connection callbacks on its own dispatcher goroutine, so
	// closing a transport at the end of a test logs after the test returned and
	// t.Log panics — the hazard the comment above warns about (RT-13901).
	tb.Cleanup(func() {
		adapter.mu.Lock()
		adapter.done = true
		adapter.mu.Unlock()
	})

	return hclog.New(&hclog.LoggerOptions{
		Name:   prefix,
		Output: adapter,
	})
}
