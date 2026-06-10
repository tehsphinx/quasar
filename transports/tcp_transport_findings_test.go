// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// captureLogger returns an hclog.Logger writing into buf, guarded for
// concurrent use since handleConn runs in its own goroutine here.
func captureLogger(buf *bytes.Buffer, mu *sync.Mutex) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:   "tcp-test",
		Level:  hclog.Debug,
		Output: &lockedWriter{buf: buf, mu: mu},
	})
}

type lockedWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// runHandleConn drives handleConn over an in-memory pipe. seed is written to
// the transport's side of the connection, then that side is closed. It returns
// the captured log output once handleConn has exited.
func runHandleConn(t *testing.T, seed []byte) string {
	t.Helper()

	client, server := net.Pipe()

	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)
	trans := &TCPTransport{
		logger:    captureLogger(&buf, &mu),
		chConsume: make(chan raft.RPC, 1),
	}

	done := make(chan struct{})
	go func() {
		trans.handleConn(context.Background(), server)
		close(done)
	}()

	if len(seed) > 0 {
		if _, err := client.Write(seed); err != nil {
			t.Fatalf("seeding connection: %v", err)
		}
	}
	// Closing the client end yields io.EOF (or io.ErrUnexpectedEOF mid-decode)
	// on the transport's reader, ending handleConn.
	_ = client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not exit")
	}

	mu.Lock()
	defer mu.Unlock()
	return buf.String()
}

// TestHandleConnLogsRealDecodeError reproduces RT-13042 m15: a real
// (non-EOF) decode/dispatch error must be logged. An unknown rpc-type byte
// makes handleCommand return a non-EOF error.
func TestHandleConnLogsRealDecodeError(t *testing.T) {
	// 0xff is not a valid rpc type -> "unknown rpc type" error.
	out := runHandleConn(t, []byte{0xff})

	if !strings.Contains(out, "failed to decode incoming command") {
		t.Fatalf("expected real decode error to be logged, got: %q", out)
	}
}

// TestHandleConnQuietOnCleanClose verifies the normal connection-close path
// (clean io.EOF) stays silent.
func TestHandleConnQuietOnCleanClose(t *testing.T) {
	out := runHandleConn(t, nil)

	if strings.Contains(out, "failed to decode incoming command") {
		t.Fatalf("clean close should not log a decode error, got: %q", out)
	}
}
