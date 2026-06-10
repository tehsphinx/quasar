// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/v2/codec"
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

// TestDecodeResponsePreservesPercentInError reproduces RT-13042 m16: a remote
// error string containing '%' must be reconstructed verbatim. Using
// fmt.Errorf(rpcError) would interpret it as a format string and garble it
// (e.g. appending "%!s(MISSING)"), breaking reattachNoLeader's substring
// matching. errors.New keeps it intact.
func TestDecodeResponsePreservesPercentInError(t *testing.T) {
	const remoteErr = "no leader: 50% of nodes unavailable %s %d"

	// Encode an error string followed by a response, exactly as the wire
	// protocol decodeResponse expects.
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	if err := enc.Encode(remoteErr); err != nil {
		t.Fatalf("encoding error string: %v", err)
	}
	var resp raft.AppendEntriesResponse
	if err := enc.Encode(&resp); err != nil {
		t.Fatalf("encoding response: %v", err)
	}

	conn := &netConn{
		conn: &nopConn{},
		dec:  codec.NewDecoder(bufio.NewReader(&buf), &codec.MsgpackHandle{}),
	}

	var got raft.AppendEntriesResponse
	_, err := decodeResponse(conn, &got)
	if err == nil {
		t.Fatal("expected an error from decodeResponse")
	}
	if err.Error() != remoteErr {
		t.Fatalf("remote error garbled: got %q, want %q", err.Error(), remoteErr)
	}
}

// nopConn is a net.Conn whose Close is a no-op, used so decodeResponse's
// conn.Release() path does not panic in the tests above.
type nopConn struct{ net.Conn }

func (nopConn) Close() error { return nil }
