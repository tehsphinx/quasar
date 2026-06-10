package quasar

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestHandleRPCUnknownCommandErrors is part of the RT-13042 m8 fix. An unknown
// cache command used to reply with {nil, nil}, indistinguishable from a
// successful empty response. It must now reply with an explicit error.
func TestHandleRPCUnknownCommandErrors(t *testing.T) {
	c := &Cache{}

	resp := make(chan raft.RPCResponse, 1)
	c.handleRPC(context.Background(), raft.RPC{Command: struct{}{}, RespChan: resp})

	select {
	case r := <-resp:
		if r.Error == nil {
			t.Fatal("expected an explicit error for an unknown cache command, got nil")
		}
		if r.Response != nil {
			t.Fatalf("expected nil response for an unknown command, got %v", r.Response)
		}
	case <-time.After(time.Second):
		t.Fatal("handleRPC did not reply")
	}
}

// TestConsumeDoesNotHeadOfLineBlock is the RT-13042 m8 regression test for the
// head-of-line blocking. consume used to handle each RPC inline, so a command
// that blocked (here: a response that is never drained) wedged the whole
// consumer and starved every following RPC. With per-RPC dispatch the stuck
// command no longer blocks the next one. Pre-fix this times out.
func TestConsumeDoesNotHeadOfLineBlock(t *testing.T) {
	c := &Cache{}

	// Background ctx is never canceled, so consume parks on its select rather
	// than taking the shutdown path (which would nil-deref on this bare Cache).
	ch := make(chan raft.RPC, 2)
	go c.consume(context.Background(), ch)

	// First RPC: its response channel is never read, so its handler blocks on
	// the response send. Inline (pre-fix) handling wedges the consume loop here.
	ch <- raft.RPC{Command: struct{}{}, RespChan: make(chan raft.RPCResponse)}

	// Second RPC must still be processed and answered.
	resp := make(chan raft.RPCResponse, 1)
	ch <- raft.RPC{Command: struct{}{}, RespChan: resp}

	select {
	case <-resp:
	case <-time.After(2 * time.Second):
		t.Fatal("second cache RPC head-of-line blocked behind a stuck first RPC")
	}
}
