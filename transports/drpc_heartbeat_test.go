// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"testing"

	"github.com/hashicorp/raft"
)

// TestDRPC_SetHeartbeatHandlerDoesNotPanic is the RT-13042 m20 regression
// test. SetHeartbeatHandler used to panic("implement me"), and raft.NewRaft
// calls it unconditionally during construction — so any attempt to use a
// DRPCTransport with raft brought the process down immediately. The
// experimental transport must accept the call as a documented no-op instead.
func TestDRPC_SetHeartbeatHandlerDoesNotPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trans, err := NewDRPCTransport(ctx, "localhost:0", nil)
	if err != nil {
		t.Fatalf("NewDRPCTransport: %v", err)
	}
	defer trans.Close()

	// Must not panic.
	trans.SetHeartbeatHandler(func(raft.RPC) {})
}
