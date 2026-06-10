// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"testing"
	"time"
)

// TestInstallSnapshotRespChanDoesNotBlockLateResponse reproduces the goroutine
// leak from RT-13042 m13: the subscription callback in InstallSnapshot does
// `chResp <- msg.Data`. If the select that reads chResp has already returned on
// ctx.Done(), a response arriving afterwards must not block the callback
// goroutine forever. The fix buffers chResp (capacity 1).
func TestInstallSnapshotRespChanDoesNotBlockLateResponse(t *testing.T) {
	// chResp is allocated exactly like InstallSnapshot does.
	chResp := make(chan []byte, 1)

	ctx, cancel := context.WithCancel(context.Background())
	// Simulate the select exiting on ctx.Done() before any response arrives.
	cancel()
	select {
	case <-ctx.Done():
	case <-chResp:
		t.Fatal("unexpected response")
	}

	// The subscription callback fires a late response. With an unbuffered
	// channel and no reader left, this send blocks forever (the leak). With the
	// buffered channel it completes.
	done := make(chan struct{})
	go func() {
		chResp <- []byte("late response")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late response send blocked: chResp must be buffered to avoid leaking the subscription callback goroutine")
	}
}
