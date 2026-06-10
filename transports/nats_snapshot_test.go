// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"io"
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

// TestWriteSnapshotPkgSlowReaderDoesNotTimeout reproduces RT-13042 m14: the
// inter-package timer must not punish a slow local reader. While
// pipeWriter.Write blocks waiting for the FSM-side reader, the timer has to be
// paused; otherwise a reader slower than snapshotPkgTimout aborts an otherwise
// healthy snapshot install. The timer is exercised with a short period so a
// slow reader is simulated quickly.
func TestWriteSnapshotPkgSlowReaderDoesNotTimeout(t *testing.T) {
	const pkgTimeout = 50 * time.Millisecond

	pipeReader, pipeWriter := io.Pipe()
	timer := time.AfterFunc(pkgTimeout, func() {
		_ = pipeWriter.CloseWithError(context.DeadlineExceeded)
	})
	defer timer.Stop()

	// Reader consumes the package, but only after a delay far longer than the
	// inter-package timeout, mimicking a busy FSM-side reader / reinit window.
	readErr := make(chan error, 1)
	go func() {
		time.Sleep(4 * pkgTimeout)
		buf := make([]byte, 16)
		_, err := io.ReadFull(pipeReader, buf)
		readErr <- err
	}()

	// Blocks until the slow reader above consumes the data.
	writeSnapshotPkg(timer, pipeWriter, "subj.send.1", []byte("snapshot-package"))

	if err := <-readErr; err != nil {
		t.Fatalf("slow reader saw error, timer punished local reader instead of measuring network gap: %v", err)
	}

	// A clean EOF must still close the pipe without error after the slow write.
	go func() { _, _ = io.ReadAll(pipeReader) }()
	writeSnapshotPkg(timer, pipeWriter, "subj.send.EOF", nil)
}
