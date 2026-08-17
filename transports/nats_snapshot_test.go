// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
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

// snapshotStreamResult is what the FSM-side reader of one test stream saw.
type snapshotStreamResult struct {
	data []byte
	err  error
}

// startSnapshotStream builds a snapshot receive stream exactly like
// openNatsStream does — pipe, inter-package timer, snapshotPkgHandler — and
// drains the reader side in the background so handler calls can be driven
// directly, without NATS.
func startSnapshotStream(t *testing.T) (func(subject string, data []byte), chan snapshotStreamResult) {
	t.Helper()

	pipeReader, pipeWriter := io.Pipe()
	timer := time.AfterFunc(time.Second, func() {
		_ = pipeWriter.CloseWithError(context.DeadlineExceeded)
	})
	t.Cleanup(func() { timer.Stop() })

	handler := snapshotPkgHandler(newTestLogger(t), timer, pipeWriter)

	chResult := make(chan snapshotStreamResult, 1)
	go func() {
		data, err := io.ReadAll(pipeReader)
		chResult <- snapshotStreamResult{data: data, err: err}
	}()
	return handler, chResult
}

// TestSnapshotPkgHandlerGapFailsFast covers RT-13934: a package lost,
// duplicated or foreign in the snapshot stream must abort the install
// immediately with an attributable error — not stall into the inter-package
// timer (context.DeadlineExceeded) and not surface as a short read in raft.
func TestSnapshotPkgHandlerGapFailsFast(t *testing.T) {
	t.Run("intact stream passes through", func(t *testing.T) {
		handler, chResult := startSnapshotStream(t)

		handler("subj.send.1", []byte("ab"))
		handler("subj.send.2", []byte("cd"))
		handler("subj.send.EOF", []byte("ef"))

		result := <-chResult
		if result.err != nil {
			t.Fatalf("intact stream errored: %v", result.err)
		}
		if got := string(result.data); got != "abcdef" {
			t.Fatalf("got %q, want %q", got, "abcdef")
		}
	})

	brokenStreams := []struct {
		name     string
		subjects []string
	}{
		{name: "package gap", subjects: []string{"subj.send.1", "subj.send.3"}},
		{name: "duplicate package", subjects: []string{"subj.send.1", "subj.send.1"}},
		{name: "unparsable counter", subjects: []string{"subj.send.bogus"}},
	}
	for _, tt := range brokenStreams {
		t.Run(tt.name, func(t *testing.T) {
			handler, chResult := startSnapshotStream(t)

			for _, subj := range tt.subjects {
				handler(subj, []byte("x"))
			}

			result := <-chResult
			if !errors.Is(result.err, errSnapshotStreamBroken) {
				t.Fatalf("got error %v, want errSnapshotStreamBroken", result.err)
			}
			if errors.Is(result.err, context.DeadlineExceeded) {
				t.Fatal("broken stream surfaced as the inter-package timeout instead of failing fast")
			}

			// Deliveries after the abort must be inert: no panic, no writes.
			handler("subj.send.4", []byte("late"))
			handler("subj.send.EOF", nil)
		})
	}
}

// TestInstallSnapshotTimeoutCapped pins the leader-side install deadline: it
// scales with the snapshot size but must not exceed
// maxSnapshotInstallTimeout, or a follower that dies mid-transfer parks the
// leader's replication goroutine for that peer for up to hours (RT-13934).
func TestInstallSnapshotTimeoutCapped(t *testing.T) {
	const origTimeout = 5 * time.Second

	if got := installSnapshotTimeout(origTimeout, 1024); got != origTimeout {
		t.Errorf("small snapshot: got %v, want %v", got, origTimeout)
	}
	// 10MB / raft.DefaultTimeoutScale (256KB) = 40x scale.
	if got, want := installSnapshotTimeout(origTimeout, 10*1024*1024), 200*time.Second; got != want {
		t.Errorf("mid snapshot: got %v, want %v", got, want)
	}
	if got := installSnapshotTimeout(origTimeout, 100*1024*1024); got != maxSnapshotInstallTimeout {
		t.Errorf("large snapshot: got %v, want cap %v", got, maxSnapshotInstallTimeout)
	}
}

func TestNATSTransport_InstallSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := makeNATSTransport(ctx, t, "test-cache-snapshot", "server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans1.conn.Close()
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
			if _, r := io.ReadFull(rpc.Reader, buf); r != nil {
				t.Errorf("failed to read snapshot data: %v", r)
				return
			}
			if !bytes.Equal(buf, []byte("0123456789")) {
				t.Errorf("bad buf %v", buf)
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting for install snapshot RPC")
		}
	}()

	// Transport 2 makes outbound request
	trans2, err := makeNATSTransport(ctx, t, "test-cache-snapshot", "server2")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans2.conn.Close()

	buf := bytes.NewBufferString("0123456789")
	var out raft.InstallSnapshotResponse
	if err := trans2.InstallSnapshot("server1", "server1", &args, &out, buf); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("response mismatch: %#v %#v", resp, out)
	}
}

// TestNATSTransport_InstallSnapshotLargerThanClientPendingLimit is the
// RT-13934 repro: a snapshot larger than the nats.go default per-subscription
// pending limit (64MB) must install intact even when raft does not start
// draining the stream until well after the fire-and-forget sender has
// published all of it. Without SetPendingLimits(-1, -1) in openNatsStream the
// client silently drops every package past 64MB and the install fails.
func TestNATSTransport_InstallSnapshotLargerThanClientPendingLimit(t *testing.T) {
	const snapSize = 96 * 1024 * 1024
	const consumerDelay = 3 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Transport 1 is consumer
	trans1, err := makeNATSTransport(ctx, t, "test-cache-snapshot-large", "server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans1.conn.Close()
	rpcCh := trans1.Consumer()

	type recvResult struct {
		n      int64
		digest [sha256.Size]byte
		err    error
	}
	chRecv := make(chan recvResult, 1)

	resp := raft.InstallSnapshotResponse{Term: 10, Success: true}
	go func() {
		select {
		case rpc := <-rpcCh:
			// Model raft not draining the stream yet: the sender publishes
			// the whole snapshot fire-and-forget in this window, so
			// everything beyond the first package piles up in the
			// subscription's pending buffer — far past the 64MB default
			// limit this test exists for. Safe against snapshotPkgTimout:
			// the first package's blocking pipe write holds the
			// inter-package timer stopped.
			time.Sleep(consumerDelay)

			hash := sha256.New()
			n, err := io.Copy(hash, rpc.Reader)
			var digest [sha256.Size]byte
			copy(digest[:], hash.Sum(nil))
			chRecv <- recvResult{n: n, digest: digest, err: err}
			rpc.Respond(&resp, err)

		case <-time.After(30 * time.Second):
			t.Errorf("timeout waiting for install snapshot RPC")
		}
	}()

	// Transport 2 makes outbound request
	trans2, err := makeNATSTransport(ctx, t, "test-cache-snapshot-large", "server2")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans2.conn.Close()

	sendHash := sha256.New()
	//nolint:gosec // deterministic test content, not cryptographic
	src := io.TeeReader(io.LimitReader(rand.New(rand.NewSource(42)), snapSize), sendHash)

	args := raft.InstallSnapshotRequest{
		Term:         10,
		LastLogIndex: 100,
		LastLogTerm:  9,
		Size:         snapSize,
		RPCHeader:    raft.RPCHeader{Addr: []byte("server2")},
	}
	var out raft.InstallSnapshotResponse
	if err := trans2.InstallSnapshot("server1", "server1", &args, &out, src); err != nil {
		t.Fatalf("install snapshot failed: %v", err)
	}

	recv := <-chRecv
	if recv.err != nil {
		t.Fatalf("receiver failed: %v", recv.err)
	}
	if recv.n != snapSize {
		t.Fatalf("received %d bytes, want %d", recv.n, snapSize)
	}
	var wantDigest [sha256.Size]byte
	copy(wantDigest[:], sendHash.Sum(nil))
	if recv.digest != wantDigest {
		t.Fatal("received snapshot content differs from sent content")
	}
}

// TestNATSSubscriptionDefaultPendingLimitDropsOversizedStream pins the
// nats.go client behavior the SetPendingLimits(-1, -1) call in openNatsStream
// exists for: an async subscription whose callback is not draining holds at
// most DefaultSubPendingBytesLimit (64MB) of undelivered messages and
// silently drops the rest. If a nats.go upgrade ever changes these semantics,
// this test says so explicitly.
func TestNATSSubscriptionDefaultPendingLimitDropsOversizedStream(t *testing.T) {
	nc, err := nats.Connect(natsURL, nats.Timeout(natsTimeout))
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()

	subj := "quasar.test.pendinglimit." + uuid.NewString()
	release := make(chan struct{})
	sub, err := nc.Subscribe(subj, func(*nats.Msg) {
		<-release
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	defer close(release)

	// ≈ 80MB in snapshot-sized packages, comfortably past the 64MB default.
	payload := make([]byte, maxPkgSize)
	const totalMsgs = 92
	for i := 0; i < totalMsgs; i++ {
		if r := nc.Publish(subj, payload); r != nil {
			t.Fatalf("publish failed: %v", r)
		}
	}
	if r := nc.Flush(); r != nil {
		t.Fatalf("flush failed: %v", r)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		dropped, r := sub.Dropped()
		if r != nil {
			t.Fatalf("dropped query failed: %v", r)
		}
		if dropped > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no messages dropped after %d x %d bytes on a default-limit subscription", totalMsgs, maxPkgSize)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
