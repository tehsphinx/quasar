// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
)

// TestNATSTransport_RaftConnsSplitBySubject pins the RT-13733 connection
// assignment. nats.Conn.publish holds the connection mutex across a socket
// flush once its 32 KB write buffer fills, so heartbeats must not share a
// connection with anything that keeps crossing that threshold — nor with the
// application connection the caller passed in.
func TestNATSTransport_RaftConnsSplitBySubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	trans, err := makeNATSTransport(ctx, t, "test-cache", "split-server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans.conn.Close()

	for _, c := range []struct {
		name string
		conn *nats.Conn
	}{{"live", trans.connLive}, {"repl", trans.connRepl}, {"bulk", trans.connBulk}} {
		if c.conn == trans.conn {
			t.Fatalf("%s connection is the caller's connection; raft must not publish on it", c.name)
		}
		if want := "quasar-test-cache-split-server1-" + c.name; c.conn.Opts.Name != want {
			t.Errorf("%s connection name = %q, want %q", c.name, c.conn.Opts.Name, want)
		}
	}
	if trans.connLive == trans.connRepl || trans.connLive == trans.connBulk || trans.connRepl == trans.connBulk {
		t.Fatal("the three raft connections must be distinct sockets")
	}

	// The receive side is where the isolation is bought: an inbound heartbeat
	// must not queue behind snapshot chunks in one TCP stream. 4 liveness
	// subjects (heartbeat, vote, prevote, timeout-now), 5 replication subjects
	// (entries.append plus the four cache RPCs), 1 bulk subject
	// (install.snapshot).
	for _, c := range []struct {
		name string
		conn *nats.Conn
		want int
	}{{"live", trans.connLive, 4}, {"repl", trans.connRepl, 5}, {"bulk", trans.connBulk, 1}} {
		if got := c.conn.NumSubscriptions(); got != c.want {
			t.Errorf("%s connection has %d subscriptions, want %d", c.name, got, c.want)
		}
	}
}

// TestNATSTransport_HeartbeatSurvivesBulkTraffic streams a snapshot and
// full-batch AppendEntries from one transport while heartbeating on it, the
// situation that cost the rt-uat cluster its leader every couple of minutes
// (RT-13733). Absolute round-trip numbers on a loopback server are far below
// any lease timeout either way, so the assertion is comparative: the same load
// is driven twice, once from a split sender and once from a sender whose three
// connections are collapsed back onto one, and the split must be materially
// faster. Collapse the assignment and this fails.
//
// Medians, not worst round trips: the worst beat of a run is one scheduler
// hiccup, which on a loaded machine lands on the split run as easily as on the
// shared one and inverts the comparison (RT-13901).
func TestNATSTransport_HeartbeatSurvivesBulkTraffic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	receiver, err := makeNATSTransport(ctx, t, "test-cache", "bulkload-server1")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer receiver.conn.Close()

	hbResp := raft.AppendEntriesResponse{Term: 10, Success: true}
	receiver.SetHeartbeatHandler(func(rpc raft.RPC) { rpc.Respond(&hbResp, nil) })

	appendResp := makeAppendRPCResponse()
	go func() {
		for {
			select {
			case rpc := <-receiver.Consumer():
				rpc.Respond(&appendResp, nil)
			case <-ctx.Done():
				return
			}
		}
	}()

	split := medianBeatUnderBulkLoad(ctx, t, receiver, "bulkload-split", false)
	shared := medianBeatUnderBulkLoad(ctx, t, receiver, "bulkload-shared", true)
	t.Logf("median heartbeat round trip under bulk load: split=%s shared=%s", split, shared)

	// A factor, not a wall-clock bound: the blocking is structural (one
	// connection mutex held across socket flushes), so the gap is large — 15 ms
	// vs 100 ms when this was written — and 2× leaves room for a noisy machine.
	const minGain = 2
	if split*minGain > shared {
		t.Fatalf("median heartbeat round trip %s is not %dx better than sharing one connection (%s) — the connection assignment is no longer isolating the beat",
			split, minGain, shared)
	}
}

// medianBeatUnderBulkLoad drives a snapshot stream and full replication batches
// at receiver from a fresh sender, and returns the median heartbeat round trip
// measured while that load runs. With collapse set, the sender publishes
// everything on one connection — the pre-RT-13733 behaviour.
func medianBeatUnderBulkLoad(ctx context.Context, t *testing.T, receiver *NATSTransport, name string, collapse bool) time.Duration {
	t.Helper()

	sender, err := makeNATSTransport(ctx, t, "test-cache", name)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer sender.conn.Close()
	if collapse {
		sender.connLive, sender.connRepl, sender.connBulk = sender.conn, sender.conn, sender.conn
	}

	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()

	var load sync.WaitGroup
	load.Add(2)
	// Snapshot chunks, published exactly as InstallSnapshot publishes them.
	// Nothing subscribes to the sink subject: what is under test is the
	// sender's write path, not the receiver's reassembly.
	go func() {
		defer load.Done()
		const snapshotSize = 16 * 1024 * 1024 // the 16.6 MB rt-uat snapshot
		snapshot := bytes.Repeat([]byte("s"), snapshotSize)
		for loadCtx.Err() == nil {
			if err := sender.sendSnapshot("quasar.test-cache."+name+"-sink", bytes.NewReader(snapshot)); err != nil {
				return
			}
		}
	}()
	// Replication: a full single-part batch and a multi-part one.
	go func() {
		defer load.Done()
		batch, large := makeAppendRPCFullBatch(), makeAppendRPCLarge()
		for loadCtx.Err() == nil {
			var out raft.AppendEntriesResponse
			_ = sender.AppendEntries("id1", receiver.LocalAddr(), &batch, &out)
			_ = sender.AppendEntries("id1", receiver.LocalAddr(), &large, &out)
		}
	}()

	// Beat at raft's cadence (HeartbeatTimeout/10) for long enough to overlap
	// several chunk streams.
	hbArgs := raft.AppendEntriesRequest{Term: 10, RPCHeader: raft.RPCHeader{Addr: []byte("kenny")}}
	const (
		beats   = 50
		beatGap = 10 * time.Millisecond
	)
	rtts := make([]time.Duration, 0, beats)
	for i := 0; i < beats; i++ {
		var out raft.AppendEntriesResponse
		start := time.Now()
		if err := sender.AppendEntries("id1", receiver.LocalAddr(), &hbArgs, &out); err != nil {
			t.Fatalf("heartbeat %d failed while bulk traffic was streaming: %v", i, err)
		}
		rtts = append(rtts, time.Since(start))
		time.Sleep(beatGap)
	}
	stopLoad()
	load.Wait()

	slices.Sort(rtts)
	return rtts[len(rtts)/2]
}

// makeAppendRPCFullBatch is a MaxAppendEntries=512 batch of contact-version
// sized entries: ~61 KB, the single-part replication payload the production
// config produces.
func makeAppendRPCFullBatch() raft.AppendEntriesRequest {
	const (
		entries   = 512
		entrySize = 24
	)
	logs := make([]*raft.Log, 0, entries)
	for i := 0; i < entries; i++ {
		logs = append(logs, &raft.Log{
			Index: uint64(101 + i),
			Term:  4,
			Type:  raft.LogCommand,
			Data:  []byte(strings.Repeat("c", entrySize)),
		})
	}
	return raft.AppendEntriesRequest{
		Term:              10,
		PrevLogEntry:      100,
		PrevLogTerm:       4,
		Entries:           logs,
		LeaderCommitIndex: 90,
		RPCHeader:         raft.RPCHeader{Addr: []byte("cartman")},
	}
}

// TestNATSTransport_PipelineStaysOffTheLiveConn holds RT-13772 to the RT-13733
// rule. Pipelining publishes a whole window of AppendEntries without waiting
// for a reply, so it is the loudest publisher the transport has; those bytes,
// and the subscription their replies land on, must stay on the replication
// connection. On the live connection they would sit in front of a heartbeat
// the moment the 32 KB write buffer flushes.
func TestNATSTransport_PipelineStaysOffTheLiveConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stub := newStubFollower(t, "pipeline-split-follower")
	trans, err := makeNATSTransport(ctx, t, "test-cache", "pipeline-split-leader")
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer trans.conn.Close()

	liveSubs, replSubs := trans.connLive.NumSubscriptions(), trans.connRepl.NumSubscriptions()

	pipe, err := trans.AppendEntriesPipeline("id1", raft.ServerAddress(stub.address))
	if err != nil {
		t.Fatalf("AppendEntriesPipeline: %v", err)
	}
	defer func() { _ = pipe.Close() }()

	if got := trans.connLive.NumSubscriptions(); got != liveSubs {
		t.Errorf("the pipeline put %d subscriptions on the live connection", got-liveSubs)
	}
	if got := trans.connRepl.NumSubscriptions(); got != replSubs+1 {
		t.Errorf("replication connection has %d subscriptions, want %d: the pipeline's replies are elsewhere",
			got, replSubs+1)
	}

	live, repl, bulk := trans.connLive.Stats(), trans.connRepl.Stats(), trans.connBulk.Stats()

	// A full window of production-sized batches, with nobody answering.
	for i := 0; i < maxPipelineInflight; i++ {
		args := makeAppendRPCFullBatch()
		if _, err := pipe.AppendEntries(&args, new(raft.AppendEntriesResponse)); err != nil {
			t.Fatalf("AppendEntries %d: %v", i, err)
		}
	}
	if err := trans.connRepl.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if got := trans.connLive.Stats().OutMsgs - live.OutMsgs; got != 0 {
		t.Errorf("the pipeline published %d messages on the live connection", got)
	}
	if got := trans.connBulk.Stats().OutMsgs - bulk.OutMsgs; got != 0 {
		t.Errorf("the pipeline published %d messages on the bulk connection", got)
	}
	if got := trans.connRepl.Stats().OutMsgs - repl.OutMsgs; got < maxPipelineInflight {
		t.Errorf("the replication connection carried %d of %d pipelined requests", got, maxPipelineInflight)
	}
}
