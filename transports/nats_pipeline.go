// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

const (
	// maxPipelineInflight bounds how many AppendEntries requests one pipeline
	// has outstanding to a follower. It is what turns pipelining from "no
	// round-trip cap" into "a bounded one": the window multiplies the
	// synchronous ceiling of MaxAppendEntries per round trip, so 16 x 512
	// entries per 5 ms RTT is ~1.6M entries/s — two orders above the ~12k
	// entries/s the cluster sustains, which leaves the follower's service time
	// as the binding constraint instead of the network.
	//
	// The upper bound is the follower's inbox, not the leader's memory: a full
	// window queues at most 16 x maxMsgSize = 14.4MB on the follower's
	// entries.append subscription, against the 64MB the NATS client buffers
	// before it starts dropping silently (DefaultSubPendingBytesLimit, the
	// failure RT-13934 chased in the snapshot path).
	//
	// A constant because no caller has needed to tune it — it becomes a
	// NATSOption the day one does. raft's own TCP transport defaults to 2.
	maxPipelineInflight = 16

	// maxPipelineReplyBuffer sizes the channel replies are delivered on. It is
	// twice the window on purpose: nats.go DROPS a message for a channel
	// subscription whose channel is full (it reports a slow consumer instead
	// of blocking), and a dropped reply costs the whole pipeline. At most
	// maxPipelineInflight+2 replies can be outstanding — the window, plus the
	// one the decoder is handling, plus the one being queued — so this channel
	// is never full.
	maxPipelineReplyBuffer = 32
)

// errPipelineOutOfOrder marks a reply that does not answer the request the
// decoder is waiting for. It means a reply was lost, duplicated or reordered,
// and every later reply in the stream now belongs to an earlier request.
// Surfacing it aborts the pipeline: handing the shifted reply to raft would
// advance the follower's matchIndex past what that follower acknowledged.
var errPipelineOutOfOrder = errors.New("pipelined response out of order")

// natsPipeline implements raft.AppendPipeline over the entries.append subject.
// Instead of waiting for each reply, it publishes with its own reply subject
// and keeps the futures in an ordered, bounded queue that a single decoder
// goroutine drains — raft pairs each response with the request it carries, so
// responses must reach Consumer() in send order.
//
// raft drives one pipeline from a single replication goroutine
// (replication.go pipelineSend), so AppendEntries is never called
// concurrently with itself; only Close races with it.
//
//nolint:govet // Initialized once per replication cycle. Preferring readability to struct optimization here.
type natsPipeline struct {
	trans *NATSTransport
	conn  *nats.Conn
	subj  string
	inbox string
	sub   *nats.Subscription
	chMsg chan *nats.Msg

	// seq numbers the reply subjects and is only touched by AppendEntries.
	seq uint64

	inprogressCh chan *pipelineEntry
	doneCh       chan raft.AppendFuture

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// pipelineEntry is one sent request awaiting its reply.
type pipelineEntry struct {
	future *appendFuture
	reply  string
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (s *NATSTransport) AppendEntriesPipeline(_ raft.ServerID, address raft.ServerAddress) (raft.AppendPipeline, error) {
	p := &natsPipeline{
		trans: s,
		// The replication connection, as the synchronous path uses for
		// entries.append: pipelined bytes must not share a socket with
		// heartbeats, votes or timeout-now (RT-13733).
		conn:         s.connRepl,
		subj:         fmt.Sprintf("quasar.%s.%s.entries.append", s.cacheName, address),
		chMsg:        make(chan *nats.Msg, maxPipelineReplyBuffer),
		inprogressCh: make(chan *pipelineEntry, maxPipelineInflight),
		doneCh:       make(chan raft.AppendFuture, maxPipelineInflight),
		shutdownCh:   make(chan struct{}),
	}
	// NewInbox, not NewRespInbox: the latter returns a subject under the
	// connection's response-mux prefix, so its replies are consumed by the
	// mux that serves Request/RequestMsg and never reach this subscription.
	p.inbox = p.conn.NewInbox()

	sub, err := p.conn.ChanSubscribe(p.inbox+".*", p.chMsg)
	if err != nil {
		return nil, err
	}
	// Subscribe only queues the SUB on the local connection. The first request
	// is published immediately after this returns, and a reply to a subject the
	// server does not know about yet is dropped (RT-13901).
	if err := p.conn.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	p.sub = sub

	go p.decodeResponses()
	return p, nil
}

// AppendEntries is used to pipeline a new append entries request.
func (p *natsPipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse,
) (raft.AppendFuture, error) {
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	p.seq++
	reply := p.inbox + "." + strconv.FormatUint(p.seq, base10)
	if err := p.publish(reply, args); err != nil {
		return nil, err
	}

	// Hand off for decoding. The bounded channel is the flow control: a
	// follower that stops answering blocks the sender here rather than
	// letting the in-flight window grow.
	select {
	case p.inprogressCh <- &pipelineEntry{future: future, reply: reply}:
		return future, nil
	case <-p.shutdownCh:
		return nil, raft.ErrPipelineShutdown
	}
}

// publish sends one request, expecting its answer on reply.
func (p *natsPipeline) publish(reply string, args *raft.AppendEntriesRequest) error {
	bts, err := proto.Marshal(pb.ToAppendEntriesRequest(args))
	if err != nil {
		return err
	}

	// On connRepl even when the payload is multi-part, where the synchronous
	// path switches to connBulk: parts on one connection and the next request
	// on another are two TCP streams with no ordering between them, so the
	// follower could serve request n+1 before request n and answer it with a
	// PrevLogEntry mismatch. One stream, one order.
	msg, err := p.trans.buildRequestMsg(p.conn, p.subj, bts)
	if err != nil {
		return err
	}
	msg.Reply = reply

	return p.conn.PublishMsg(msg)
}

// decodeResponses drains the in-flight queue in send order, waiting for each
// request's own reply before completing its future.
func (p *natsPipeline) decodeResponses() {
	for {
		select {
		case entry := <-p.inprogressCh:
			err := p.awaitReply(entry)
			if err != nil && !errors.Is(err, raft.ErrPipelineShutdown) {
				// The response stays zero-valued, which reaches raft as
				// Success=false and aborts the pipeline back to synchronous
				// replication (replication.go pipelineDecode). Log it here:
				// raft only logs that the pipeline aborted, not why. Shutdown
				// is not a failure: Close raced the reply, which happens on
				// every step-down and every cache shutdown.
				p.trans.logger.Error("pipelined append entries failed", "error", err, "subject", p.subj)
			}
			entry.future.respond(err)

			select {
			case p.doneCh <- entry.future:
			case <-p.shutdownCh:
				return
			}
		case <-p.shutdownCh:
			return
		}
	}
}

// awaitReply waits for the reply belonging to entry, bounded by the transport
// timeout measured from the moment the request was sent.
func (p *natsPipeline) awaitReply(entry *pipelineEntry) error {
	var chTimeout <-chan time.Time
	if p.trans.timeout > 0 {
		timer := time.NewTimer(time.Until(entry.future.start.Add(p.trans.timeout)))
		defer timer.Stop()

		chTimeout = timer.C
	}

	select {
	case msg := <-p.chMsg:
		return decodeReply(entry, msg)
	case <-chTimeout:
		return fmt.Errorf("pipelined append entries timed out after %s", p.trans.timeout)
	case <-p.shutdownCh:
		return raft.ErrPipelineShutdown
	}
}

// decodeReply verifies that msg answers entry and fills in its response.
func decodeReply(entry *pipelineEntry, msg *nats.Msg) error {
	if msg.Subject != entry.reply {
		return fmt.Errorf("%w: reply on %q while awaiting %q", errPipelineOutOfOrder, msg.Subject, entry.reply)
	}

	var protoResp pb.CommandResponse
	if err := proto.Unmarshal(msg.Data, &protoResp); err != nil {
		return err
	}
	payload, err := checkRaftResponse(&protoResp, (*pb.CommandResponse).GetAppendEntries)
	if err != nil {
		return err
	}

	*entry.future.resp = *payload.Convert()
	return nil
}

// Consumer returns a channel that can be used to consume complete futures.
func (p *natsPipeline) Consumer() <-chan raft.AppendFuture {
	return p.doneCh
}

// Close is used to shut down the pipeline connection.
func (p *natsPipeline) Close() error {
	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()

	if p.shutdown {
		return nil
	}
	p.shutdown = true
	close(p.shutdownCh)

	return p.sub.Unsubscribe()
}
