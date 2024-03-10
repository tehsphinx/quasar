package transports

import (
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// newNetPipeline is used to construct a netPipeline from a given transport and
// connection. It is a bug to ever call this with maxInFlight less than 2
// (minInFlightForPipelining) and will cause a panic.
func newNetPipeline(trans *tcpTransport, conn *netConn, maxInFlight int) *netPipeline {
	if maxInFlight < minInFlightForPipelining {
		// Shouldn't happen (tm) since we validate this in the one call site and
		// skip pipelining if it's lower.
		panic("pipelining makes no sense if maxInFlight < 2")
	}
	n := &netPipeline{
		conn:  conn,
		trans: trans,
		// The buffer size is 2 less than the configured max because we send before
		// waiting on the chann	el and the decode routine unblocks the channel as
		// soon as it's waiting on the first request. So a zero-buffered channel
		// still allows 1 request to be sent even while decode is still waiting for
		// a response from the previous one. i.e. two are inflight at the same time.
		inprogressCh: make(chan *appendFuture, maxInFlight-2),
		doneCh:       make(chan raft.AppendFuture, maxInFlight-2),
		shutdownCh:   make(chan struct{}),
	}
	go n.decodeResponses()
	return n
}

type netPipeline struct {
	conn  *netConn
	trans *tcpTransport

	doneCh       chan raft.AppendFuture
	inprogressCh chan *appendFuture

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// decodeResponses is a long running routine that decodes the responses
// sent on the connection.
func (n *netPipeline) decodeResponses() {
	timeout := n.trans.timeout
	for {
		select {
		case future := <-n.inprogressCh:
			if timeout > 0 {
				_ = n.conn.conn.SetReadDeadline(time.Now().Add(timeout))
			}

			_, err := decodeResponse(n.conn, future.resp)
			future.respond(err)
			select {
			case n.doneCh <- future:
			case <-n.shutdownCh:
				return
			}
		case <-n.shutdownCh:
			return
		}
	}
}

// AppendEntries is used to pipeline a new append entries request.
func (n *netPipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	// Add a send timeout
	if timeout := n.trans.timeout; timeout > 0 {
		_ = n.conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	}

	// Send the RPC
	if err := sendRPC(n.conn, rpcAppendEntries, future.args); err != nil {
		return nil, err
	}

	// Hand-off for decoding, this can also cause back-pressure
	// to prevent too many inflight requests
	select {
	case n.inprogressCh <- future:
		return future, nil
	case <-n.shutdownCh:
		return nil, raft.ErrPipelineShutdown
	}
}

// Consumer returns a channel that can be used to consume complete futures.
func (n *netPipeline) Consumer() <-chan raft.AppendFuture {
	return n.doneCh
}

// Close is used to shut down the pipeline connection.
func (n *netPipeline) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	if n.shutdown {
		return nil
	}

	// Release the connection
	_ = n.conn.Release()

	n.shutdown = true
	close(n.shutdownCh)
	return nil
}
