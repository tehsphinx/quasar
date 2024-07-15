package transports

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
)

// NewInmemAddr returns a new in-memory addr with
// a randomly generate UUID as the ID.
func NewInmemAddr() raft.ServerAddress {
	return raft.ServerAddress(uuid.NewString())
}

type consumeType int

const (
	consumeRaft consumeType = iota
	consumeCache
)

// inmemPipeline is used to pipeline requests for the in-mem transport.
type inmemPipeline struct {
	trans    *InmemTransport
	peer     *InmemTransport
	peerAddr raft.ServerAddress

	doneCh       chan raft.AppendFuture
	inprogressCh chan *inmemPipelineInflight

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.RWMutex
}

type inmemPipelineInflight struct {
	future *appendFuture
	respCh <-chan raft.RPCResponse
}

// InmemTransport Implements the Transport interface, to allow Raft to be
// tested in-memory without going over a network.
type InmemTransport struct {
	sync.RWMutex
	consumerCh     chan raft.RPC
	chConsumeCache chan raft.RPC
	localAddr      raft.ServerAddress
	peers          map[raft.ServerAddress]*InmemTransport
	pipelines      []*inmemPipeline
	timeout        time.Duration
}

func (i *InmemTransport) CacheConsumer() <-chan raft.RPC {
	return i.chConsumeCache
}

func (i *InmemTransport) Store(id raft.ServerID, target raft.ServerAddress, command *pb.Store) (*pb.StoreResponse, error) {
	rpcResp, err := i.makeRPC(target, command, nil, consumeCache, i.timeout)
	if err != nil {
		return nil, err
	}

	return rpcResp.Response.(*pb.StoreResponse), nil
}

func (i *InmemTransport) LatestUID(id raft.ServerID, target raft.ServerAddress, command *pb.LatestUid) (*pb.LatestUidResponse, error) {
	rpcResp, err := i.makeRPC(target, command, nil, consumeCache, i.timeout)
	if err != nil {
		return nil, err
	}

	return rpcResp.Response.(*pb.LatestUidResponse), nil
}

var _ raft.Transport = (*InmemTransport)(nil)
var _ Transport = (*InmemTransport)(nil)

// NewInmemTransportWithTimeout is used to initialize a new transport and
// generates a random local address if none is specified. The given timeout
// will be used to decide how long to wait for a connected peer to process the
// RPCs that we're sending it. See also Connect() and Consumer().
func NewInmemTransportWithTimeout(addr raft.ServerAddress, timeout time.Duration) (raft.ServerAddress, *InmemTransport) {
	if string(addr) == "" {
		addr = NewInmemAddr()
	}
	trans := &InmemTransport{
		consumerCh:     make(chan raft.RPC, 16),
		chConsumeCache: make(chan raft.RPC, 16),
		localAddr:      addr,
		peers:          make(map[raft.ServerAddress]*InmemTransport),
		timeout:        timeout,
	}
	return addr, trans
}

// NewInmemTransport is used to initialize a new transport
// and generates a random local address if none is specified
func NewInmemTransport(addr raft.ServerAddress) (raft.ServerAddress, *InmemTransport) {
	return NewInmemTransportWithTimeout(addr, 500*time.Millisecond)
}

// SetHeartbeatHandler is used to set optional fast-path for
// heartbeats, not supported for this transport.
func (i *InmemTransport) SetHeartbeatHandler(cb func(raft.RPC)) {
}

// Consumer implements the Transport interface.
func (i *InmemTransport) Consumer() <-chan raft.RPC {
	return i.consumerCh
}

// LocalAddr implements the Transport interface.
func (i *InmemTransport) LocalAddr() raft.ServerAddress {
	return i.localAddr
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (i *InmemTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	i.Lock()
	defer i.Unlock()

	peer, ok := i.peers[target]
	if !ok {
		return nil, fmt.Errorf("failed to connect to peer: %v", target)
	}
	pipeline := newInmemPipeline(i, peer, target)
	i.pipelines = append(i.pipelines, pipeline)
	return pipeline, nil
}

// AppendEntries implements the Transport interface.
func (i *InmemTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, consumeRaft, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*raft.AppendEntriesResponse)
	*resp = *out
	return nil
}

// RequestVote implements the Transport interface.
func (i *InmemTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, consumeRaft, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*raft.RequestVoteResponse)
	*resp = *out
	return nil
}

// InstallSnapshot implements the Transport interface.
func (i *InmemTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	rpcResp, err := i.makeRPC(target, args, data, consumeRaft, 10*i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*raft.InstallSnapshotResponse)
	*resp = *out
	return nil
}

// TimeoutNow implements the Transport interface.
func (i *InmemTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, consumeRaft, 10*i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*raft.TimeoutNowResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) makeRPC(target raft.ServerAddress, args interface{}, r io.Reader, cType consumeType, timeout time.Duration) (rpcResp raft.RPCResponse, err error) {
	i.RLock()
	peer, ok := i.peers[target]
	i.RUnlock()

	if !ok {
		err = fmt.Errorf("failed to connect to peer: %v", target)
		return
	}

	// Send the RPC over
	respCh := make(chan raft.RPCResponse, 1)
	req := raft.RPC{
		Command:  args,
		Reader:   r,
		RespChan: respCh,
	}
	chConsume := getConsumeChan(peer, cType)
	select {
	case chConsume <- req:
	case <-time.After(timeout):
		err = fmt.Errorf("send timed out")
		return
	}

	// Wait for a response
	select {
	case rpcResp = <-respCh:
		if rpcResp.Error != nil {
			err = rpcResp.Error
		}
	case <-time.After(timeout):
		err = fmt.Errorf("command timed out")
	}
	return
}

func getConsumeChan(peer *InmemTransport, cType consumeType) chan raft.RPC {
	if cType == consumeCache {
		return peer.chConsumeCache
	}
	return peer.consumerCh
}

// EncodePeer implements the Transport interface.
func (i *InmemTransport) EncodePeer(id raft.ServerID, p raft.ServerAddress) []byte {
	return []byte(p)
}

// DecodePeer implements the Transport interface.
func (i *InmemTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

// Connect is used to connect this transport to another transport for
// a given peer name. This allows for local routing.
func (i *InmemTransport) Connect(peer raft.ServerAddress, t raft.Transport) {
	trans := t.(*InmemTransport)
	i.Lock()
	defer i.Unlock()
	i.peers[peer] = trans
}

// Disconnect is used to remove the ability to route to a given peer.
func (i *InmemTransport) Disconnect(peer raft.ServerAddress) {
	i.Lock()
	defer i.Unlock()
	delete(i.peers, peer)

	// Disconnect any pipelines
	n := len(i.pipelines)
	for idx := 0; idx < n; idx++ {
		if i.pipelines[idx].peerAddr == peer {
			i.pipelines[idx].Close()
			i.pipelines[idx], i.pipelines[n-1] = i.pipelines[n-1], nil
			idx--
			n--
		}
	}
	i.pipelines = i.pipelines[:n]
}

// DisconnectAll is used to remove all routes to peers.
func (i *InmemTransport) DisconnectAll() {
	i.Lock()
	defer i.Unlock()
	i.peers = make(map[raft.ServerAddress]*InmemTransport)

	// Handle pipelines
	for _, pipeline := range i.pipelines {
		pipeline.Close()
	}
	i.pipelines = nil
}

// Close is used to permanently disable the transport
func (i *InmemTransport) Close() error {
	i.DisconnectAll()
	return nil
}

func newInmemPipeline(trans *InmemTransport, peer *InmemTransport, addr raft.ServerAddress) *inmemPipeline {
	i := &inmemPipeline{
		trans:        trans,
		peer:         peer,
		peerAddr:     addr,
		doneCh:       make(chan raft.AppendFuture, 16),
		inprogressCh: make(chan *inmemPipelineInflight, 16),
		shutdownCh:   make(chan struct{}),
	}
	go i.decodeResponses()
	return i
}

func (i *inmemPipeline) decodeResponses() {
	timeout := i.trans.timeout
	for {
		select {
		case inp := <-i.inprogressCh:
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}

			select {
			case rpcResp := <-inp.respCh:
				// Copy the result back
				*inp.future.resp = *rpcResp.Response.(*raft.AppendEntriesResponse)
				inp.future.respond(rpcResp.Error)

				select {
				case i.doneCh <- inp.future:
				case <-i.shutdownCh:
					return
				}

			case <-timeoutCh:
				inp.future.respond(fmt.Errorf("command timed out"))
				select {
				case i.doneCh <- inp.future:
				case <-i.shutdownCh:
					return
				}

			case <-i.shutdownCh:
				return
			}
		case <-i.shutdownCh:
			return
		}
	}
}

func (i *inmemPipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	// Handle a timeout
	var timeout <-chan time.Time
	if i.trans.timeout > 0 {
		timeout = time.After(i.trans.timeout)
	}

	// Send the RPC over
	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  args,
		RespChan: respCh,
	}

	// Check if we have been already shutdown, otherwise the random choose
	// made by select statement below might pick consumerCh even if
	// shutdownCh was closed.
	i.shutdownLock.RLock()
	shutdown := i.shutdown
	i.shutdownLock.RUnlock()
	if shutdown {
		return nil, raft.ErrPipelineShutdown
	}

	select {
	case i.peer.consumerCh <- rpc:
	case <-timeout:
		return nil, fmt.Errorf("command enqueue timeout")
	case <-i.shutdownCh:
		return nil, raft.ErrPipelineShutdown
	}

	// Send to be decoded
	select {
	case i.inprogressCh <- &inmemPipelineInflight{future, respCh}:
		return future, nil
	case <-i.shutdownCh:
		return nil, raft.ErrPipelineShutdown
	}
}

func (i *inmemPipeline) Consumer() <-chan raft.AppendFuture {
	return i.doneCh
}

func (i *inmemPipeline) Close() error {
	i.shutdownLock.Lock()
	defer i.shutdownLock.Unlock()
	if i.shutdown {
		return nil
	}

	i.shutdown = true
	close(i.shutdownCh)
	return nil
}
