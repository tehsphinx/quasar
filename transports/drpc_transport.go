package transports

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
)

func newDRPCTransport(ctx context.Context, list net.Listener, opts ...DRPCOption) *DRPCTransport {
	cfg := getDRPCOptions(opts)
	ctx, cancel := context.WithCancel(ctx)

	return &DRPCTransport{
		listener: list,
		clients:  newClientsPool(),

		timeout: cfg.timeout,

		ctxClose:       ctx,
		close:          cancel,
		chConsume:      make(chan raft.RPC, consumerChanSize),
		chConsumeCache: make(chan raft.RPC, consumerChanSize),
	}
}

// DRPCTransport defines a drpc based transport layer for the cache.
//
//nolint:govet // Usually initialized once. Preferring readability to struct optimization here.
type DRPCTransport struct {
	listener net.Listener
	clients  *clients

	timeout time.Duration

	//nolint:containedctx // shutdown context.
	// TODO: get rid of this context.
	ctxClose       context.Context
	close          context.CancelFunc
	chConsume      chan raft.RPC
	chConsumeCache chan raft.RPC
}

// Consumer returns a channel that can be used to
// consume and respond to RPC requests.
func (s *DRPCTransport) Consumer() <-chan raft.RPC {
	return s.chConsume
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (s *DRPCTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(s.listener.Addr().String())
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (s *DRPCTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	// TODO implement me
	panic("implement me")
}

// AppendEntries sends the appropriate RPC to the target node.
func (s *DRPCTransport) AppendEntries(_ raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest,
	resp *raft.AppendEntriesResponse,
) error {
	conn, err := s.clients.GetClient(target)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(s.ctxClose, s.timeout)
	defer cancel()

	res, err := conn.AppendEntries(ctx, pb.ToAppendEntriesRequest(args))
	if err != nil {
		return err
	}

	if resp == nil {
		return nil
	}

	*resp = *res.Convert()
	return nil
}

// RequestVote sends the appropriate RPC to the target node.
func (s *DRPCTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest,
	resp *raft.RequestVoteResponse,
) error {
	// TODO implement me
	panic("implement me")
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (s *DRPCTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest,
	resp *raft.InstallSnapshotResponse, data io.Reader,
) error {
	// TODO implement me
	panic("implement me")
}

// EncodePeer is used to serialize a peer's address.
func (s *DRPCTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	// TODO implement me
	panic("implement me")
}

// DecodePeer is used to deserialize a peer's address.
func (s *DRPCTransport) DecodePeer(bytes []byte) raft.ServerAddress {
	// TODO implement me
	panic("implement me")
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a Transport does not support this, it can simply
// ignore the call, and push the heartbeat onto the Consumer channel.
func (s *DRPCTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	// TODO implement me
	panic("implement me")
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (s *DRPCTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest,
	resp *raft.TimeoutNowResponse,
) error {
	// TODO implement me
	panic("implement me")
}

// CacheConsumer returns the cache consumer channel to which all incoming cache commands are sent.
func (s *DRPCTransport) CacheConsumer() <-chan raft.RPC {
	return s.chConsumeCache
}

// Store asks the master to apply a change command to the raft cluster.
func (s *DRPCTransport) Store(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.Store,
) (*pb.StoreResponse, error) {
	// TODO implement me
	panic("implement me")
}

// ResetCache asks the master to reset the cache.
func (s *DRPCTransport) ResetCache(ctx context.Context, _ raft.ServerID, address raft.ServerAddress,
	request *pb.ResetCache,
) (*pb.ResetCacheResponse, error) {
	// TODO implement me
	panic("implement me")
}

// LatestUID asks the master to return its latest known / generated uid.
func (s *DRPCTransport) LatestUID(ctx context.Context, id raft.ServerID, target raft.ServerAddress,
	command *pb.LatestUid,
) (*pb.LatestUidResponse, error) {
	// TODO implement me
	panic("implement me")
}

// Close closes the transport.
func (s *DRPCTransport) Close() {
	_ = s.listener.Close()
	s.close()
}
