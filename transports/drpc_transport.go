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
		chConsume:      make(chan raft.RPC),
		chConsumeCache: make(chan raft.RPC),
	}
}

type DRPCTransport struct {
	listener net.Listener
	clients  *clients

	timeout time.Duration

	ctxClose       context.Context
	close          context.CancelFunc
	chConsume      chan raft.RPC
	chConsumeCache chan raft.RPC
}

func (s *DRPCTransport) Consumer() <-chan raft.RPC {
	return s.chConsume
}

func (s *DRPCTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(s.listener.Addr().String())
}

func (s *DRPCTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
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

func (s *DRPCTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) DecodePeer(bytes []byte) raft.ServerAddress {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) CacheConsumer() <-chan raft.RPC {
	return s.chConsumeCache
}

func (s *DRPCTransport) Store(id raft.ServerID, target raft.ServerAddress, command *pb.StoreValue) (*pb.StoreValueResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) Load(id raft.ServerID, target raft.ServerAddress, command *pb.LoadValue) (*pb.LoadValueResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (s *DRPCTransport) Close() {
	_ = s.listener.Close()
	s.close()
}
