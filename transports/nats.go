package transports

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

const (
	maxPkgSize        = 900 * 1024 // 900KB
	snapshotPkgTimout = 5 * time.Second
)

// NewNATSTransport creates a new NATS based transport.
func NewNATSTransport(ctx context.Context, conn *nats.Conn, cacheName, serverName string, opts ...NATSOption) (*NATSTransport, error) {
	config := getNATSOptions(opts)

	s := &NATSTransport{
		conn:           conn,
		logger:         config.logger,
		serverName:     serverName,
		cacheName:      cacheName,
		timeout:        config.timeout,
		chConsume:      make(chan raft.RPC, consumerChanSize),
		chConsumeCache: make(chan raft.RPC, consumerChanSize),
	}
	if err := s.listen(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// NATSTransport implements the transport layer for the quasar.Cache using NATS.
//
//nolint:govet // Usually initialized once. Preferring readability to struct optimization here.
type NATSTransport struct {
	conn *nats.Conn

	logger hclog.Logger

	serverName string
	cacheName  string
	timeout    time.Duration

	chConsume      chan raft.RPC
	chConsumeCache chan raft.RPC

	heartbeatFn     func(raft.RPC)
	heartbeatFnLock sync.Mutex
}

func (s *NATSTransport) listen(ctx context.Context) error {
	subjPrefix := fmt.Sprintf("quasar.%s.%s", s.cacheName, s.serverName)
	// fmt.Println("server =", s.serverName, " subjPrefix =", subjPrefix)

	subEntries, err := s.conn.Subscribe(subjPrefix+".entries.append", s.handleEntries(ctx))
	if err != nil {
		return err
	}
	subVote, err := s.conn.Subscribe(subjPrefix+".request.vote", s.handleVote(ctx))
	if err != nil {
		return err
	}
	subStore, err := s.conn.Subscribe(subjPrefix+".cache.store", s.handleStore(ctx))
	if err != nil {
		return err
	}
	subLatestUID, err := s.conn.Subscribe(subjPrefix+".cache.uid.latest", s.handleLatestUID(ctx))
	if err != nil {
		return err
	}
	subInstallSnapshot, err := s.conn.Subscribe(subjPrefix+".install.snapshot", s.handleInstallSnapshot(ctx))
	if err != nil {
		return err
	}
	subTimeoutNow, err := s.conn.Subscribe(subjPrefix+".timeout.now", s.handleTimeoutNow(ctx))
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = subEntries.Unsubscribe()
		_ = subVote.Unsubscribe()
		_ = subStore.Unsubscribe()
		_ = subLatestUID.Unsubscribe()
		_ = subInstallSnapshot.Unsubscribe()
		_ = subTimeoutNow.Unsubscribe()
	}()
	return nil
}

// CacheConsumer returns the cache consumer channel to which all incoming cache commands are sent.
func (s *NATSTransport) CacheConsumer() <-chan raft.RPC {
	return s.chConsumeCache
}

// Store asks the master to apply a change command to the raft cluster.
func (s *NATSTransport) Store(ctx context.Context, _ raft.ServerID, address raft.ServerAddress, request *pb.Store) (*pb.StoreResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	subj := fmt.Sprintf("quasar.%s.%s.cache.store", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(ctx, subj, request, &protoResp); err != nil {
		return nil, err
	}
	return protoResp.GetStore(), nil
}

func (s *NATSTransport) handleStore(ctx context.Context) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var protoMsg pb.Store
		if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
			s.logger.Error("failed to decode incoming command", "error", r)
			return
		}

		// Create the RPC object
		chResp := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			RespChan: chResp,
			Command:  &protoMsg,
		}

		s.chConsumeCache <- rpc

		bts, err := s.awaitResponse(ctx, chResp, func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*pb.StoreResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_Store{
				Store: resp,
			}}
		})
		if err != nil {
			s.logger.Error("failed to send response", "error", err)
			return
		}
		if r := msg.Respond(bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
}

// LatestUID asks the master to return its latest known / generated uid.
func (s *NATSTransport) LatestUID(ctx context.Context, _ raft.ServerID, address raft.ServerAddress,
	request *pb.LatestUid,
) (*pb.LatestUidResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	subj := fmt.Sprintf("quasar.%s.%s.cache.uid.latest", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(ctx, subj, request, &protoResp); err != nil {
		return nil, err
	}
	return protoResp.GetLatestUid(), nil
}

func (s *NATSTransport) handleLatestUID(ctx context.Context) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var protoMsg pb.LatestUid
		if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
			s.logger.Error("failed to decode incoming command", "error", r)
			return
		}

		// Create the RPC object
		chResp := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			RespChan: chResp,
			Command:  &protoMsg,
		}

		s.chConsumeCache <- rpc

		bts, err := s.awaitResponse(ctx, chResp, func(i interface{}) *pb.CommandResponse {
			// TODO: be able to return error if type cast fails
			resp, _ := i.(*pb.LatestUidResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_LatestUid{
				LatestUid: resp,
			}}
		})
		if err != nil {
			s.logger.Error("failed to send response", "error", err)
			return
		}
		if r := msg.Respond(bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
}

// Consumer returns a channel that can be used to
// consume and respond to RPC requests.
func (s *NATSTransport) Consumer() <-chan raft.RPC {
	return s.chConsume
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (s *NATSTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(s.serverName)
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (s *NATSTransport) AppendEntriesPipeline(_ raft.ServerID, _ raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, raft.ErrPipelineReplicationNotSupported
}

// AppendEntries sends the appropriate RPC to the target node.
func (s *NATSTransport) AppendEntries(_ raft.ServerID, address raft.ServerAddress, request *raft.AppendEntriesRequest,
	resp *raft.AppendEntriesResponse,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	subj := fmt.Sprintf("quasar.%s.%s.entries.append", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(ctx, subj, pb.ToAppendEntriesRequest(request), &protoResp); err != nil {
		return err
	}
	*resp = *protoResp.GetAppendEntries().Convert()
	return nil
}

func (s *NATSTransport) handleEntries(ctx context.Context) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var protoMsg pb.AppendEntriesRequest
		if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
			s.logger.Error("failed to decode incoming command", "error", r)
			return
		}

		// Create the RPC object
		chResp := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			RespChan: chResp,
			Command:  protoMsg.Convert(),
		}

		s.chConsume <- rpc

		bts, err := s.awaitResponse(ctx, chResp, func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.AppendEntriesResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_AppendEntries{
				AppendEntries: pb.ToAppendEntriesResponse(resp),
			}}
		})
		if err != nil {
			s.logger.Error("failed to send response", "error", err)
			return
		}
		if r := msg.Respond(bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
}

// RequestVote sends the appropriate RPC to the target node.
func (s *NATSTransport) RequestVote(_ raft.ServerID, address raft.ServerAddress, request *raft.RequestVoteRequest,
	resp *raft.RequestVoteResponse,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	subj := fmt.Sprintf("quasar.%s.%s.request.vote", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(ctx, subj, pb.ToRequestVoteRequest(request), &protoResp); err != nil {
		return err
	}

	*resp = *protoResp.GetRequestVote().Convert()
	return nil
}

func (s *NATSTransport) handleVote(ctx context.Context) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var protoMsg pb.RequestVoteRequest
		if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
			s.logger.Error("failed to decode incoming command", "error", r)
			return
		}
		// fmt.Printf("incoming request: %+v\n", protoMsg)

		// Create the RPC object
		chResp := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			RespChan: chResp,
			Command:  protoMsg.Convert(),
		}

		s.chConsume <- rpc

		bts, err := s.awaitResponse(ctx, chResp, func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.RequestVoteResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_RequestVote{RequestVote: pb.ToRequestVoteResponse(resp)}}
		})
		if err != nil {
			s.logger.Error("error awaiting response", "error", err)
			return
		}

		if r := msg.Respond(bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
}

// EncodePeer is used to serialize a peer's address.
func (s *NATSTransport) EncodePeer(_ raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

// DecodePeer is used to deserialize a peer's address.
func (s *NATSTransport) DecodePeer(bytes []byte) raft.ServerAddress {
	return raft.ServerAddress(bytes)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a transport does not support this, it can simply
// ignore the call, and push the heartbeat onto the Consumer channel.
//
// Implements raft.Transport.
func (s *NATSTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	s.heartbeatFnLock.Lock()
	defer s.heartbeatFnLock.Unlock()
	s.heartbeatFn = cb
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (s *NATSTransport) TimeoutNow(_ raft.ServerID, address raft.ServerAddress, request *raft.TimeoutNowRequest,
	resp *raft.TimeoutNowResponse,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	subj := fmt.Sprintf("quasar.%s.%s.timeout.now", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(ctx, subj, pb.ToTimeoutNowRequest(request), &protoResp); err != nil {
		return err
	}

	*resp = *protoResp.GetTimeoutNow().Convert()
	return nil
}

func (s *NATSTransport) handleTimeoutNow(ctx context.Context) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var protoMsg pb.TimeoutNowRequest
		if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
			s.logger.Error("failed to decode incoming command", "error", r)
			return
		}

		// Create the RPC object
		chResp := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			RespChan: chResp,
			Command:  protoMsg.Convert(),
		}

		s.chConsume <- rpc

		bts, err := s.awaitResponse(ctx, chResp, func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.TimeoutNowResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_TimeoutNow{
				TimeoutNow: pb.ToTimeoutNowResponse(resp),
			}}
		})
		if err != nil {
			s.logger.Error("failed to send response", "error", err)
			return
		}
		if r := msg.Respond(bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
}

func (s *NATSTransport) request(ctx context.Context, subj string, msg, protoResp proto.Message) error {
	bts, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// fmt.Println("request data:", fmt.Sprintf("%+v", msg))
	response, err := s.conn.RequestWithContext(ctx, subj, bts)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(response.Data, protoResp)
	// fmt.Println("response data:", fmt.Sprintf("%+v", protoResp))
	return err
}

func (s *NATSTransport) awaitResponse(
	ctx context.Context,
	ch <-chan raft.RPCResponse,
	toProto func(interface{}) *pb.CommandResponse,
) ([]byte, error) {
	select {
	case resp := <-ch:
		protoResp := &pb.CommandResponse{}
		if resp.Response != nil {
			protoResp = toProto(resp.Response)
		}
		if resp.Error != nil {
			protoResp.Error = resp.Error.Error()
		}

		// fmt.Printf("outgoing response: %+v\n", protoResp)
		bts, err := proto.Marshal(protoResp)
		if err != nil {
			return nil, err
		}

		return bts, nil
	case <-ctx.Done():
		return nil, raft.ErrTransportShutdown
	}
}
