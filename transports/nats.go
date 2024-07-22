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

func NewNATSTransport(ctx context.Context, conn *nats.Conn, cacheName, serverName string, opts ...NATSOption) (*NATSTransport, error) {
	config := getNATSOptions(opts)

	s := &NATSTransport{
		conn:           conn,
		logger:         config.logger,
		serverName:     serverName,
		cacheName:      cacheName,
		ctx:            ctx,
		timeout:        config.timeout,
		chConsume:      make(chan raft.RPC),
		chConsumeCache: make(chan raft.RPC),
	}
	if err := s.listen(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

type NATSTransport struct {
	conn *nats.Conn

	logger hclog.Logger

	serverName string
	cacheName  string
	ctx        context.Context
	timeout    time.Duration

	chConsume      chan raft.RPC
	chConsumeCache chan raft.RPC

	heartbeatFn     func(raft.RPC)
	heartbeatFnLock sync.Mutex
}

func (s *NATSTransport) listen(ctx context.Context) error {
	subjPrefix := fmt.Sprintf("quasar.%s.%s", s.cacheName, s.serverName)
	// fmt.Println("server =", s.serverName, " subjPrefix =", subjPrefix)

	subEntries, err := s.conn.Subscribe(subjPrefix+".entries.append", s.handleEntries)
	if err != nil {
		return err
	}
	subVote, err := s.conn.Subscribe(subjPrefix+".request.vote", s.handleVote)
	if err != nil {
		return err
	}
	subStore, err := s.conn.Subscribe(subjPrefix+".cache.store", s.handleStore)
	if err != nil {
		return err
	}
	subLatestUID, err := s.conn.Subscribe(subjPrefix+".cache.uid.latest", s.handleLatestUID)
	if err != nil {
		return err
	}
	subInstallSnapshot, err := s.conn.Subscribe(subjPrefix+".install.snapshot", s.handleInstallSnapshot)
	if err != nil {
		return err
	}
	subTimeoutNow, err := s.conn.Subscribe(subjPrefix+".timeout.now", s.handleTimeoutNow)
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

func (s *NATSTransport) CacheConsumer() <-chan raft.RPC {
	return s.chConsumeCache
}

func (s *NATSTransport) Store(_ raft.ServerID, address raft.ServerAddress, request *pb.Store) (*pb.StoreResponse, error) {
	subj := fmt.Sprintf("quasar.%s.%s.cache.store", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(subj, request, &protoResp); err != nil {
		return nil, err
	}
	return protoResp.GetStore(), nil
}

func (s *NATSTransport) handleStore(msg *nats.Msg) {
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

	bts, err := s.awaitResponse(chResp, func(i interface{}) *pb.CommandResponse {
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

func (s *NATSTransport) LatestUID(_ raft.ServerID, address raft.ServerAddress, request *pb.LatestUid) (*pb.LatestUidResponse, error) {
	subj := fmt.Sprintf("quasar.%s.%s.cache.uid.latest", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(subj, request, &protoResp); err != nil {
		return nil, err
	}
	return protoResp.GetLatestUid(), nil
}

func (s *NATSTransport) handleLatestUID(msg *nats.Msg) {
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

	bts, err := s.awaitResponse(chResp, func(i interface{}) *pb.CommandResponse {
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

func (s *NATSTransport) Consumer() <-chan raft.RPC {
	return s.chConsume
}

func (s *NATSTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(s.serverName)
}

func (s *NATSTransport) AppendEntriesPipeline(_ raft.ServerID, _ raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, raft.ErrPipelineReplicationNotSupported
}

func (s *NATSTransport) AppendEntries(_ raft.ServerID, address raft.ServerAddress, request *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	subj := fmt.Sprintf("quasar.%s.%s.entries.append", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(subj, pb.ToAppendEntriesRequest(request), &protoResp); err != nil {
		return err
	}
	*resp = *protoResp.GetAppendEntries().Convert()
	return nil
}

func (s *NATSTransport) handleEntries(msg *nats.Msg) {
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

	bts, err := s.awaitResponse(chResp, func(i interface{}) *pb.CommandResponse {
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

func (s *NATSTransport) RequestVote(_ raft.ServerID, address raft.ServerAddress, request *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	subj := fmt.Sprintf("quasar.%s.%s.request.vote", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(subj, pb.ToRequestVoteRequest(request), &protoResp); err != nil {
		return err
	}

	*resp = *protoResp.GetRequestVote().Convert()
	return nil
}

func (s *NATSTransport) handleVote(msg *nats.Msg) {
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

	bts, err := s.awaitResponse(chResp, func(i interface{}) *pb.CommandResponse {
		resp, _ := i.(*raft.RequestVoteResponse)
		return &pb.CommandResponse{Resp: &pb.CommandResponse_RequestVote{RequestVote: pb.ToRequestVoteResponse(resp)}}
	})

	if err != nil {
		s.logger.Error("failed to send response", "error", err)
		return
	}
	if r := msg.Respond(bts); r != nil {
		s.logger.Error("failed to send response", "error", r)
	}
}

func (s *NATSTransport) EncodePeer(_ raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (s *NATSTransport) DecodePeer(bytes []byte) raft.ServerAddress {
	return raft.ServerAddress(bytes)
}

func (s *NATSTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	s.heartbeatFnLock.Lock()
	defer s.heartbeatFnLock.Unlock()
	s.heartbeatFn = cb
}

func (s *NATSTransport) TimeoutNow(_ raft.ServerID, address raft.ServerAddress, request *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	subj := fmt.Sprintf("quasar.%s.%s.timeout.now", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.request(subj, pb.ToTimeoutNowRequest(request), &protoResp); err != nil {
		return err
	}

	*resp = *protoResp.GetTimeoutNow().Convert()
	return nil
}

func (s *NATSTransport) handleTimeoutNow(msg *nats.Msg) {
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

	bts, err := s.awaitResponse(chResp, func(i interface{}) *pb.CommandResponse {
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

func (s *NATSTransport) request(subj string, msg proto.Message, protoResp proto.Message) error {
	bts, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), s.timeout)
	defer cancel()

	// fmt.Println("request data:", fmt.Sprintf("%+v", msg))
	response, err := s.conn.RequestWithContext(ctx, subj, bts)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(response.Data, protoResp)
	// fmt.Println("response data:", fmt.Sprintf("%+v", protoResp))
	return err
}

func (s *NATSTransport) awaitResponse(ch <-chan raft.RPCResponse, toProto func(interface{}) *pb.CommandResponse) ([]byte, error) {
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
	case <-s.ctx.Done():
		return nil, raft.ErrTransportShutdown
	}
}
