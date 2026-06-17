package transports

import (
	"context"
	"errors"
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
	base10            = 10
)

var (
	_ Transport        = (*NATSTransport)(nil)
	_ raft.Transport   = (*NATSTransport)(nil)
	_ raft.WithPreVote = (*NATSTransport)(nil)
)

// NewNATSTransport creates a new NATS based transport.
func NewNATSTransport(ctx context.Context, conn *nats.Conn, cacheName, serverName string, opts ...NATSOption) (*NATSTransport, error) {
	config := getNATSOptions(opts)

	s := &NATSTransport{
		conn:             conn,
		logger:           config.logger,
		serverName:       serverName,
		cacheName:        cacheName,
		timeout:          config.timeout,
		heartbeatTimeout: config.heartbeatTimeout,
		maxMsgSize:       config.maxMsgSize,
		chConsume:        make(chan raft.RPC, consumerChanSize),
		chConsumeCache:   make(chan raft.RPC, consumerChanSize),
	}
	if config.persistedQueue != nil {
		q, err := newNatsPersistedQueue(conn, cacheName, *config.persistedQueue)
		if err != nil {
			return nil, err
		}
		s.queue = q
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

	serverName       string
	cacheName        string
	timeout          time.Duration
	heartbeatTimeout time.Duration

	chConsume      chan raft.RPC
	chConsumeCache chan raft.RPC

	heartbeatFn     func(raft.RPC)
	heartbeatFnLock sync.Mutex

	requestIDCounter uint64
	maxMsgSize       int

	// queue is the optional persisted-FIFO mode binding. Set by
	// WithNATSPersistedQueue and resolved against the connection on
	// the first call to StorePersisted / StartPersistedConsumer. nil
	// when the transport was constructed without persisted-FIFO.
	queue *natsPersistedQueue
}

func (s *NATSTransport) listen(ctx context.Context) error {
	subjPrefix := fmt.Sprintf("quasar.%s.%s", s.cacheName, s.serverName)
	// fmt.Println("server =", s.serverName, " subjPrefix =", subjPrefix)

	subEntries, err := s.conn.Subscribe(subjPrefix+".entries.append", s.handleEntries(ctx))
	if err != nil {
		return err
	}
	subHeartbeat, err := s.conn.Subscribe(subjPrefix+".entries.heartbeat", s.handleHeartbeat(ctx))
	if err != nil {
		return err
	}
	subVote, err := s.conn.Subscribe(subjPrefix+".request.vote", s.handleVote(ctx))
	if err != nil {
		return err
	}
	subPreVote, err := s.conn.Subscribe(subjPrefix+".request.prevote", s.handlePreVote(ctx))
	if err != nil {
		return err
	}
	subStore, err := s.conn.Subscribe(subjPrefix+".cache.store", s.handleStore(ctx))
	if err != nil {
		return err
	}
	subResetCache, err := s.conn.Subscribe(subjPrefix+".cache.reset", s.handleResetCache(ctx))
	if err != nil {
		return err
	}
	subRemoveServer, err := s.conn.Subscribe(subjPrefix+".cache.server.remove", s.handleRemoveServer(ctx))
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
		_ = subHeartbeat.Unsubscribe()
		_ = subVote.Unsubscribe()
		_ = subPreVote.Unsubscribe()
		_ = subStore.Unsubscribe()
		_ = subResetCache.Unsubscribe()
		_ = subRemoveServer.Unsubscribe()
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

// rpcHandler builds the NATS subscription callback for one cache- or raft-RPC.
// The cache handlers (Store, ResetCache, RemoveServer, LatestUID) and raft
// handlers (Vote, PreVote, TimeoutNow, AppendEntries) all share the same body —
// decode the payload, push a raft.RPC onto a consumer channel, await the
// response and reply — and previously differed only in proto request type,
// whether the decoded message is Convert()ed to a raft struct, which consumer
// channel they feed, and the CommandResponse oneof wrapper (RT-13042 S1).
//
// newReq allocates a fresh request message; toCommand turns it into the
// raft.RPC command (identity for cache RPCs, Convert() for raft RPCs); toResp
// wraps the consumer's response in the matching CommandResponse oneof. When
// assembler is non-nil the payload is first reassembled from multi-part NATS
// messages (Store, AppendEntries); otherwise msg.Data is single-part.
func rpcHandler[Req proto.Message](
	s *NATSTransport,
	ctx context.Context,
	ch chan raft.RPC,
	assembler *multipartAssembler,
	newReq func() Req,
	toCommand func(Req) interface{},
	toResp func(interface{}) *pb.CommandResponse,
) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		data := msg.Data
		if assembler != nil {
			var (
				complete bool
				err      error
			)
			if data, complete, err = assembler.handle(msg); err != nil {
				s.handleError(msg, fmt.Errorf("failed to handle multi-part message: %w", err))
				return
			}
			if !complete {
				return
			}
		}

		req := newReq()
		if r := proto.Unmarshal(data, req); r != nil {
			s.handleError(msg, fmt.Errorf("failed to decode incoming command: %w", r))
			return
		}

		chResp := make(chan raft.RPCResponse, 1)
		ch <- raft.RPC{
			RespChan: chResp,
			Command:  toCommand(req),
		}

		bts, err := s.awaitResponse(ctx, chResp, toResp)
		if err != nil {
			s.handleError(msg, fmt.Errorf("failed to consume message: %w", err))
			return
		}
		if r := msg.Respond(bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
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
	if _, err := s.request(ctx, subj, request, &protoResp); err != nil {
		return nil, err
	}
	if errStr := protoResp.GetError(); errStr != "" {
		return nil, errors.New(errStr)
	}
	return protoResp.GetStore(), nil
}

func (s *NATSTransport) handleStore(ctx context.Context) func(*nats.Msg) {
	// Reassembly is keyed by the sender-qualified request_id: unlike
	// AppendEntries (one leader, sequential), ANY node — and multiple
	// goroutines per node — may forward large Stores to this subject
	// concurrently, so interleaved parts must not share one buffer
	// (RT-13042 M3).
	return rpcHandler(s, ctx, s.chConsumeCache, newMultipartAssembler(),
		func() *pb.Store { return &pb.Store{} },
		func(r *pb.Store) interface{} { return r },
		func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*pb.StoreResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_Store{Store: resp}}
		})
}

// ResetCache asks the master to reset the cache.
func (s *NATSTransport) ResetCache(ctx context.Context, _ raft.ServerID, address raft.ServerAddress,
	request *pb.ResetCache,
) (*pb.ResetCacheResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	subj := fmt.Sprintf("quasar.%s.%s.cache.reset", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.requestSmall(ctx, subj, request, &protoResp); err != nil {
		return nil, err
	}
	if errStr := protoResp.GetError(); errStr != "" {
		return nil, errors.New(errStr)
	}

	return protoResp.GetResetCache(), nil
}

// RemoveServer asks the leader to remove a server from the raft configuration.
func (s *NATSTransport) RemoveServer(ctx context.Context, _ raft.ServerID, address raft.ServerAddress,
	request *pb.RemoveServer,
) (*pb.RemoveServerResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	subj := fmt.Sprintf("quasar.%s.%s.cache.server.remove", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.requestSmall(ctx, subj, request, &protoResp); err != nil {
		return nil, err
	}
	if errStr := protoResp.GetError(); errStr != "" {
		return nil, errors.New(errStr)
	}

	return protoResp.GetRemoveServer(), nil
}

func (s *NATSTransport) handleResetCache(ctx context.Context) func(*nats.Msg) {
	return rpcHandler(s, ctx, s.chConsumeCache, nil,
		func() *pb.ResetCache { return &pb.ResetCache{} },
		func(r *pb.ResetCache) interface{} { return r },
		func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*pb.ResetCacheResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_ResetCache{ResetCache: resp}}
		})
}

func (s *NATSTransport) handleRemoveServer(ctx context.Context) func(*nats.Msg) {
	return rpcHandler(s, ctx, s.chConsumeCache, nil,
		func() *pb.RemoveServer { return &pb.RemoveServer{} },
		func(r *pb.RemoveServer) interface{} { return r },
		func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*pb.RemoveServerResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_RemoveServer{RemoveServer: resp}}
		})
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
	if err := s.requestSmall(ctx, subj, request, &protoResp); err != nil {
		return nil, err
	}
	if errStr := protoResp.GetError(); errStr != "" {
		return nil, errors.New(errStr)
	}
	return protoResp.GetLatestUid(), nil
}

func (s *NATSTransport) handleLatestUID(ctx context.Context) func(*nats.Msg) {
	return rpcHandler(s, ctx, s.chConsumeCache, nil,
		func() *pb.LatestUid { return &pb.LatestUid{} },
		func(r *pb.LatestUid) interface{} { return r },
		func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*pb.LatestUidResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_LatestUid{LatestUid: resp}}
		})
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

	if isHeartbeat(request) {
		// Try the dedicated heartbeat subject first so the round-trip
		// can't be blocked by a slow chConsume push on entries.append.
		//
		// Bound this attempt by heartbeatTimeout (≈ raft's HeartbeatTimeout)
		// rather than the full request timeout: a peer can be subscribed to
		// entries.heartbeat yet never answer -- e.g. its heartbeat fast-path
		// (raft SetHeartbeatHandler) is wired to a raft instance that was
		// torn down during a reinit/quorum-recovery, so processHeartbeat
		// returns on a closed shutdownCh without responding. On ANY failure
		// -- ErrNoResponders (older peer without the subject) or a
		// timeout/transport error -- fall through to entries.append, which is
		// serviced by the live raft's consumer loop and answers normally.
		// Without this fall-through the leader would log "failed to heartbeat"
		// every beat forever and the follower would never converge, even
		// though replication on entries.append is healthy (RT-13010).
		hbCtx, hbCancel := ctx, context.CancelFunc(func() {})
		if s.heartbeatTimeout > 0 {
			hbCtx, hbCancel = context.WithTimeout(ctx, s.heartbeatTimeout)
		}
		hbSubj := fmt.Sprintf("quasar.%s.%s.entries.heartbeat", s.cacheName, address)
		var protoResp pb.CommandResponse
		err := s.requestSmall(hbCtx, hbSubj, pb.ToAppendEntriesRequest(request), &protoResp)
		hbCancel()
		if err == nil {
			payload, rErr := checkRaftResponse(&protoResp, (*pb.CommandResponse).GetAppendEntries)
			if rErr == nil {
				*resp = *payload.Convert()
				return nil
			}
			// Payload-level failure on the heartbeat subject: fall back to
			// entries.append like any other heartbeat failure.
			err = rErr
		}
		if !errors.Is(err, nats.ErrNoResponders) {
			// Subscribed but unanswered: don't surface this at error level
			// (it would just re-create the per-beat log flood). Fall back to
			// entries.append below.
			s.logger.Debug("heartbeat subject did not answer; falling back to entries.append",
				"error", err, "peer", address)
		}
	}

	subj := fmt.Sprintf("quasar.%s.%s.entries.append", s.cacheName, address)

	var protoResp pb.CommandResponse
	if size, err := s.request(ctx, subj, pb.ToAppendEntriesRequest(request), &protoResp); err != nil {
		s.logger.Error("failed to send append entries request", "error", err, "size", size, "entries", len(request.Entries))
		return err
	}
	payload, err := checkRaftResponse(&protoResp, (*pb.CommandResponse).GetAppendEntries)
	if err != nil {
		return err
	}
	*resp = *payload.Convert()
	return nil
}

func (s *NATSTransport) handleEntries(ctx context.Context) func(*nats.Msg) {
	return rpcHandler(s, ctx, s.chConsume, newMultipartAssembler(),
		func() *pb.AppendEntriesRequest { return &pb.AppendEntriesRequest{} },
		func(r *pb.AppendEntriesRequest) interface{} { return r.Convert() },
		func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.AppendEntriesResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_AppendEntries{AppendEntries: pb.ToAppendEntriesResponse(resp)}}
		})
}

// isHeartbeat reports whether req is a hashicorp/raft heartbeat
// AppendEntries — i.e. one carrying no log entries and no probe-back state.
// Mirrors the check in hashicorp/raft's net_transport.go.
func isHeartbeat(req *raft.AppendEntriesRequest) bool {
	leaderAddr := req.RPCHeader.Addr
	if len(leaderAddr) == 0 {
		//nolint:staticcheck // backwards compatibility with the deprecated Leader field.
		leaderAddr = req.Leader
	}
	return req.Term != 0 && leaderAddr != nil &&
		req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
		len(req.Entries) == 0 && req.LeaderCommitIndex == 0
}

// handleHeartbeat handles AppendEntries delivered on the dedicated heartbeat
// subscription. Because this is a distinct NATS subscription, callbacks run
// on a goroutine separate from handleEntries — so a slow chConsume push for
// regular entries cannot stall heartbeat round-trips.
func (s *NATSTransport) handleHeartbeat(ctx context.Context) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		var protoMsg pb.AppendEntriesRequest
		if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
			s.handleError(msg, fmt.Errorf("failed to decode incoming command: %w", r))
			return
		}

		chResp := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			RespChan: chResp,
			Command:  protoMsg.Convert(),
		}

		s.heartbeatFnLock.Lock()
		fn := s.heartbeatFn
		s.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
		} else {
			// No live raft is bound: either initial wiring before the first
			// raft registers, or the window between a reinit's Shutdown and the
			// new raft's rebind. Route to the consumer so the beat reaches the
			// live raft (once it drains Consumer()) instead of being dropped.
			s.chConsume <- rpc
		}

		waitCtx, cancel := s.respCtx(ctx)
		bts, err := s.awaitResponse(waitCtx, chResp, func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.AppendEntriesResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_AppendEntries{
				AppendEntries: pb.ToAppendEntriesResponse(resp),
			}}
		})
		cancel()
		if err != nil {
			s.handleError(msg, fmt.Errorf("failed to consume message: %w", err))
			return
		}
		if r := msg.Respond(bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
}

func (s *NATSTransport) respCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	// Bound the response wait. Callbacks on this subscription run serially
	// on a single NATS dispatcher goroutine, and raft's processHeartbeat
	// returns WITHOUT responding once its shutdownCh is closed — so a beat
	// handed to a raft torn down mid-reinit would otherwise park the
	// dispatcher forever, permanently wedging the heartbeat subject for
	// this transport instance (RT-13042). A heartbeat that can't be
	// answered within the heartbeat window is worthless anyway: the sender
	// has already fallen back to entries.append.
	waitTimeout := s.heartbeatTimeout
	if waitTimeout <= 0 {
		waitTimeout = s.timeout
	}
	if waitTimeout > 0 {
		return context.WithTimeout(ctx, waitTimeout)
	}
	return ctx, func() {}
}

// RequestVote sends the appropriate RPC to the target node.
func (s *NATSTransport) RequestVote(_ raft.ServerID, address raft.ServerAddress, request *raft.RequestVoteRequest,
	resp *raft.RequestVoteResponse,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	subj := fmt.Sprintf("quasar.%s.%s.request.vote", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.requestSmall(ctx, subj, pb.ToRequestVoteRequest(request), &protoResp); err != nil {
		return err
	}

	payload, err := checkRaftResponse(&protoResp, (*pb.CommandResponse).GetRequestVote)
	if err != nil {
		return err
	}
	*resp = *payload.Convert()
	return nil
}

// RequestPreVote sends the appropriate RPC to the target node.
//
// Implements raft.WithPreVote.
func (s *NATSTransport) RequestPreVote(_ raft.ServerID, address raft.ServerAddress, request *raft.RequestPreVoteRequest,
	resp *raft.RequestPreVoteResponse,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	subj := fmt.Sprintf("quasar.%s.%s.request.prevote", s.cacheName, address)

	var protoResp pb.CommandResponse
	if err := s.requestSmall(ctx, subj, pb.ToRequestPreVoteRequest(request), &protoResp); err != nil {
		return err
	}

	payload, err := checkRaftResponse(&protoResp, (*pb.CommandResponse).GetRequestPreVote)
	if err != nil {
		return err
	}
	*resp = *payload.Convert()
	return nil
}

func (s *NATSTransport) handlePreVote(ctx context.Context) func(*nats.Msg) {
	return rpcHandler(s, ctx, s.chConsume, nil,
		func() *pb.RequestPreVoteRequest { return &pb.RequestPreVoteRequest{} },
		func(r *pb.RequestPreVoteRequest) interface{} { return r.Convert() },
		func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.RequestPreVoteResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_RequestPreVote{RequestPreVote: pb.ToRequestPreVoteResponse(resp)}}
		})
}

func (s *NATSTransport) handleVote(ctx context.Context) func(*nats.Msg) {
	return rpcHandler(s, ctx, s.chConsume, nil,
		func() *pb.RequestVoteRequest { return &pb.RequestVoteRequest{} },
		func(r *pb.RequestVoteRequest) interface{} { return r.Convert() },
		func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.RequestVoteResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_RequestVote{RequestVote: pb.ToRequestVoteResponse(resp)}}
		})
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
	if err := s.requestSmall(ctx, subj, pb.ToTimeoutNowRequest(request), &protoResp); err != nil {
		return err
	}

	payload, err := checkRaftResponse(&protoResp, (*pb.CommandResponse).GetTimeoutNow)
	if err != nil {
		return err
	}
	*resp = *payload.Convert()
	return nil
}

// SupportsPersisted reports whether this NATSTransport instance has been
// configured with a persisted-FIFO mode via WithNATSPersistedQueue. When
// false, the cache uses the synchronous Store RPC path; missing leader
// returns ErrNoLeader. When true, every Store flows through the JS
// work-queue stream and the queue itself handles leaderless windows.
func (s *NATSTransport) SupportsPersisted() bool {
	return s.queue != nil
}

// StorePersisted publishes a Store command into the JS work-queue stream
// and waits for the leader's reply via a NATS request-reply inbox.
// Returns ErrPersistedNotSupported when the transport wasn't constructed
// with WithNATSPersistedQueue.
func (s *NATSTransport) StorePersisted(ctx context.Context, command *pb.Store, opts PersistedStoreOpts) (*pb.StoreResponse, error) {
	if s.queue == nil {
		return nil, ErrPersistedNotSupported
	}
	return s.queue.publish(ctx, command, opts)
}

// StartPersistedConsumer begins draining the persisted-FIFO stream on
// this node. Called by the cache when this node becomes leader.
func (s *NATSTransport) StartPersistedConsumer(ctx context.Context) (<-chan PersistedItem, error) {
	if s.queue == nil {
		return nil, ErrPersistedNotSupported
	}
	return s.queue.startConsumer(ctx)
}

// StopPersistedConsumer stops the consumer started by
// StartPersistedConsumer and NAKs the in-flight item so the next leader
// picks it up without waiting for AckWait to elapse.
func (s *NATSTransport) StopPersistedConsumer() error {
	if s.queue == nil {
		return nil
	}
	return s.queue.stopConsumer()
}

func (s *NATSTransport) handleTimeoutNow(ctx context.Context) func(*nats.Msg) {
	return rpcHandler(s, ctx, s.chConsume, nil,
		func() *pb.TimeoutNowRequest { return &pb.TimeoutNowRequest{} },
		func(r *pb.TimeoutNowRequest) interface{} { return r.Convert() },
		func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.TimeoutNowResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_TimeoutNow{TimeoutNow: pb.ToTimeoutNowResponse(resp)}}
		})
}

// request sends msg on subj, splitting it across multiple NATS messages if
// it exceeds s.maxMsgSize. Use this for RPCs whose payload can grow large
// (AppendEntries, Store).
func (s *NATSTransport) request(ctx context.Context, subj string, msg, protoResp proto.Message) (int, error) {
	bts, err := proto.Marshal(msg)
	if err != nil {
		return 0, err
	}

	// Split the message if it is too large and build the last message part.
	reqMsg := nats.NewMsg(subj)
	if s.maxMsgSize < len(bts) {
		var (
			requestIDStr  string
			lastPartIndex int
		)
		bts, requestIDStr, lastPartIndex, err = s.publishMultiPart(subj, bts)
		if err != nil {
			return 0, err
		}
		reqMsg.Header.Set("request_id", requestIDStr)
		reqMsg.Header.Set("pkg_part", fmt.Sprintf("%d", lastPartIndex))
	}
	reqMsg.Data = bts

	// Send the prepared message — NOT the raw bytes. The final part of a
	// multi-part request carries the request_id / pkg_part headers set
	// above; sending raw bytes would strip them, so the receiver could
	// neither validate the final part against the assembly nor detect a
	// lost middle part (RT-13042 M2).
	response, err := s.conn.RequestMsgWithContext(ctx, reqMsg)
	if err != nil {
		return len(bts), err
	}

	err = proto.Unmarshal(response.Data, protoResp)
	// fmt.Println("response data:", fmt.Sprintf("%+v", protoResp))
	return len(bts), err
}

// checkRaftResponse validates the CommandResponse envelope of a raft RPC and
// returns its concrete payload. Handlers reply with CommandResponse{Error: …}
// on decode / multipart / consume failures; the raft RPC callers used to
// ignore that envelope and convert the nil oneof into a zero-valued response
// (e.g. AppendEntriesResponse{Term: 0, Success: false}) — surfacing a
// payload-level failure as an endless nextIndex-decrement retry loop, or as
// a fabricated "vote not granted at term 0", instead of an explicit
// transport error (RT-13042 M6).
func checkRaftResponse[T any](protoResp *pb.CommandResponse, get func(*pb.CommandResponse) *T) (*T, error) {
	if errStr := protoResp.GetError(); errStr != "" {
		return nil, errors.New(errStr)
	}
	payload := get(protoResp)
	if payload == nil {
		return nil, errors.New("raft rpc response missing expected payload")
	}
	return payload, nil
}

// requestSmall sends msg on subj as a single NATS message. Use this for RPCs
// whose payload is bounded by a few small fields (RequestVote, TimeoutNow,
// LatestUid, ResetCache, RemoveServer, heartbeat AppendEntries) — the
// multi-part path in request is dead code for them.
func (s *NATSTransport) requestSmall(ctx context.Context, subj string, msg, protoResp proto.Message) error {
	bts, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	response, err := s.conn.RequestWithContext(ctx, subj, bts)
	if err != nil {
		return err
	}
	return proto.Unmarshal(response.Data, protoResp)
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

func (s *NATSTransport) handleError(msg *nats.Msg, err error) {
	if err == nil {
		s.logger.Error("handleError: no error to handle")
		return
	}

	s.logger.Error("failed to handle request", "error", err, "subject", msg.Subject)

	resp := &pb.CommandResponse{Error: err.Error()}
	bts, err := proto.Marshal(resp)
	if err != nil {
		s.logger.Error("failed to marshal error response", "error", err, "subject", msg.Subject)
		return
	}
	if r := msg.Respond(bts); r != nil {
		s.logger.Error("failed to send error response", "error", r, "subject", msg.Subject)
		return
	}
	return
}
