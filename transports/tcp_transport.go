package transports

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
)

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow
	rpcStore
	rpcLatestUID
	// rpcLoad

	// connReceiveBufferSize is the size of the buffer we will use for reading RPC requests into
	// on followers
	connReceiveBufferSize = 256 * 1024 // 256KB

	// connSendBufferSize is the size of the buffer we will use for sending RPC request data from
	// the leader to followers.
	connSendBufferSize = 256 * 1024 // 256KB

	// minInFlightForPipelining is a property of our current pipelining
	// implementation and must not be changed unless we change the invariants of
	// that implementation. Roughly speaking even with a zero-length in-flight
	// buffer we still allow 2 requests to be in-flight before we block because we
	// only block after sending and the receiving go-routine always unblocks the
	// chan right after first send. This is a constant just to provide context
	// rather than a magic number in a few places we have to check invariants to
	// avoid panics etc.
	minInFlightForPipelining = 2
)

// newTPCTransport creates a network transport layer for the cache.
func newTPCTransport(stream raft.StreamLayer, opts ...TCPOption) *TCPTransport {
	config := getConfig(stream, getTCPOptions(opts))

	trans := &TCPTransport{
		connPool:              make(map[raft.ServerAddress][]*netConn),
		chConsume:             make(chan raft.RPC),
		chConsumeCache:        make(chan raft.RPC),
		logger:                config.Logger,
		maxPool:               config.MaxPool,
		maxInFlight:           config.MaxRPCsInFlight,
		shutdownCh:            make(chan struct{}),
		stream:                config.Stream,
		timeout:               config.Timeout,
		TimeoutScale:          raft.DefaultTimeoutScale,
		serverAddressProvider: config.ServerAddressProvider,
	}

	// Create the connection context and then start our listener.
	trans.setupStreamContext()
	go trans.listen()

	return trans
}

func getConfig(stream raft.StreamLayer, cfg tcpOptions) *raft.NetworkTransportConfig {
	config := cfg.config
	if config == nil {
		config = &raft.NetworkTransportConfig{
			MaxPool: cfg.maxPool,
			Timeout: cfg.timeout,
			Logger:  cfg.logger,
		}
	}

	config.Stream = stream
	if config.MaxRPCsInFlight == 0 {
		config.MaxRPCsInFlight = raft.DefaultMaxRPCsInFlight
	}
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",
			Output: cfg.output,
			Level:  hclog.DefaultLevel,
		})
	}

	return config
}

var (
	_ Transport      = (*TCPTransport)(nil)
	_ raft.Transport = (*TCPTransport)(nil)
)

// TCPTransport provides a network based transport that can be
// used to communicate with Raft on remote machines. It requires
// an underlying stream layer to provide a stream abstraction, which can
// be simple TCP, TLS, etc.
//
// This transport is very simple and lightweight. Each RPC request is
// framed by sending a byte that indicates the message type, followed
// by the MsgPack encoded request.
//
// The response is an error string followed by the response object,
// both are encoded using MsgPack.
//
// InstallSnapshot is special, in that after the RPC request we stream
// the entire state. That socket is not re-used as the connection state
// is not known if there is an error.
type TCPTransport struct {
	connPool     map[raft.ServerAddress][]*netConn
	connPoolLock sync.Mutex

	chConsume      chan raft.RPC
	chConsumeCache chan raft.RPC

	heartbeatFn     func(raft.RPC)
	heartbeatFnLock sync.Mutex

	logger hclog.Logger

	maxPool     int
	maxInFlight int

	serverAddressProvider raft.ServerAddressProvider

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	stream raft.StreamLayer

	// streamCtx is used to cancel existing connection handlers.
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	timeout      time.Duration
	TimeoutScale int
}

func (s *TCPTransport) Store(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.Store) (*pb.StoreResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	var resp pb.StoreResponse
	err := s.genericRPC(ctx, id, target, rpcStore, command, &resp)
	return &resp, err
}

func (s *TCPTransport) LatestUID(ctx context.Context, id raft.ServerID, target raft.ServerAddress, command *pb.LatestUid) (*pb.LatestUidResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	var resp pb.LatestUidResponse
	err := s.genericRPC(ctx, id, target, rpcLatestUID, command, &resp)
	return &resp, err
}

// func (s *TCPTransport) Load(id raft.ServerID, target raft.ServerAddress, command *pb.LoadValue) (*pb.LoadValueResponse, error) {
// 	var resp pb.LoadValueResponse
// 	err := s.genericRPC(id, target, rpcLoad, command, &resp)
// 	return &resp, err
// }

// SetHeartbeatHandler is used to set up a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
func (s *TCPTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	s.heartbeatFnLock.Lock()
	defer s.heartbeatFnLock.Unlock()
	s.heartbeatFn = cb
}

// CloseStreams closes the current streams.
func (s *TCPTransport) CloseStreams() {
	s.connPoolLock.Lock()
	defer s.connPoolLock.Unlock()

	// Close all the connections in the connection pool and then remove their
	// entry.
	for k, e := range s.connPool {
		for _, conn := range e {
			_ = conn.Release()
		}

		delete(s.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	s.streamCtxLock.Lock()
	s.streamCancel()
	s.setupStreamContext()
	s.streamCtxLock.Unlock()
}

// Close is used to stop the network transport.
func (s *TCPTransport) Close() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if !s.shutdown {
		close(s.shutdownCh)
		s.stream.Close()
		s.shutdown = true
	}
	return nil
}

// Consumer implements the raft.Transport interface.
func (s *TCPTransport) Consumer() <-chan raft.RPC {
	return s.chConsume
}

// CacheConsumer implements the Transport interface.
func (s *TCPTransport) CacheConsumer() <-chan raft.RPC {
	return s.chConsumeCache
}

// LocalAddr implements the Transport interface.
func (s *TCPTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(s.stream.Addr().String())
}

// IsShutdown is used to check if the transport is shutdown.
func (s *TCPTransport) IsShutdown() bool {
	select {
	case <-s.shutdownCh:
		return true
	default:
		return false
	}
}

// getExistingConn is used to grab a pooled connection.
func (s *TCPTransport) getPooledConn(target raft.ServerAddress) *netConn {
	s.connPoolLock.Lock()
	defer s.connPoolLock.Unlock()

	conns, ok := s.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	s.connPool[target] = conns[:num-1]
	return conn
}

// getConnFromAddressProvider returns a connection from the server address provider if available, or defaults to a connection using the target server address
func (s *TCPTransport) getConnFromAddressProvider(id raft.ServerID, target raft.ServerAddress) (*netConn, error) {
	address := s.getProviderAddressOrFallback(id, target)
	return s.getConn(address)
}

func (s *TCPTransport) getProviderAddressOrFallback(id raft.ServerID, target raft.ServerAddress) raft.ServerAddress {
	if s.serverAddressProvider != nil {
		serverAddressOverride, err := s.serverAddressProvider.ServerAddr(id)
		if err != nil {
			s.logger.Warn("unable to get address for server, using fallback address", "id", id, "fallback", target, "error", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

// getConn is used to get a connection from the pool.
func (s *TCPTransport) getConn(target raft.ServerAddress) (*netConn, error) {
	// Check for a pooled conn
	if conn := s.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new connection
	conn, err := s.stream.Dial(target, s.timeout)
	if err != nil {
		return nil, err
	}

	// Wrap the conn
	netConn := &netConn{
		target: target,
		conn:   conn,
		dec:    codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{}),
		w:      bufio.NewWriterSize(conn, connSendBufferSize),
	}

	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{})

	// Done
	return netConn, nil
}

// returnConn returns a connection back to the pool.
func (s *TCPTransport) returnConn(conn *netConn) {
	s.connPoolLock.Lock()
	defer s.connPoolLock.Unlock()

	key := conn.target
	conns := s.connPool[key]

	if !s.IsShutdown() && len(conns) < s.maxPool {
		s.connPool[key] = append(conns, conn)
	} else {
		_ = conn.Release()
	}
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (s *TCPTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if s.maxInFlight < minInFlightForPipelining {
		// Pipelining is disabled since no more than one request can be outstanding
		// at once. Skip the whole code path and use synchronous requests.
		return nil, raft.ErrPipelineReplicationNotSupported
	}

	// Get a connection
	conn, err := s.getConnFromAddressProvider(id, target)
	if err != nil {
		return nil, err
	}

	// Create the pipeline
	return newNetPipeline(s, conn, s.maxInFlight), nil
}

// AppendEntries implements the Transport interface.
func (s *TCPTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	return s.genericRPC(ctx, id, target, rpcAppendEntries, args, resp)
}

// RequestVote implements the Transport interface.
func (s *TCPTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	return s.genericRPC(ctx, id, target, rpcRequestVote, args, resp)
}

// genericRPC handles a simple request/response RPC.
func (s *TCPTransport) genericRPC(ctx context.Context, id raft.ServerID, target raft.ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	// Get a conn
	conn, err := s.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	if deadline, ok := getDeadline(ctx, s.timeout); ok {
		_ = conn.conn.SetDeadline(deadline)
	}

	// Send the RPC
	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	// Decode the response
	canReturn, err := decodeResponse(conn, resp)
	if canReturn {
		s.returnConn(conn)
	}
	if err != nil && strings.HasSuffix(err.Error(), "i/o timeout") {
		return fmt.Errorf("rpc: no response received: %w", err)
	}
	return err
}

// InstallSnapshot implements the Transport interface.
func (s *TCPTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	// Get a conn, always close for InstallSnapshot
	conn, err := s.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Release() }()

	// Set a deadline, scaled by request size
	if s.timeout > 0 {
		timeout := s.timeout * time.Duration(args.Size/int64(s.TimeoutScale))
		if timeout < s.timeout {
			timeout = s.timeout
		}
		_ = conn.conn.SetDeadline(time.Now().Add(timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, rpcInstallSnapshot, args); err != nil {
		return err
	}

	// Stream the state
	if _, err = io.Copy(conn.w, data); err != nil {
		return err
	}

	// Flush
	if err = conn.w.Flush(); err != nil {
		return err
	}

	// Decode the response, do not return conn
	_, err = decodeResponse(conn, resp)
	return err
}

// EncodePeer implements the Transport interface.
func (s *TCPTransport) EncodePeer(id raft.ServerID, p raft.ServerAddress) []byte {
	address := s.getProviderAddressOrFallback(id, p)
	return []byte(address)
}

// DecodePeer implements the Transport interface.
func (s *TCPTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

// TimeoutNow implements the Transport interface.
func (s *TCPTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	return s.genericRPC(ctx, id, target, rpcTimeoutNow, args, resp)
}

// listen is used to handling incoming connections.
func (s *TCPTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// Accept incoming connections
		conn, err := s.stream.Accept()
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			if !s.IsShutdown() {
				s.logger.Error("failed to accept connection", "error", err)
			}

			select {
			case <-s.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}
		// No error, reset loop delay
		loopDelay = 0

		s.logger.Debug("accepted connection", "local-address", s.LocalAddr(), "remote-address", conn.RemoteAddr().String())

		// Handle the connection in dedicated routine
		go s.handleConn(s.getStreamContext(), conn)
	}
}

// handleConn is used to handle an inbound connection for its lifespan. The
// handler will exit when the passed context is cancelled or the connection is
// closed.
func (s *TCPTransport) handleConn(connCtx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReaderSize(conn, connReceiveBufferSize)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		select {
		case <-connCtx.Done():
			s.logger.Debug("stream layer is closed")
			return
		default:
		}

		if err := s.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				s.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			s.logger.Error("failed to flush response", "error", err)
			return
		}
	}
}

// handleCommand is used to decode and dispatch a single command.
func (s *TCPTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	getTypeStart := time.Now()

	// Get the rpc type
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// measuring the time to get the first byte separately because the heartbeat conn will hang out here
	// for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.
	metrics.MeasureSince([]string{"raft", "net", "getRPCType"}, getTypeStart)
	decodeStart := time.Now()

	// Create the RPC object
	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		RespChan: respCh,
	}

	consumeCh := s.chConsume

	// Decode the command
	isHeartbeat := false
	var labels []metrics.Label
	switch rpcType {
	case rpcAppendEntries:
		var req raft.AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

		leaderAddr := req.RPCHeader.Addr

		// Check if this is a heartbeat
		if req.Term != 0 && leaderAddr != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

		if isHeartbeat {
			labels = []metrics.Label{{Name: "rpcType", Value: "Heartbeat"}}
		} else {
			labels = []metrics.Label{{Name: "rpcType", Value: "AppendEntries"}}
		}
	case rpcRequestVote:
		var req raft.RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestVote"}}
	case rpcInstallSnapshot:
		var req raft.InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		rpc.Reader = io.LimitReader(r, req.Size)
		labels = []metrics.Label{{Name: "rpcType", Value: "InstallSnapshot"}}
	case rpcTimeoutNow:
		var req raft.TimeoutNowRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "TimeoutNow"}}
	case rpcStore:
		var req pb.Store
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "Store"}}
		consumeCh = s.chConsumeCache
	case rpcLatestUID:
		var req pb.LatestUid
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "Load"}}
		consumeCh = s.chConsumeCache
	// case rpcLoad:
	// 	var req pb.LoadValue
	// 	if err := dec.Decode(&req); err != nil {
	// 		return err
	// 	}
	// 	rpc.Command = &req
	// 	labels = []metrics.Label{{Name: "rpcType", Value: "Load"}}
	// 	consumeCh = s.chConsumeCache
	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcDecode"}, decodeStart, labels)

	processStart := time.Now()

	// Check for heartbeat fast-path
	if isHeartbeat {
		s.heartbeatFnLock.Lock()
		fn := s.heartbeatFn
		s.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
			goto RESP
		}
	}

	// Dispatch the RPC
	select {
	case consumeCh <- rpc:
	case <-s.shutdownCh:
		return raft.ErrTransportShutdown
	}

	// Wait for response
RESP:
	// we will differentiate the heartbeat fast path from normal RPCs with labels
	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcEnqueue"}, processStart, labels)
	respWaitStart := time.Now()
	select {
	case resp := <-respCh:
		defer metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcRespond"}, respWaitStart, labels)
		// Send the error first
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		// Send the response
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-s.shutdownCh:
		return raft.ErrTransportShutdown
	}
	return nil
}

// setupStreamContext is used to create a new stream context. This should be
// called with the stream lock held.
func (s *TCPTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	s.streamCtx = ctx
	s.streamCancel = cancel
}

// getStreamContext is used retrieve the current stream context.
func (s *TCPTransport) getStreamContext() context.Context {
	s.streamCtxLock.RLock()
	defer s.streamCtxLock.RUnlock()
	return s.streamCtx
}

func getDeadline(ctx context.Context, timeout time.Duration) (time.Time, bool) {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline, true
	}
	if timeout != 0 {
		return time.Now().Add(timeout), true
	}
	return time.Time{}, false
}

// decodeResponse is used to decode an RPC response and reports whether
// the connection can be reused.
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// Decode the error if any
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		_ = conn.Release()
		return false, err
	}

	// Decode the response
	if err := conn.dec.Decode(resp); err != nil {
		_ = conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, fmt.Errorf(rpcError)
	}
	return true, nil
}

// sendRPC is used to encode and send the RPC.
func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {
	// Write the request type
	if err := conn.w.WriteByte(rpcType); err != nil {
		_ = conn.Release()
		return err
	}

	// Send the request
	if err := conn.enc.Encode(args); err != nil {
		_ = conn.Release()
		return err
	}

	// Flush
	if err := conn.w.Flush(); err != nil {
		_ = conn.Release()
		return err
	}
	return nil
}

type netConn struct {
	target raft.ServerAddress
	conn   net.Conn
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (n *netConn) Release() error {
	return n.conn.Close()
}
