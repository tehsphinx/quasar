package transports

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// maxSnapshotInstallTimeout caps the size-scaled install deadline. The scaled
// timeout exists to let big snapshots finish, but it is also how long
// InstallSnapshot waits on a follower that died mid-transfer and will never
// respond (~33min for a 100MB snapshot) — parking raft's replication
// goroutine for that peer the whole time. A NATS transfer runs at LAN speed
// and a live receiver always answers, with an error at the latest 5s
// (snapshotPkgTimout) after its stream stalls, so the cap only ever cuts
// short the wait on a dead peer.
const maxSnapshotInstallTimeout = 10 * time.Minute

// installSnapshotTimeout is the size-scaled snapshot timeout capped at
// maxSnapshotInstallTimeout. The TCP transport keeps the uncapped scaling: its
// deadline covers the transfer itself, which on a slow link legitimately
// needs it.
func installSnapshotTimeout(origTimeout time.Duration, size int64) time.Duration {
	return min(snapshotTimeout(origTimeout, size), maxSnapshotInstallTimeout)
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (s *NATSTransport) InstallSnapshot(_ raft.ServerID, address raft.ServerAddress,
	request *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader,
) error {
	ctx, cancel := context.WithTimeout(context.TODO(), installSnapshotTimeout(s.timeout, request.Size))
	defer cancel()

	protoRespCh, err := s.requestOpenChannel(ctx, address, request)
	if err != nil {
		return err
	}

	// Buffered (capacity 1) so a response arriving after the select below has
	// exited on ctx.Done() never blocks the subscription callback goroutine.
	chResp := make(chan []byte, 1)
	recvSubj := protoRespCh.GetSubject() + ".resp"
	respSub, err := s.connBulk.Subscribe(recvSubj, func(msg *nats.Msg) {
		chResp <- msg.Data
	})
	if err != nil {
		return err
	}
	defer func() { _ = respSub.Unsubscribe() }()

	sendSubj := protoRespCh.GetSubject() + ".send"
	if r := s.sendSnapshot(sendSubj, data); r != nil {
		return r
	}

	select {
	case <-ctx.Done():
		return errors.New("timeout waiting for response on install snapshot")
	case respBts := <-chResp:
		var protoResp pb.CommandResponse
		if r := proto.Unmarshal(respBts, &protoResp); r != nil {
			return r
		}
		payload, err := checkRaftResponse(&protoResp, (*pb.CommandResponse).GetInstallSnapshot)
		if err != nil {
			return err
		}
		*resp = *payload.Convert()
	}
	return nil
}

func (s *NATSTransport) sendSnapshot(sendSubj string, data io.Reader) error {
	buf := make([]byte, s.maxMsgSize)
	var counter int
	for {
		n, r := data.Read(buf)
		if r != nil {
			if errors.Is(r, io.EOF) {
				if e := s.connBulk.Publish(sendSubj+".EOF", buf[:n]); e != nil {
					return e
				}
				break
			}
			return r
		}

		counter++
		if e := s.connBulk.Publish(sendSubj+"."+strconv.Itoa(counter), buf[:n]); e != nil {
			return e
		}
	}
	return nil
}

func (s *NATSTransport) requestOpenChannel(ctx context.Context, address raft.ServerAddress,
	request *raft.InstallSnapshotRequest,
) (*pb.InstallSnapshotChannel, error) {
	bts, err := proto.Marshal(pb.ToInstallSnapshotRequest(request))
	if err != nil {
		return nil, err
	}

	subj := fmt.Sprintf("quasar.%s.%s.install.snapshot", s.cacheName, address)
	response, err := s.connBulk.RequestWithContext(ctx, subj, bts)
	if err != nil {
		return nil, err
	}

	var protoRespCh pb.InstallSnapshotChannel
	if r := proto.Unmarshal(response.Data, &protoRespCh); r != nil {
		return nil, r
	}

	return &protoRespCh, nil
}

func (s *NATSTransport) handleInstallSnapshot(ctx context.Context) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		// Decoded before the stream is opened: the request carries the snapshot
		// size, which the receive side needs to report transfer progress as a
		// percentage (RT-14059).
		var protoMsg pb.InstallSnapshotRequest
		if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
			s.logger.Error("failed to decode incoming command", "error", r)
			return
		}

		chanSubj := "quasar.snapshot.channel." + uuid.NewString()
		pipeReader, chanSub, err := s.openNatsStream(chanSubj, protoMsg.GetSize())
		if err != nil {
			s.logger.Error("failed to open snapshot channel", "error", err)
			return
		}
		defer func() { _ = chanSub.Unsubscribe() }()

		bts, err := proto.Marshal(&pb.InstallSnapshotChannel{Subject: chanSubj})
		if err != nil {
			s.logger.Error("failed to marshal InstallSnapshotChannel message", "error", err)
			return
		}
		if r := msg.Respond(bts); r != nil {
			s.logger.Error("failed to send install snapshot channel message", "error", r)
			return
		}

		chResp, rpc := buildConsumeMsg(&protoMsg, pipeReader)

		s.chConsume <- rpc

		bts, err = s.awaitResponse(ctx, chResp, func(i interface{}) *pb.CommandResponse {
			resp, _ := i.(*raft.InstallSnapshotResponse)
			return &pb.CommandResponse{Resp: &pb.CommandResponse_InstallSnapshot{
				InstallSnapshot: pb.ToInstallSnapshotResponse(resp),
			}}
		})
		if err != nil {
			s.logger.Error("failed to consume message", "error", err)
			return
		}

		respSubj := chanSubj + ".resp"
		if r := s.connBulk.Publish(respSubj, bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
}

func (s *NATSTransport) openNatsStream(subj string, size int64) (*io.PipeReader, *nats.Subscription, error) {
	pipeReader, pipeWriter := io.Pipe()
	timer := time.AfterFunc(snapshotPkgTimout, func() {
		_ = pipeWriter.CloseWithError(context.DeadlineExceeded)
	})

	handler := snapshotPkgHandler(s.logger, timer, pipeWriter, size)
	chanSub, err := s.connBulk.Subscribe(subj+".send.*", func(msg *nats.Msg) {
		handler(msg.Subject, msg.Data)
	})
	if err != nil {
		timer.Stop()
		return nil, nil, err
	}
	// The client caps each subscription's undelivered backlog at 64MB
	// (DefaultSubPendingBytesLimit) and silently drops what exceeds it. The
	// sender publishes the whole snapshot with no flow control while raft may
	// not have started draining the pipe yet, so any snapshot beyond that cap
	// lost packages mid-stream (RT-13934). Unlimited is bounded in practice
	// by the snapshot size — a copy the leader already holds in its in-memory
	// snapshot store.
	if r := chanSub.SetPendingLimits(-1, -1); r != nil {
		timer.Stop()
		_ = chanSub.Unsubscribe()
		return nil, nil, r
	}
	return pipeReader, chanSub, nil
}

// errSnapshotStreamBroken marks a snapshot stream whose package sequence is
// provably broken: a package was lost, duplicated or foreign. Aborting on it
// fails the install immediately and attributably instead of stalling into the
// inter-package timer or surfacing as a short read in raft.
var errSnapshotStreamBroken = errors.New("snapshot stream broken")

// snapshotPkgHandler returns the receive handler for one snapshot stream. It
// verifies the package counter in the subject's trailing token before handing
// the payload to writeSnapshotPkg: the sender numbers packages from 1 and the
// NATS client may drop messages (e.g. across a reconnect mid-stream), which
// would otherwise silently corrupt the stream. The .EOF package carries no
// counter and stays guarded by the inter-package timer alone.
//
// nats.go dispatches the callbacks of one async subscription serially from a
// single goroutine — the ordered, blocking pipe writes already depend on
// that — so the closure state needs no locking.
//
// size is the snapshot size announced in the InstallSnapshot request and is
// only used to report transfer progress.
func snapshotPkgHandler(logger hclog.Logger, timer *time.Timer, pipeWriter *io.PipeWriter, size int64,
) func(subject string, data []byte) {
	expected := 1
	failed := false
	progress := newSnapshotProgress(logger, size)
	return func(subject string, data []byte) {
		if failed {
			// The sender is fire-and-forget: after an abort the rest of the
			// stream still arrives and is dropped here without further logs.
			return
		}
		isEOF := strings.HasSuffix(subject, ".EOF")
		if !isEOF {
			var err error
			token := subject[strings.LastIndex(subject, ".")+1:]
			got, convErr := strconv.Atoi(token)
			switch {
			case convErr != nil:
				err = fmt.Errorf("%w: unparsable package counter in subject %q: %v", errSnapshotStreamBroken, subject, convErr)
			case got != expected:
				err = fmt.Errorf("%w: got package %d, expected %d", errSnapshotStreamBroken, got, expected)
			}
			if err != nil {
				failed = true
				logger.Error("aborting snapshot install", "error", err)
				timer.Stop()
				_ = pipeWriter.CloseWithError(err)
				return
			}
			expected++
		}
		if writeSnapshotPkg(timer, pipeWriter, subject, data) {
			progress.record(len(data), isEOF)
		}
	}
}

// snapshotProgressInterval is the minimum gap between receiver-side snapshot
// transfer progress lines.
const snapshotProgressInterval = time.Second

// snapshotProgress logs the receiver's view of one snapshot transfer. raft
// monitors the same transfer, but on a hardcoded 10s tick
// (snapshotRestoreMonitorInterval) whose first line is only emitted once the
// interval elapses — so a transfer finishing sooner is reported by a single
// closing 100% line, and a slower one by very little (RT-14059).
//
// Progress is driven by the arriving packages rather than by a ticker: the pipe
// write each package goes through blocks until raft consumes it, so the byte
// count tracks what the FSM side has really ingested, and there is no goroutine
// to tear down on the abort, timeout and EOF paths. A stream that stalls
// therefore stops reporting; that case is already covered by the inter-package
// timer, which fails the install with context.DeadlineExceeded.
type snapshotProgress struct {
	logger  hclog.Logger
	size    int64
	read    int64
	lastLog time.Time
}

func newSnapshotProgress(logger hclog.Logger, size int64) *snapshotProgress {
	return &snapshotProgress{logger: logger, size: size, lastLog: time.Now()}
}

// record accounts for one package handed to the FSM-side reader and logs
// progress at most every snapshotProgressInterval. The last package of a
// stream always logs, so every transfer closes with a final line. Field names
// match raft's own progress monitor so both read alike.
func (p *snapshotProgress) record(n int, last bool) {
	p.read += int64(n)
	if !last && time.Since(p.lastLog) < snapshotProgressInterval {
		return
	}
	p.lastLog = time.Now()

	p.logger.Info("snapshot transfer progress",
		"read-bytes", p.read,
		"size", p.size,
		"percent-complete", p.percent())
}

// percent renders the share of the announced snapshot size received so far.
// The size is whatever the leader put in the InstallSnapshot request; a
// missing or nonsensical one must not turn the progress line into NaN or Inf.
func (p *snapshotProgress) percent() string {
	if p.size <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%0.2f%%", float64(100*p.read)/float64(p.size))
}

// writeSnapshotPkg hands one received snapshot package to the FSM-side reader
// through pipeWriter. The timer is stopped for the duration of the write:
// pipeWriter.Write blocks until that reader consumes the data, and a slow/busy
// local reader (or a reinit window) must not be mistaken for a stalled network.
// The timer only measures the gap between network packages, so it is re-armed
// after a successful (non-EOF) write.
//
// It reports whether the package reached the reader, so a caller tracking
// transfer progress does not count a package the pipe rejected.
func writeSnapshotPkg(timer *time.Timer, pipeWriter *io.PipeWriter, subject string, data []byte) bool {
	timer.Stop()
	if _, r := pipeWriter.Write(data); r != nil {
		_ = pipeWriter.CloseWithError(r)
		return false
	}
	if strings.HasSuffix(subject, ".EOF") {
		_ = pipeWriter.Close()
		return true
	}
	timer.Reset(snapshotPkgTimout)
	return true
}

func buildConsumeMsg(protoMsg *pb.InstallSnapshotRequest, pipeReader *io.PipeReader) (chan raft.RPCResponse, raft.RPC) {
	// Create the RPC object
	chResp := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		RespChan: chResp,
		Command:  protoMsg.Convert(),
		Reader:   pipeReader,
	}
	return chResp, rpc
}
