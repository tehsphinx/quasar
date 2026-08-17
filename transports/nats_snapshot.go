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
		chanSubj := "quasar.snapshot.channel." + uuid.NewString()
		pipeReader, chanSub, err := s.openNatsStream(chanSubj)
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

		chResp, rpc, err := s.buildConsumeMsg(msg, pipeReader)
		if err != nil {
			s.logger.Error("failed to decode incoming command", "error", err)
			return
		}

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

func (s *NATSTransport) openNatsStream(subj string) (*io.PipeReader, *nats.Subscription, error) {
	pipeReader, pipeWriter := io.Pipe()
	timer := time.AfterFunc(snapshotPkgTimout, func() {
		_ = pipeWriter.CloseWithError(context.DeadlineExceeded)
	})

	handler := snapshotPkgHandler(s.logger, timer, pipeWriter)
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
func snapshotPkgHandler(logger hclog.Logger, timer *time.Timer, pipeWriter *io.PipeWriter) func(subject string, data []byte) {
	expected := 1
	failed := false
	return func(subject string, data []byte) {
		if failed {
			// The sender is fire-and-forget: after an abort the rest of the
			// stream still arrives and is dropped here without further logs.
			return
		}
		if !strings.HasSuffix(subject, ".EOF") {
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
		writeSnapshotPkg(timer, pipeWriter, subject, data)
	}
}

// writeSnapshotPkg hands one received snapshot package to the FSM-side reader
// through pipeWriter. The timer is stopped for the duration of the write:
// pipeWriter.Write blocks until that reader consumes the data, and a slow/busy
// local reader (or a reinit window) must not be mistaken for a stalled network.
// The timer only measures the gap between network packages, so it is re-armed
// after a successful (non-EOF) write.
func writeSnapshotPkg(timer *time.Timer, pipeWriter *io.PipeWriter, subject string, data []byte) {
	timer.Stop()
	if _, r := pipeWriter.Write(data); r != nil {
		_ = pipeWriter.CloseWithError(r)
		return
	}
	if strings.HasSuffix(subject, ".EOF") {
		_ = pipeWriter.Close()
		return
	}
	timer.Reset(snapshotPkgTimout)
}

func (s *NATSTransport) buildConsumeMsg(msg *nats.Msg, pipeReader *io.PipeReader) (chan raft.RPCResponse, raft.RPC, error) {
	var protoMsg pb.InstallSnapshotRequest
	if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
		return nil, raft.RPC{}, r
	}

	// Create the RPC object
	chResp := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		RespChan: chResp,
		Command:  protoMsg.Convert(),
		Reader:   pipeReader,
	}
	return chResp, rpc, nil
}
