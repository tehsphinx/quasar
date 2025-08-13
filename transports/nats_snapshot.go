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
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (s *NATSTransport) InstallSnapshot(_ raft.ServerID, address raft.ServerAddress,
	request *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader,
) error {
	ctx, cancel := context.WithTimeout(context.TODO(), snapshotTimeout(s.timeout, request.Size))
	defer cancel()

	protoRespCh, err := s.requestOpenChannel(ctx, address, request)
	if err != nil {
		return err
	}

	chResp := make(chan []byte)
	recvSubj := protoRespCh.GetSubject() + ".resp"
	respSub, err := s.conn.Subscribe(recvSubj, func(msg *nats.Msg) {
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
		*resp = *protoResp.GetInstallSnapshot().Convert()
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
				if e := s.conn.Publish(sendSubj+".EOF", buf[:n]); e != nil {
					return e
				}
				break
			}
			return r
		}

		counter++
		if e := s.conn.Publish(sendSubj+"."+strconv.Itoa(counter), buf[:n]); e != nil {
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
	response, err := s.conn.RequestWithContext(ctx, subj, bts)
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
		if r := s.conn.Publish(respSubj, bts); r != nil {
			s.logger.Error("failed to send response", "error", r)
		}
	}
}

func (s *NATSTransport) openNatsStream(subj string) (*io.PipeReader, *nats.Subscription, error) {
	pipeReader, pipeWriter := io.Pipe()
	timer := time.AfterFunc(snapshotPkgTimout, func() {
		_ = pipeWriter.CloseWithError(context.DeadlineExceeded)
	})

	chanSub, err := s.conn.Subscribe(subj+".send.*", func(msg *nats.Msg) {
		timer.Reset(snapshotPkgTimout)
		if _, r := pipeWriter.Write(msg.Data); r != nil {
			_ = pipeWriter.CloseWithError(r)
			return
		}
		if strings.HasSuffix(msg.Subject, ".EOF") {
			timer.Stop()
			_ = pipeWriter.Close()
		}
	})
	return pipeReader, chanSub, err
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
