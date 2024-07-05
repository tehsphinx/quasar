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

func (s *NATSTransport) InstallSnapshot(_ raft.ServerID, address raft.ServerAddress,
	request *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader,
) error {
	subj := fmt.Sprintf("quasar.%s.%s.install.snapshot", s.cacheName, address)

	bts, err := proto.Marshal(pb.ToInstallSnapshotRequest(request))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), s.timeout)
	defer cancel()

	response, err := s.conn.RequestWithContext(ctx, subj, bts)
	if err != nil {
		return err
	}

	var protoRespCh pb.InstallSnapshotChannel
	if r := proto.Unmarshal(response.Data, &protoRespCh); r != nil {
		return r
	}

	recvSubj := protoRespCh.GetSubject() + ".resp"
	chResp := make(chan []byte)
	respSub, err := s.conn.Subscribe(recvSubj, func(msg *nats.Msg) {
		chResp <- msg.Data
	})
	defer func() { _ = respSub.Unsubscribe() }()

	sendSubj := protoRespCh.GetSubject() + ".send"
	buf := make([]byte, maxPkgSize)
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

	select {
	case <-time.After(60 * time.Second):
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

func (s *NATSTransport) handleInstallSnapshot(msg *nats.Msg) {
	var protoMsg pb.InstallSnapshotRequest
	if r := proto.Unmarshal(msg.Data, &protoMsg); r != nil {
		s.logger.Error("failed to decode incoming command", "error", r)
		return
	}

	chanSubj := "quasar.snapshot.channel." + uuid.NewString()
	pipeReader, pipeWriter := io.Pipe()
	timer := time.AfterFunc(snapshotPkgTimout, func() {
		_ = pipeWriter.CloseWithError(context.DeadlineExceeded)
	})

	chanSub, err := s.conn.Subscribe(chanSubj+".send.*", func(msg *nats.Msg) {
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

	// Create the RPC object
	chResp := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		RespChan: chResp,
		Command:  protoMsg.Convert(),
		Reader:   pipeReader,
	}

	s.chConsume <- rpc

	bts, err = s.awaitResponse(chResp, func(i interface{}) *pb.CommandResponse {
		resp, _ := i.(*raft.InstallSnapshotResponse)
		return &pb.CommandResponse{Resp: &pb.CommandResponse_InstallSnapshot{
			InstallSnapshot: pb.ToInstallSnapshotResponse(resp),
		}}
	})
	if err != nil {
		s.logger.Error("failed to send response", "error", err)
		return
	}

	respSubj := chanSubj + ".resp"
	if r := s.conn.Publish(respSubj, bts); r != nil {
		s.logger.Error("failed to send response", "error", r)
	}
}
