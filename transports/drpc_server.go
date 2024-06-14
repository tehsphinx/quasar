package transports

import (
	"context"
	"errors"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"storj.io/drpc/drpcerr"
)

func newDRPCServer(transport *DRPCTransport) *drpcServer {
	return &drpcServer{
		trans: transport,
	}
}

type drpcServer struct {
	trans *DRPCTransport
}

func (d *drpcServer) Apply(ctx context.Context, command *pb.Command) (*pb.CommandResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (d *drpcServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp, err := d.reqChConsume(request.Convert())
	if err != nil {
		return nil, err
	}

	res, ok := resp.(*raft.AppendEntriesResponse)
	if !ok {
		return nil, drpcerr.WithCode(errors.New("can't send invalid response type"), Internal)
	}
	return pb.ToAppendEntriesResponse(res), nil
}

func (d *drpcServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (d *drpcServer) InstallSnapshot(ctx context.Context, request *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (d *drpcServer) TimeoutNow(ctx context.Context, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (d *drpcServer) reqChConsume(command interface{}) (interface{}, error) {
	respCh := make(chan raft.RPCResponse, 1)

	// send to be consumed by raft
	select {
	case d.trans.chConsume <- raft.RPC{Command: command, RespChan: respCh}:
	case <-d.trans.ctxClose.Done():
		return nil, drpcerr.WithCode(raft.ErrTransportShutdown, Aborted)
	}

	// await raft response
	select {
	case resp := <-respCh:
		if err := resp.Error; err != nil {
			return nil, drpcerr.WithCode(err, Internal)
		}
		return resp.Response, nil
	case <-d.trans.ctxClose.Done():
		return nil, drpcerr.WithCode(raft.ErrTransportShutdown, Aborted)
	}
}
