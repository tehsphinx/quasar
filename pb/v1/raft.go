package pb

import (
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ToRPCHeader(in raft.RPCHeader) *RPCHeader {
	return &RPCHeader{
		ProtocolVersion: uint32(in.ProtocolVersion),
		Id:              in.ID,
		Addr:            in.Addr,
	}
}

func (x *RPCHeader) Convert() raft.RPCHeader {
	return raft.RPCHeader{
		ProtocolVersion: raft.ProtocolVersion(x.GetProtocolVersion()),
		ID:              x.GetId(),
		Addr:            x.GetAddr(),
	}
}

func ToAppendEntriesRequest(in *raft.AppendEntriesRequest) *AppendEntriesRequest {
	return &AppendEntriesRequest{
		Header:            ToRPCHeader(in.GetRPCHeader()),
		Term:              in.Term,
		Leader:            in.Leader,
		PrevLogEntry:      in.PrevLogEntry,
		PrevLogTerm:       in.PrevLogTerm,
		Entries:           ToLogs(in.Entries),
		LeaderCommitIndex: in.LeaderCommitIndex,
	}
}

func (x *AppendEntriesRequest) Convert() *raft.AppendEntriesRequest {
	return &raft.AppendEntriesRequest{
		RPCHeader:         x.GetHeader().Convert(),
		Term:              x.GetTerm(),
		Leader:            x.GetLeader(),
		PrevLogEntry:      x.GetPrevLogEntry(),
		PrevLogTerm:       x.GetPrevLogTerm(),
		Entries:           FromLogs(x.GetEntries()),
		LeaderCommitIndex: x.GetLeaderCommitIndex(),
	}
}

func ToAppendEntriesResponse(in *raft.AppendEntriesResponse) *AppendEntriesResponse {
	return &AppendEntriesResponse{
		Header:         ToRPCHeader(in.GetRPCHeader()),
		Term:           in.Term,
		LastLog:        in.LastLog,
		Success:        in.Success,
		NoRetryBackoff: in.NoRetryBackoff,
	}
}

func (x *AppendEntriesResponse) Convert() *raft.AppendEntriesResponse {
	return &raft.AppendEntriesResponse{
		RPCHeader:      x.GetHeader().Convert(),
		Term:           x.GetTerm(),
		LastLog:        x.GetLastLog(),
		Success:        x.GetSuccess(),
		NoRetryBackoff: x.GetNoRetryBackoff(),
	}
}

func ToRequestVoteRequest(in *raft.RequestVoteRequest) *RequestVoteRequest {
	return &RequestVoteRequest{
		Header:             ToRPCHeader(in.GetRPCHeader()),
		Term:               in.Term,
		Candidate:          in.Candidate,
		LastLogIndex:       in.LastLogIndex,
		LastLogTerm:        in.LastLogTerm,
		LeadershipTransfer: in.LeadershipTransfer,
	}
}

func (x *RequestVoteRequest) Convert() *raft.RequestVoteRequest {
	return &raft.RequestVoteRequest{
		RPCHeader:          x.GetHeader().Convert(),
		Term:               x.GetTerm(),
		Candidate:          x.GetCandidate(),
		LastLogIndex:       x.GetLastLogIndex(),
		LastLogTerm:        x.GetLastLogTerm(),
		LeadershipTransfer: x.GetLeadershipTransfer(),
	}
}

func ToRequestVoteResponse(in *raft.RequestVoteResponse) *RequestVoteResponse {
	return &RequestVoteResponse{
		Header:  ToRPCHeader(in.GetRPCHeader()),
		Term:    in.Term,
		Peers:   in.Peers,
		Granted: in.Granted,
	}
}

func (x *RequestVoteResponse) Convert() *raft.RequestVoteResponse {
	return &raft.RequestVoteResponse{
		RPCHeader: x.GetHeader().Convert(),
		Term:      x.GetTerm(),
		Peers:     x.GetPeers(),
		Granted:   x.GetGranted(),
	}
}

func ToInstallSnapshotRequest(in *raft.InstallSnapshotRequest) *InstallSnapshotRequest {
	return &InstallSnapshotRequest{
		Header:             ToRPCHeader(in.GetRPCHeader()),
		SnapshotVersion:    uint32(in.SnapshotVersion),
		Term:               in.Term,
		Leader:             in.Leader,
		LastLogIndex:       in.LastLogIndex,
		LastLogTerm:        in.LastLogTerm,
		Peers:              in.Peers,
		Configuration:      in.Configuration,
		ConfigurationIndex: in.ConfigurationIndex,
		Size:               in.Size,
	}
}

func (x *InstallSnapshotRequest) Convert() *raft.InstallSnapshotRequest {
	return &raft.InstallSnapshotRequest{
		RPCHeader:          x.GetHeader().Convert(),
		SnapshotVersion:    raft.SnapshotVersion(x.GetSnapshotVersion()),
		Term:               x.GetTerm(),
		Leader:             x.GetLeader(),
		LastLogIndex:       x.GetLastLogIndex(),
		LastLogTerm:        x.GetLastLogTerm(),
		Peers:              x.GetPeers(),
		Configuration:      x.GetConfiguration(),
		ConfigurationIndex: x.GetConfigurationIndex(),
		Size:               x.GetSize(),
	}
}

func ToInstallSnapshotResponse(in *raft.InstallSnapshotResponse) *InstallSnapshotResponse {
	return &InstallSnapshotResponse{
		Header:  ToRPCHeader(in.GetRPCHeader()),
		Term:    in.Term,
		Success: in.Success,
	}
}

func (x *InstallSnapshotResponse) Convert() *raft.InstallSnapshotResponse {
	return &raft.InstallSnapshotResponse{
		RPCHeader: x.GetHeader().Convert(),
		Term:      x.GetTerm(),
		Success:   x.GetSuccess(),
	}
}

func ToTimeoutNowRequest(in *raft.TimeoutNowRequest) *TimeoutNowRequest {
	return &TimeoutNowRequest{
		Header: ToRPCHeader(in.GetRPCHeader()),
	}
}

func (x *TimeoutNowRequest) Convert() *raft.TimeoutNowRequest {
	return &raft.TimeoutNowRequest{
		RPCHeader: x.GetHeader().Convert(),
	}
}

func ToTimeoutNowResponse(in *raft.TimeoutNowResponse) *TimeoutNowResponse {
	return &TimeoutNowResponse{
		Header: ToRPCHeader(in.GetRPCHeader()),
	}
}

func (x *TimeoutNowResponse) Convert() *raft.TimeoutNowResponse {
	return &raft.TimeoutNowResponse{
		RPCHeader: x.GetHeader().Convert(),
	}
}

func FromLogs(in []*Log) []*raft.Log {
	logs := make([]*raft.Log, 0, len(in))
	for _, l := range in {
		log := l.Convert()
		logs = append(logs, log)
	}
	return logs
}

func ToLogs(in []*raft.Log) []*Log {
	logs := make([]*Log, 0, len(in))
	for _, log := range in {
		logs = append(logs, ToLog(log))
	}
	return logs
}

func ToLog(in *raft.Log) *Log {
	return &Log{
		Index:      in.Index,
		Term:       in.Term,
		LogType:    uint32(in.Type),
		Data:       in.Data,
		Extensions: in.Extensions,
		AppendedAt: timestamppb.New(in.AppendedAt),
	}
}

func (x *Log) Convert() *raft.Log {
	return &raft.Log{
		Index:      x.GetIndex(),
		Term:       x.GetTerm(),
		Type:       raft.LogType(x.GetLogType()),
		Data:       x.GetData(),
		Extensions: x.GetExtensions(),
		AppendedAt: x.GetAppendedAt().AsTime(),
	}
}
