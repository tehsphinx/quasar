package pb

import (
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ToRPCHeader converts the *raft.RPCHeader to a *pb.RPCHeader.
func ToRPCHeader(in raft.RPCHeader) *RPCHeader {
	return &RPCHeader{
		ProtocolVersion: uint32(in.ProtocolVersion),
		Id:              in.ID,
		Addr:            in.Addr,
	}
}

// Convert converts the *pb.RPCHeader to a *raft.RPCHeader.
func (x *RPCHeader) Convert() raft.RPCHeader {
	return raft.RPCHeader{
		ProtocolVersion: raft.ProtocolVersion(x.GetProtocolVersion()),
		ID:              x.GetId(),
		Addr:            x.GetAddr(),
	}
}

// ToAppendEntriesRequest converts the *raft.AppendEntriesRequest to a *pb.AppendEntriesRequest.
func ToAppendEntriesRequest(in *raft.AppendEntriesRequest) *AppendEntriesRequest {
	return &AppendEntriesRequest{
		Header: ToRPCHeader(in.GetRPCHeader()),
		Term:   in.Term,
		//nolint:staticcheck // just to be backwards compatible
		Leader:            in.Leader,
		PrevLogEntry:      in.PrevLogEntry,
		PrevLogTerm:       in.PrevLogTerm,
		Entries:           ToLogs(in.Entries),
		LeaderCommitIndex: in.LeaderCommitIndex,
	}
}

// Convert converts the *pb.AppendEntriesRequest to a *raft.AppendEntriesRequest.
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

// ToAppendEntriesResponse converts the *raft.AppendEntriesResponse to a *pb.AppendEntriesResponse.
func ToAppendEntriesResponse(in *raft.AppendEntriesResponse) *AppendEntriesResponse {
	return &AppendEntriesResponse{
		Header:         ToRPCHeader(in.GetRPCHeader()),
		Term:           in.Term,
		LastLog:        in.LastLog,
		Success:        in.Success,
		NoRetryBackoff: in.NoRetryBackoff,
	}
}

// Convert converts the *pb.AppendEntriesResponse to a *raft.AppendEntriesResponse.
func (x *AppendEntriesResponse) Convert() *raft.AppendEntriesResponse {
	return &raft.AppendEntriesResponse{
		RPCHeader:      x.GetHeader().Convert(),
		Term:           x.GetTerm(),
		LastLog:        x.GetLastLog(),
		Success:        x.GetSuccess(),
		NoRetryBackoff: x.GetNoRetryBackoff(),
	}
}

// ToRequestVoteRequest converts the *raft.RequestVoteRequest to a *pb.RequestVoteRequest.
func ToRequestVoteRequest(in *raft.RequestVoteRequest) *RequestVoteRequest {
	return &RequestVoteRequest{
		Header: ToRPCHeader(in.GetRPCHeader()),
		Term:   in.Term,
		//nolint:staticcheck // just to be backwards compatible
		Candidate:          in.Candidate,
		LastLogIndex:       in.LastLogIndex,
		LastLogTerm:        in.LastLogTerm,
		LeadershipTransfer: in.LeadershipTransfer,
	}
}

// Convert converts the *pb.RequestVoteRequest to a *raft.RequestVoteRequest.
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

// ToRequestVoteResponse converts the *raft.RequestVoteResponse to a *pb.RequestVoteResponse.
func ToRequestVoteResponse(in *raft.RequestVoteResponse) *RequestVoteResponse {
	return &RequestVoteResponse{
		Header:  ToRPCHeader(in.GetRPCHeader()),
		Term:    in.Term,
		Peers:   in.Peers,
		Granted: in.Granted,
	}
}

// Convert converts the *pb.RequestVoteResponse to a *raft.RequestVoteResponse.
func (x *RequestVoteResponse) Convert() *raft.RequestVoteResponse {
	return &raft.RequestVoteResponse{
		RPCHeader: x.GetHeader().Convert(),
		Term:      x.GetTerm(),
		Peers:     x.GetPeers(),
		Granted:   x.GetGranted(),
	}
}

// ToInstallSnapshotRequest converts the *raft.InstallSnapshotRequest to a *pb.InstallSnapshotRequest.
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

// Convert converts the *pb.InstallSnapshotRequest to a *raft.InstallSnapshotRequest.
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

// ToInstallSnapshotResponse converts the *raft.InstallSnapshotResponse to a *pb.InstallSnapshotResponse.
func ToInstallSnapshotResponse(in *raft.InstallSnapshotResponse) *InstallSnapshotResponse {
	return &InstallSnapshotResponse{
		Header:  ToRPCHeader(in.GetRPCHeader()),
		Term:    in.Term,
		Success: in.Success,
	}
}

// Convert converts the *pb.InstallSnapshotResponse to a *raft.InstallSnapshotResponse.
func (x *InstallSnapshotResponse) Convert() *raft.InstallSnapshotResponse {
	return &raft.InstallSnapshotResponse{
		RPCHeader: x.GetHeader().Convert(),
		Term:      x.GetTerm(),
		Success:   x.GetSuccess(),
	}
}

// ToTimeoutNowRequest converts the *raft.TimeoutNowRequest to a *pb.TimeoutNowRequest.
func ToTimeoutNowRequest(in *raft.TimeoutNowRequest) *TimeoutNowRequest {
	return &TimeoutNowRequest{
		Header: ToRPCHeader(in.GetRPCHeader()),
	}
}

// Convert converts the *pb.TimeoutNowRequest to a *raft.TimeoutNowRequest.
func (x *TimeoutNowRequest) Convert() *raft.TimeoutNowRequest {
	return &raft.TimeoutNowRequest{
		RPCHeader: x.GetHeader().Convert(),
	}
}

// ToTimeoutNowResponse converts the *raft.TimeoutNowResponse to a *pb.TimeoutNowResponse.
func ToTimeoutNowResponse(in *raft.TimeoutNowResponse) *TimeoutNowResponse {
	return &TimeoutNowResponse{
		Header: ToRPCHeader(in.GetRPCHeader()),
	}
}

// Convert converts the *pb.TimeoutNowResponse to a *raft.TimeoutNowResponse.
func (x *TimeoutNowResponse) Convert() *raft.TimeoutNowResponse {
	return &raft.TimeoutNowResponse{
		RPCHeader: x.GetHeader().Convert(),
	}
}

// FromLogs converts a []*pb.Log to []*raft.Log.
func FromLogs(in []*Log) []*raft.Log {
	logs := make([]*raft.Log, 0, len(in))
	for _, l := range in {
		log := l.Convert()
		logs = append(logs, log)
	}
	return logs
}

// ToLogs converts a []*raft.Log to []*pb.Log.
func ToLogs(in []*raft.Log) []*Log {
	logs := make([]*Log, 0, len(in))
	for _, log := range in {
		logs = append(logs, ToLog(log))
	}
	return logs
}

// ToLog converts a *raft.Log to *pb.Log.
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

// Convert converts the *pb.Log to a *raft.Log.
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
