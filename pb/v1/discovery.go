package pb

import (
	"github.com/hashicorp/raft"
)

func ToServerInfo(info raft.Server) *ServerInfo {
	return &ServerInfo{
		ServerId:      string(info.ID),
		ServerAddress: string(info.Address),
		Suffrage:      ServerSuffrage(info.Suffrage),
	}
}

func (x *ServerInfo) Convert() raft.Server {
	if x == nil {
		return raft.Server{}
	}

	return raft.Server{
		ID:       raft.ServerID(x.ServerId),
		Address:  raft.ServerAddress(x.ServerAddress),
		Suffrage: raft.ServerSuffrage(x.Suffrage),
	}
}
