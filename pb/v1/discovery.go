package pb

import (
	"github.com/hashicorp/raft"
)

func ToServerInfo(info raft.Server) *ServerInfo {
	return &ServerInfo{
		ServerId:      string(info.ID),
		ServerAddress: string(info.Address),
		Voter:         info.Suffrage == raft.Voter,
	}
}

func (x *ServerInfo) Convert() raft.Server {
	if x == nil {
		return raft.Server{}
	}

	suffrage := raft.Nonvoter
	if x.Voter {
		suffrage = raft.Voter
	}
	return raft.Server{
		ID:       raft.ServerID(x.ServerId),
		Address:  raft.ServerAddress(x.ServerAddress),
		Suffrage: suffrage,
	}
}

func ToServerInfos(servers []raft.Server) []*ServerInfo {
	resp := make([]*ServerInfo, len(servers))
	for index, server := range servers {
		resp[index] = ToServerInfo(server)
	}
	return resp
}
