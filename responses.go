package quasar

import (
	"github.com/tehsphinx/quasar/pb/v1"
)

func respStore(cmd *pb.StoreResponse) *pb.CommandResponse {
	return &pb.CommandResponse{
		Resp: &pb.CommandResponse_Store{
			Store: cmd,
		},
	}
}

func respLatestUID(cmd *pb.LatestUidResponse) *pb.CommandResponse {
	return &pb.CommandResponse{
		Resp: &pb.CommandResponse_LatestUid{
			LatestUid: cmd,
		},
	}
}

func respRemoveServer(cmd *pb.RemoveServerResponse) *pb.CommandResponse {
	return &pb.CommandResponse{
		Resp: &pb.CommandResponse_RemoveServer{
			RemoveServer: cmd,
		},
	}
}

func respResetCache(cmd *pb.ResetCacheResponse) *pb.CommandResponse {
	return &pb.CommandResponse{
		Resp: &pb.CommandResponse_ResetCache{
			ResetCache: cmd,
		},
	}
}
