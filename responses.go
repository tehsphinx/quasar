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
