package quasar

import (
	"github.com/tehsphinx/quasar/pb/v1"
)

func respStore(cmd *pb.StoreValueResponse) *pb.CommandResponse {
	return &pb.CommandResponse{
		Resp: &pb.CommandResponse_StoreValue{
			StoreValue: cmd,
		},
	}
}

func respLoad(cmd *pb.LoadValueResponse) *pb.CommandResponse {
	return &pb.CommandResponse{
		Resp: &pb.CommandResponse_LoadValue{
			LoadValue: cmd,
		},
	}
}
