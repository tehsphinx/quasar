package quasar

import (
	"github.com/tehsphinx/quasar/pb"
)

func respStore() *pb.CommandResponse {
	return &pb.CommandResponse{
		Resp: &pb.CommandResponse_StoreValue{
			StoreValue: &pb.StoreValueResponse{},
		},
	}
}

func respLoad(data []byte) *pb.CommandResponse {
	return &pb.CommandResponse{
		Resp: &pb.CommandResponse_LoadValue{
			LoadValue: &pb.LoadValueResponse{
				Data: data,
			},
		},
	}
}
