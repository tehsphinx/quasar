package quasar

import (
	"github.com/tehsphinx/quasar/pb/v1"
)

func cmdStore(key string, data []byte) *pb.Command {
	return &pb.Command{Cmd: &pb.Command_Store{
		Store: &pb.Store{
			Data: data,
			Key:  key,
		},
	}}
}

func cmdLatestUID() *pb.Command {
	return &pb.Command{Cmd: &pb.Command_LatestUid{
		LatestUid: &pb.LatestUid{},
	}}
}
