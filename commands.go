package quasar

import (
	"github.com/tehsphinx/quasar/pb/v1"
)

func cmdStore(key string, data []byte) *pb.Command {
	return &pb.Command{Cmd: &pb.Command_StoreValue{
		StoreValue: &pb.StoreValue{
			Key:  key,
			Data: data,
		},
	}}
}

func cmdLoad(key string) *pb.Command {
	return &pb.Command{Cmd: &pb.Command_LoadValue{
		LoadValue: &pb.LoadValue{
			Key: key,
		},
	}}
}
