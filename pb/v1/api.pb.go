// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v4.24.3
// source: v1/api.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_v1_api_proto protoreflect.FileDescriptor

var file_v1_api_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x76, 0x31, 0x2f, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x09,
	0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2e, 0x76, 0x31, 0x1a, 0x0c, 0x76, 0x31, 0x2f, 0x63, 0x6d,
	0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0d, 0x76, 0x31, 0x2f, 0x72, 0x61, 0x66, 0x74,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x32, 0x8f, 0x03, 0x0a, 0x0d, 0x51, 0x75, 0x61, 0x73, 0x61,
	0x72, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x37, 0x0a, 0x05, 0x41, 0x70, 0x70, 0x6c,
	0x79, 0x12, 0x12, 0x2e, 0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f,
	0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x1a, 0x1a, 0x2e, 0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2e, 0x76,
	0x31, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x52, 0x0a, 0x0d, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69,
	0x65, 0x73, 0x12, 0x1f, 0x2e, 0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x41,
	0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2e, 0x76, 0x31, 0x2e,
	0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4c, 0x0a, 0x0b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x56, 0x6f, 0x74, 0x65, 0x12, 0x1d, 0x2e, 0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2e, 0x76, 0x31,
	0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e, 0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2e, 0x76, 0x31, 0x2e,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x58, 0x0a, 0x0f, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6c, 0x6c, 0x53, 0x6e,
	0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x12, 0x21, 0x2e, 0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2e,
	0x76, 0x31, 0x2e, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6c, 0x6c, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68,
	0x6f, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x22, 0x2e, 0x71, 0x75, 0x61, 0x73,
	0x61, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6c, 0x6c, 0x53, 0x6e, 0x61,
	0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x49, 0x0a,
	0x0a, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x4e, 0x6f, 0x77, 0x12, 0x1c, 0x2e, 0x71, 0x75,
	0x61, 0x73, 0x61, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x4e,
	0x6f, 0x77, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1d, 0x2e, 0x71, 0x75, 0x61, 0x73,
	0x61, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x4e, 0x6f, 0x77,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x20, 0x5a, 0x1e, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x68, 0x73, 0x70, 0x68, 0x69, 0x6e, 0x78,
	0x2f, 0x71, 0x75, 0x61, 0x73, 0x61, 0x72, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var file_v1_api_proto_goTypes = []interface{}{
	(*Command)(nil),                 // 0: quasar.v1.Command
	(*AppendEntriesRequest)(nil),    // 1: quasar.v1.AppendEntriesRequest
	(*RequestVoteRequest)(nil),      // 2: quasar.v1.RequestVoteRequest
	(*InstallSnapshotRequest)(nil),  // 3: quasar.v1.InstallSnapshotRequest
	(*TimeoutNowRequest)(nil),       // 4: quasar.v1.TimeoutNowRequest
	(*CommandResponse)(nil),         // 5: quasar.v1.CommandResponse
	(*AppendEntriesResponse)(nil),   // 6: quasar.v1.AppendEntriesResponse
	(*RequestVoteResponse)(nil),     // 7: quasar.v1.RequestVoteResponse
	(*InstallSnapshotResponse)(nil), // 8: quasar.v1.InstallSnapshotResponse
	(*TimeoutNowResponse)(nil),      // 9: quasar.v1.TimeoutNowResponse
}
var file_v1_api_proto_depIdxs = []int32{
	0, // 0: quasar.v1.QuasarService.Apply:input_type -> quasar.v1.Command
	1, // 1: quasar.v1.QuasarService.AppendEntries:input_type -> quasar.v1.AppendEntriesRequest
	2, // 2: quasar.v1.QuasarService.RequestVote:input_type -> quasar.v1.RequestVoteRequest
	3, // 3: quasar.v1.QuasarService.InstallSnapshot:input_type -> quasar.v1.InstallSnapshotRequest
	4, // 4: quasar.v1.QuasarService.TimeoutNow:input_type -> quasar.v1.TimeoutNowRequest
	5, // 5: quasar.v1.QuasarService.Apply:output_type -> quasar.v1.CommandResponse
	6, // 6: quasar.v1.QuasarService.AppendEntries:output_type -> quasar.v1.AppendEntriesResponse
	7, // 7: quasar.v1.QuasarService.RequestVote:output_type -> quasar.v1.RequestVoteResponse
	8, // 8: quasar.v1.QuasarService.InstallSnapshot:output_type -> quasar.v1.InstallSnapshotResponse
	9, // 9: quasar.v1.QuasarService.TimeoutNow:output_type -> quasar.v1.TimeoutNowResponse
	5, // [5:10] is the sub-list for method output_type
	0, // [0:5] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_v1_api_proto_init() }
func file_v1_api_proto_init() {
	if File_v1_api_proto != nil {
		return
	}
	file_v1_cmd_proto_init()
	file_v1_raft_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_v1_api_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_v1_api_proto_goTypes,
		DependencyIndexes: file_v1_api_proto_depIdxs,
	}.Build()
	File_v1_api_proto = out.File
	file_v1_api_proto_rawDesc = nil
	file_v1_api_proto_goTypes = nil
	file_v1_api_proto_depIdxs = nil
}
