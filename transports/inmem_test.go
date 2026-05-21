package transports

import (
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestInmemTransport_RequestPreVote(t *testing.T) {
	_, trans1 := NewInmemTransport("")
	_, trans2 := NewInmemTransport("")
	trans2.Connect(trans1.LocalAddr(), trans1)

	args := raft.RequestPreVoteRequest{
		Term:         42,
		LastLogIndex: 200,
		LastLogTerm:  41,
		RPCHeader:    raft.RPCHeader{Addr: []byte("butters")},
	}

	resp := raft.RequestPreVoteResponse{
		Term:    42,
		Granted: true,
	}

	go func() {
		select {
		case rpc := <-trans1.Consumer():
			req, ok := rpc.Command.(*raft.RequestPreVoteRequest)
			if !ok {
				t.Errorf("unexpected command type %T", rpc.Command)
				return
			}
			if !reflect.DeepEqual(req, &args) {
				t.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}
			rpc.Respond(&resp, nil)
		case <-time.After(200 * time.Millisecond):
			t.Errorf("timeout")
		}
	}()

	var out raft.RequestPreVoteResponse
	if err := trans2.RequestPreVote("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}
