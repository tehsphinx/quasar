package transports

import (
	"time"

	"github.com/hashicorp/raft"
)

// appendFuture is used for waiting on a pipelined append
// entries RPC.
type appendFuture struct {
	start time.Time
	args  *raft.AppendEntriesRequest
	resp  *raft.AppendEntriesResponse
	deferError
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *raft.AppendEntriesRequest {
	return a.args
}

func (a *appendFuture) Response() *raft.AppendEntriesResponse {
	return a.resp
}

// deferError can be embedded to allow a future
// to provide an error in the future.
type deferError struct {
	err        error
	errCh      chan error
	ShutdownCh chan struct{}
	responded  bool
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		// Note that when we've received a nil error, this
		// won't trigger, but the channel is closed after
		// send so we'll still return nil below.
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case d.err = <-d.errCh:
	case <-d.ShutdownCh:
		d.err = raft.ErrRaftShutdown
	}
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}
