package quasar

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// noPreVoteTransport is a transports.Transport with no pre-vote support:
// embedding the interface hides the concrete transport's RequestPreVote.
type noPreVoteTransport struct {
	transports.Transport
}

// TestHideCloseHidesCloseKeepsPreVote is the RT-14147 regression test.
//
// The rebuild paths rebuild raft on the same transport, so they can only await
// the old instance's shutdown future if raft cannot close the transport — hence
// hideClose. Hiding Close() must not also hide pre-vote support, which is
// carried by raft.WithPreVote and not declared on transports.Transport.
func TestHideCloseHidesCloseKeepsPreVote(t *testing.T) {
	_, tr := transports.NewInmemTransport("")
	if _, ok := interface{}(tr).(raft.WithClose); !ok {
		t.Fatal("InmemTransport no longer implements raft.WithClose; this test proves nothing")
	}

	wrapped := hideClose(tr)
	if _, ok := wrapped.(raft.WithClose); ok {
		t.Fatal("wrapped transport still satisfies raft.WithClose: shutdownFuture.Error() would close it")
	}
	if _, ok := wrapped.(raft.WithPreVote); !ok {
		t.Fatal("wrapped transport lost raft.WithPreVote: raft would silently disable pre-vote")
	}

	noPreVote := hideClose(noPreVoteTransport{Transport: tr})
	if _, ok := noPreVote.(raft.WithClose); ok {
		t.Fatal("wrapped transport still satisfies raft.WithClose")
	}
	if _, ok := noPreVote.(raft.WithPreVote); ok {
		t.Fatal("wrapper claims pre-vote support the transport does not have: every pre-vote would be denied")
	}
}
