package quasar

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/stores"
)

// monotonicStub stands in for a log store that declares itself monotonic, which
// no store wrapped here does any more (see stores.LogRing.IsMonotonic). The
// forwarding still has to work, so the test needs something that says true.
type monotonicStub struct {
	raft.LogStore
}

func (monotonicStub) IsMonotonic() bool { return true }

// TestStoreForwardsIsMonotonic guards the one silent failure mode of RT-13771:
// the wrapper embeds raft.LogStore as an interface, so without an explicit
// IsMonotonic method raft's type assertion fails and the wrapped store's answer
// never reaches raft at all.
func TestStoreForwardsIsMonotonic(t *testing.T) {
	wrapped := wrapStore(monotonicStub{LogStore: raft.NewInmemStore()}, nil)

	monotonic, ok := any(wrapped).(raft.MonotonicLogStore)
	if !ok {
		t.Fatal("the wrapped store does not satisfy raft.MonotonicLogStore")
	}
	if !monotonic.IsMonotonic() {
		t.Error("IsMonotonic: got false, expected the wrapped store's true")
	}
}

// TestStoreIsMonotonicFalseForGapTolerantStore keeps the forwarding honest: a
// store that does not declare itself monotonic must not be reported as one.
func TestStoreIsMonotonicFalseForGapTolerantStore(t *testing.T) {
	if wrapStore(raft.NewInmemStore(), nil).IsMonotonic() {
		t.Error("IsMonotonic: got true for raft.InmemStore")
	}
}

// TestStoreIsMonotonicFalseForLogRing is the RT-14463 guard at the level raft
// actually asserts on. Reporting true here sends installSnapshot down
// removeOldLogs, which wipes the log without lowering raft's lastLog, and a
// follower that was ahead of the snapshot panics in processLogs on the next
// AppendEntries that advances the commit index.
func TestStoreIsMonotonicFalseForLogRing(t *testing.T) {
	if wrapStore(stores.NewLogRing(), nil).IsMonotonic() {
		t.Error("IsMonotonic: got true for stores.LogRing; see RT-14463")
	}
}
