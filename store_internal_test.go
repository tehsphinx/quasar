package quasar

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/stores"
)

// TestStoreForwardsIsMonotonic guards the one silent failure mode of RT-13771:
// the wrapper embeds raft.LogStore as an interface, so without an explicit
// IsMonotonic method raft's type assertion fails and it goes on leaving a gap in
// the log on snapshot install and user restore — which a ring buffer cannot hold.
func TestStoreForwardsIsMonotonic(t *testing.T) {
	wrapped := wrapStore(stores.NewLogRing(), nil)

	monotonic, ok := any(wrapped).(raft.MonotonicLogStore)
	if !ok {
		t.Fatal("the wrapped store does not satisfy raft.MonotonicLogStore")
	}
	if !monotonic.IsMonotonic() {
		t.Error("IsMonotonic: got false, expected the wrapped LogRing's true")
	}
}

// TestStoreIsMonotonicFalseForGapTolerantStore keeps the forwarding honest: a
// store that does not declare itself monotonic must not be reported as one.
func TestStoreIsMonotonicFalseForGapTolerantStore(t *testing.T) {
	if wrapStore(raft.NewInmemStore(), nil).IsMonotonic() {
		t.Error("IsMonotonic: got true for raft.InmemStore")
	}
}
