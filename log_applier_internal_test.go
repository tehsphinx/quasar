package quasar

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// userLogApplierFSM is an external FSM that happens to implement
// Apply(*raft.Log) — the logApplier interface — but returns its own type,
// not the unexported applyResponse. Before L1, wrapFSM detected it via the
// interface and routed leader applies through the fast path, so applyLocal's
// applyResponse type assertion panicked on the first store.
type userLogApplierFSM struct {
	applied int
}

func (f *userLogApplierFSM) Inject(*FSMInjector)         {}
func (f *userLogApplierFSM) ApplyCmd([]byte) error       { f.applied++; return nil }
func (f *userLogApplierFSM) Restore(io.ReadCloser) error { return nil }
func (f *userLogApplierFSM) Reset() error                { return nil }
func (f *userLogApplierFSM) Apply(*raft.Log) interface{} { return "not an applyResponse" }
func (f *userLogApplierFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

// TestUserLogApplierFSMDoesNotPanicLeader is the L1 regression test.
func TestUserLogApplierFSMDoesNotPanicLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, &userLogApplierFSM{},
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	// Pre-fix this panics inside applyLocal's applyResponse type assertion.
	if _, err := c.store(ctx, "", []byte("v")); err != nil {
		t.Fatalf("store on a user FSM: %v", err)
	}
}
