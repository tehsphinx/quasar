package quasar

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

func newKVLeaderCache(ctx context.Context, t *testing.T) *KVCache {
	t.Helper()

	_, tr := transports.NewInmemTransport("")
	c, err := NewKVCache(ctx,
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
	)
	if err != nil {
		t.Fatalf("NewKVCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	return c
}

// TestKVCacheSnapshotRestore is the RT-13042 M8 regression test for the
// snapshot path. kvFSM.Snapshot and Restore used to panic "implement me" —
// fatal in practice, because raft's default config auto-snapshots any FSM
// that accumulates more than SnapshotThreshold log entries, so a
// long-running KVCache crashed its process. Snapshot + Restore must
// round-trip the store content.
func TestKVCacheSnapshotRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c := newKVLeaderCache(ctx, t)

	if _, err := c.Store(ctx, "artist", []byte("Roberto")); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if _, err := c.Store(ctx, "instrument", []byte("violin")); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Pre-fix this panics the process.
	meta, rc, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = rc.Close() }()

	// Overwrite and add data after the snapshot, then restore the snapshot:
	// the post-snapshot changes must be gone.
	if _, err := c.Store(ctx, "artist", []byte("Else")); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if _, err := c.Store(ctx, "extra", []byte("gone-after-restore")); err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := c.Restore(ctx, meta, rc); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got, err := c.LoadLocal(ctx, "artist")
	if err != nil {
		t.Fatalf("LoadLocal(artist): %v", err)
	}
	if string(got) != "Roberto" {
		t.Fatalf("artist = %q after restore, want %q", got, "Roberto")
	}
	if _, err := c.LoadLocal(ctx, "extra"); !errors.Is(err, stores.ErrNotFound) {
		t.Fatalf("LoadLocal(extra) after restore = %v, want ErrNotFound", err)
	}
}

// TestKVCacheReset is the RT-13042 M8 regression test for the reset path.
// The log-replicated soft reset used to fail on a KVCache: kvFSM.Apply
// returned "command not implemented" for Command_ResetCache, and
// kvFSM.Reset (the hard-reset path) panicked "implement me". Reset must
// clear the store.
func TestKVCacheReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c := newKVLeaderCache(ctx, t)

	if _, err := c.Store(ctx, "artist", []byte("Roberto")); err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := c.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if _, err := c.LoadLocal(ctx, "artist"); !errors.Is(err, stores.ErrNotFound) {
		t.Fatalf("LoadLocal(artist) after reset = %v, want ErrNotFound", err)
	}
}
