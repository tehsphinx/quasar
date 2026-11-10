package quasar

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
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

// TestMaxLogBytesCapsTheLog covers WithMaxLogBytes end to end. raft's own
// settings never fire here: its default SnapshotThreshold is far above the
// entries written. The cap has to make the cache snapshot by itself, and the
// compaction has to keep the newest entries that fit half the cap, not just
// raft's TrailingLogs.
func TestMaxLogBytesCapsTheLog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		trailing = 10
		entries  = 300
		value    = 1024
		maxBytes = 100 * value
	)
	raftCfg := raft.DefaultConfig()
	raftCfg.TrailingLogs = trailing

	_, tr := transports.NewInmemTransport("")
	c, err := NewKVCache(ctx,
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
		WithRaftConfig(raftCfg),
		WithMaxLogBytes(maxBytes),
	)
	if err != nil {
		t.Fatalf("NewKVCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	for i := range entries {
		if _, err := c.Store(ctx, "key"+strconv.Itoa(i), make([]byte, value)); err != nil {
			t.Fatalf("Store: %v", err)
		}
	}

	logs, ok := c.logStore.(*store)
	if !ok {
		t.Fatalf("log store is a %T", c.logStore)
	}
	sized, ok := logs.LogStore.(logSizer)
	if !ok {
		t.Fatalf("wrapped log store is a %T", logs.LogStore)
	}

	// The snapshot runs off the append path, so the last one may still be on
	// its way.
	for deadline := time.Now().Add(10 * time.Second); sized.Bytes() > maxBytes; {
		if time.Now().After(deadline) {
			t.Fatalf("the log holds %d bytes, over its cap of %d", sized.Bytes(), maxBytes)
		}
		time.Sleep(20 * time.Millisecond)
	}

	first, _ := c.logStore.FirstIndex()
	last, _ := c.logStore.LastIndex()
	if first == 1 {
		t.Fatalf("all %d entries are still in the log: the cap never snapshotted", last)
	}
	if kept := last - first + 1; kept <= trailing {
		t.Errorf("kept %d entries, which is TrailingLogs; half the cap has room for more", kept)
	}
}
