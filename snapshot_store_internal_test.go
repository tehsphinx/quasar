package quasar

import (
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// TestSnapshotStoreKeepsTheLastSnapshotOnCancel: raft cancels a sink whose
// Persist or InstallSnapshot copy failed. The in-memory store had already made
// it the latest snapshot, so the last good one was replaced by a truncated one
// (RT-15590). A canceled sink must leave the previous snapshot in place, even
// when a Persist that defers Close closes it after the cancel, and a sink
// closed twice, by the FSM and by raft, must be stored once and intact.
func TestSnapshotStoreKeepsTheLastSnapshotOnCancel(t *testing.T) {
	store := wrapSnapshotStore(raft.NewInmemSnapshotStore(), wrapFSM(newSnapFSM()))
	_, tr := raft.NewInmemTransport("")

	write := func(index uint64, data string, finish func(raft.SnapshotSink) error) {
		t.Helper()
		sink, err := store.Create(1, index, 1, raft.Configuration{}, 1, tr)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := io.WriteString(sink, data); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := finish(sink); err != nil {
			t.Fatalf("finish: %v", err)
		}
	}
	closeTwice := func(sink raft.SnapshotSink) error {
		if err := sink.Close(); err != nil {
			return err
		}
		return sink.Close()
	}
	cancelThenClose := func(sink raft.SnapshotSink) error {
		if err := sink.Cancel(); err != nil {
			return err
		}
		return sink.Close()
	}

	write(5, "complete", closeTwice)
	write(9, "trunc", raft.SnapshotSink.Cancel)
	write(9, "trunc", cancelThenClose)

	snaps, err := store.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(snaps) != 1 || snaps[0].Index != 5 {
		t.Fatalf("expected only the snapshot at index 5, got %+v", snaps)
	}
	_, rc, err := store.Open(snaps[0].ID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "complete" {
		t.Fatalf("expected the complete snapshot, got %q", got)
	}
}
