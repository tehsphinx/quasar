package quasar

import (
	"io"
	"testing"
	"time"

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

// TestSnapshotStoreKeepsANewerInstallOverALateLocalSnapshot: on a follower,
// raft's takeSnapshot can still be persisting a local snapshot while the main
// loop installs and restores a newer one from the leader. With the create
// deferred to Close, the local snapshot closed last replaced the install
// (RT-15590). Its Close must fail and leave the install in place, while a
// snapshot at the stored index, as raft.RecoverCluster writes it, is stored.
func TestSnapshotStoreKeepsANewerInstallOverALateLocalSnapshot(t *testing.T) {
	store := wrapSnapshotStore(raft.NewInmemSnapshotStore(), wrapFSM(newSnapFSM()))
	_, tr := raft.NewInmemTransport("")

	create := func(index uint64, data string) raft.SnapshotSink {
		t.Helper()
		sink, err := store.Create(1, index, 1, raft.Configuration{}, 1, tr)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := io.WriteString(sink, data); err != nil {
			t.Fatalf("write: %v", err)
		}
		return sink
	}
	expectLatest := func(index uint64, data string) {
		t.Helper()
		snaps, err := store.List()
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(snaps) != 1 || snaps[0].Index != index {
			t.Fatalf("expected only the snapshot at index %d, got %+v", index, snaps)
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
		if string(got) != data {
			t.Fatalf("expected %q, got %q", data, got)
		}
	}

	local := create(100, "local")
	if err := create(500, "install").Close(); err != nil {
		t.Fatalf("close install: %v", err)
	}
	expectLatest(500, "install")

	for range 2 {
		if err := local.Close(); err == nil {
			t.Fatal("expected the late local snapshot's close to fail")
		}
	}
	expectLatest(500, "install")

	if err := create(500, "recovered").Close(); err != nil {
		t.Fatalf("close recovered: %v", err)
	}
	expectLatest(500, "recovered")
}

// TestSnapshotStoreOpenWaitsForAClosingSnapshot: Close makes the snapshot the
// wrapped store's latest before it writes the buffer into it. A leader whose
// replication opened it in between streamed an empty snapshot, which the
// follower stored at that index (RT-15590). Open must wait until Close has
// written it.
func TestSnapshotStoreOpenWaitsForAClosingSnapshot(t *testing.T) {
	inner := stallingSnapshotStore{
		SnapshotStore: raft.NewInmemSnapshotStore(),
		writing:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	store := wrapSnapshotStore(inner, wrapFSM(newSnapFSM()))
	_, tr := raft.NewInmemTransport("")

	sink, err := store.Create(1, 7, 1, raft.Configuration{}, 1, tr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := io.WriteString(sink, "complete"); err != nil {
		t.Fatalf("write: %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- sink.Close() }()
	<-inner.writing

	snaps, err := store.List()
	if err != nil || len(snaps) != 1 {
		t.Fatalf("expected the closing snapshot to be listed, got %+v, %v", snaps, err)
	}
	opened := make(chan string, 1)
	go func() {
		_, rc, err := store.Open(snaps[0].ID)
		if err != nil {
			opened <- "open: " + err.Error()
			return
		}
		got, _ := io.ReadAll(rc)
		opened <- string(got)
	}()

	select {
	case got := <-opened:
		t.Fatalf("open returned %q while close was still writing", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(inner.release)
	if err := <-closed; err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := <-opened; got != "complete" {
		t.Fatalf("expected the complete snapshot, got %q", got)
	}
}

// stallingSnapshotStore hands out sinks whose Write stops until release is
// closed, after closing writing.
type stallingSnapshotStore struct {
	raft.SnapshotStore
	writing chan struct{}
	release chan struct{}
}

func (s stallingSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64,
	configuration raft.Configuration, configurationIndex uint64, trans raft.Transport,
) (raft.SnapshotSink, error) {
	sink, err := s.SnapshotStore.Create(version, index, term, configuration, configurationIndex, trans)
	return stallingSink{SnapshotSink: sink, store: s}, err
}

type stallingSink struct {
	raft.SnapshotSink
	store stallingSnapshotStore
}

func (s stallingSink) Write(p []byte) (int, error) {
	close(s.store.writing)
	<-s.store.release
	return s.SnapshotSink.Write(p)
}
