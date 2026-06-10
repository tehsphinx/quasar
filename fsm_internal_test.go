package quasar

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// snapFSM is a minimal snapshot-capable FSM for internal tests: a string map
// whose ApplyCmd stores the raw command under a counter key and whose
// snapshot round-trips the map as JSON.
type snapFSM struct {
	mu   sync.Mutex
	data map[string]string
	seq  int
}

func newSnapFSM() *snapFSM {
	return &snapFSM{data: map[string]string{}}
}

func (s *snapFSM) Inject(*FSMInjector) {}

func (s *snapFSM) ApplyCmd(cmd []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	s.data[string(cmd)] = string(cmd)
	return nil
}

func (s *snapFSM) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bts, err := json.Marshal(s.data)
	if err != nil {
		return nil, err
	}
	return snapFSMSnapshot(bts), nil
}

func (s *snapFSM) Restore(snapshot io.ReadCloser) error {
	defer func() { _ = snapshot.Close() }()
	bts, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}
	data := map[string]string{}
	if r := json.Unmarshal(bts, &data); r != nil {
		return r
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}

func (s *snapFSM) Reset() error {
	s.mu.Lock()
	s.data = map[string]string{}
	s.mu.Unlock()
	return nil
}

type snapFSMSnapshot []byte

func (s snapFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		return err
	}
	return sink.Close()
}

func (s snapFSMSnapshot) Release() {}

// dribbleReader returns at most one byte per Read call.
type dribbleReader struct {
	data []byte
}

func (r *dribbleReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}

func (r *dribbleReader) Close() error { return nil }

// TestFSMRestoreToleratesShortReads is part of the RT-13042 M7 regression.
// The embedded lastApplied was read with a single Read call; an io.Reader
// may legally return fewer than 8 bytes with nil error, which Restore turned
// into an error — and on the user-restore path raft v1.7.x PANICS the whole
// process when the FSM's Restore errors. A reader that dribbles one byte at
// a time must restore cleanly.
func TestFSMRestoreToleratesShortReads(t *testing.T) {
	fsm := wrapFSM(newSnapFSM())

	var payload bytes.Buffer
	payload.Write(uint64ToBytes(7))
	payload.WriteString(`{"k":"v"}`)

	if err := fsm.Restore(&dribbleReader{data: payload.Bytes()}); err != nil {
		t.Fatalf("Restore failed on a short-read reader: %v", err)
	}
	if got := fsm.getLastApplied(); got != 7 {
		t.Fatalf("lastApplied = %d, want 7 (embedded value fallback without snapshot store)", got)
	}
}

// TestSystemUIDCommitTimeRegistration is the RT-13042 M10 regression test.
// Non-command log indexes used to be registered at STORE time — but logs are
// stored before they are committed, so a follower storing an uncommitted
// configuration entry advanced lastApplied immediately. If a new leader later
// truncated and replaced that entry, WaitFor had already released its waiters
// without the real entry being applied — a hole in the read-your-writes
// guarantee. Configuration entries must only register at commit time (via
// raft.ConfigurationStore); noop entries — which raft never hands to the FSM
// on any path — keep the store-time registration.
func TestSystemUIDCommitTimeRegistration(t *testing.T) {
	fsm := wrapFSM(newSnapFSM())
	st := wrapStore(raft.NewInmemStore(), fsm)

	// A stored-but-uncommitted configuration entry must not move lastApplied.
	if err := st.StoreLog(&raft.Log{Index: 1, Type: raft.LogConfiguration}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	if got := fsm.getLastApplied(); got != 0 {
		t.Fatalf("lastApplied = %d after storing an uncommitted configuration entry, want 0", got)
	}

	// Noop entries never reach the FSM, so they still register at store
	// time. This mirrors the real ordering on a fresh leader: the noop at
	// index 2 is stored BEFORE the configuration entry at index 1 commits,
	// so it queues behind the not-yet-registered config index.
	if err := st.StoreLogs([]*raft.Log{{Index: 2, Type: raft.LogNoop}}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	if got := fsm.getLastApplied(); got != 0 {
		t.Fatalf("lastApplied = %d with the noop queued behind the uncommitted config entry, want 0", got)
	}

	// On commit raft hands the configuration entry to StoreConfiguration;
	// registering it must also drain the queued noop, or every WaitReady on
	// a fresh cluster hangs until the first command applies.
	fsm.StoreConfiguration(1, raft.Configuration{})
	if got := fsm.getLastApplied(); got != 2 {
		t.Fatalf("lastApplied = %d after committed configuration entry, want 2 (config + queued noop)", got)
	}

	// Command entries register through Apply, never through the store wrapper.
	if err := st.StoreLog(&raft.Log{Index: 3, Type: raft.LogCommand}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	if got := fsm.getLastApplied(); got != 2 {
		t.Fatalf("lastApplied = %d after stored command entry, want 2 (commands apply via FSM.Apply)", got)
	}
}

// TestUserRestoreAlignsLastAppliedWithRaftIndex is the RT-13042 M7 regression
// test for the restore index. Restoring a snapshot through raft.Restore burns
// a fresh index (max(lastIndex, snapshotIndex)+1) — the old code instead set
// lastApplied to the value embedded in the snapshot data plus one, which is
// only correct when nothing was written after the snapshot. With entries
// written between Snapshot and Restore, lastApplied lands far below raft's
// applied index and every WaitFor anchored on the current index hangs until
// its deadline. lastApplied must equal raft's last index right after the
// restore completes.
func TestUserRestoreAlignsLastAppliedWithRaftIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fsm := newSnapFSM()
	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, fsm,
		WithLocalID("leader"),
		WithTransport(tr),
		WithBootstrap(true),
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	defer func() { _ = c.Shutdown() }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := c.store(ctx, "k", []byte{'a' + byte(i)}); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}

	meta, rc, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = rc.Close() }()

	// Entries written after the snapshot move raft's last index well past
	// the snapshot index, so the restore burns an index far above the value
	// embedded in the snapshot data.
	for i := 0; i < 5; i++ {
		if _, err := c.store(ctx, "k", []byte{'z' - byte(i)}); err != nil {
			t.Fatalf("store post-snapshot %d: %v", i, err)
		}
	}

	if err := c.Restore(ctx, meta, rc); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	burned := c.raft().LastIndex()
	if got := c.fsm.getLastApplied(); got != burned {
		t.Fatalf("lastApplied = %d after user restore, want raft last index %d", got, burned)
	}

	// The practical symptom: a WaitFor anchored on the current index (as
	// WaitReady does via masterLastIndex) must return, not hang.
	waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
	defer waitCancel()
	if err := c.fsm.WaitFor(waitCtx, burned); err != nil {
		t.Fatalf("WaitFor(%d) after user restore: %v", burned, err)
	}
}
