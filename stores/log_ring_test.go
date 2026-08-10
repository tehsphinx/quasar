package stores

import (
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

// payload is 24 bytes, the size of a gatexcache cache entry command.
var payload = []byte("240-byte-command-payload")

func testLog(index uint64) *raft.Log {
	return &raft.Log{
		Index: index,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  append([]byte(nil), payload...),
	}
}

func storeRange(t testing.TB, store raft.LogStore, from, to uint64) {
	t.Helper()

	logs := make([]*raft.Log, 0, to-from+1)
	for i := from; i <= to; i++ {
		logs = append(logs, testLog(i))
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs(%d-%d): %v", from, to, err)
	}
}

func assertRange(t testing.TB, store raft.LogStore, first, last uint64) {
	t.Helper()

	got, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	if got != first {
		t.Errorf("FirstIndex: got %d, expected %d", got, first)
	}

	got, err = store.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if got != last {
		t.Errorf("LastIndex: got %d, expected %d", got, last)
	}
}

// assertReadable checks every index in [first, last] is stored and intact, and
// that the indexes just outside the range are not.
func assertReadable(t testing.TB, store raft.LogStore, first, last uint64) {
	t.Helper()

	for i := first; i <= last; i++ {
		var log raft.Log
		if err := store.GetLog(i, &log); err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
		if log.Index != i {
			t.Fatalf("GetLog(%d): got index %d", i, log.Index)
		}
		if string(log.Data) != string(payload) {
			t.Fatalf("GetLog(%d): got data %q", i, log.Data)
		}
	}

	var log raft.Log
	if first > 1 {
		if err := store.GetLog(first-1, &log); !errors.Is(err, raft.ErrLogNotFound) {
			t.Errorf("GetLog(%d) below range: got %v, expected ErrLogNotFound", first-1, err)
		}
	}
	if err := store.GetLog(last+1, &log); !errors.Is(err, raft.ErrLogNotFound) {
		t.Errorf("GetLog(%d) above range: got %v, expected ErrLogNotFound", last+1, err)
	}
}

func TestLogRing_Empty(t *testing.T) {
	ring := NewLogRing()

	assertRange(t, ring, 0, 0)

	var log raft.Log
	if err := ring.GetLog(1, &log); !errors.Is(err, raft.ErrLogNotFound) {
		t.Errorf("GetLog on empty store: got %v, expected ErrLogNotFound", err)
	}
	if err := ring.DeleteRange(1, 100); err != nil {
		t.Errorf("DeleteRange on empty store: %v", err)
	}
}

func TestLogRing_StoreAndGet(t *testing.T) {
	ring := NewLogRing()

	if err := ring.StoreLog(testLog(7)); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	assertRange(t, ring, 7, 7)

	storeRange(t, ring, 8, 20)
	assertRange(t, ring, 7, 20)
	assertReadable(t, ring, 7, 20)
}

// TestLogRing_DeleteRangeHead is log compaction after a snapshot: raft deletes
// from FirstIndex up to the newest index it no longer needs.
func TestLogRing_DeleteRangeHead(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, 100)

	if err := ring.DeleteRange(1, 60); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	assertRange(t, ring, 61, 100)
	assertReadable(t, ring, 61, 100)

	// Appends continue where the log ends, not where the buffer starts.
	storeRange(t, ring, 101, 110)
	assertRange(t, ring, 61, 110)
	assertReadable(t, ring, 61, 110)
}

// TestLogRing_DeleteRangeTail is a follower clearing a conflicting suffix: raft
// deletes from the conflicting index up to its last index.
func TestLogRing_DeleteRangeTail(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, 100)

	if err := ring.DeleteRange(41, 100); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	assertRange(t, ring, 1, 40)
	assertReadable(t, ring, 1, 40)

	storeRange(t, ring, 41, 50)
	assertRange(t, ring, 1, 50)
	assertReadable(t, ring, 1, 50)
}

// TestLogRing_DeleteRangeAll is removeOldLogs and RecoverCluster: the store is
// emptied and then reseeded from whatever index raft continues at.
func TestLogRing_DeleteRangeAll(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, 100)

	if err := ring.DeleteRange(1, 100); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	assertRange(t, ring, 0, 0)

	storeRange(t, ring, 5001, 5010)
	assertRange(t, ring, 5001, 5010)
	assertReadable(t, ring, 5001, 5010)
}

func TestLogRing_DeleteRangeOutsideStoredRange(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 100, 200)

	for _, tt := range []struct{ minIdx, maxIdx uint64 }{
		{1, 99},    // entirely below
		{201, 300}, // entirely above
		{200, 100}, // inverted
	} {
		if err := ring.DeleteRange(tt.minIdx, tt.maxIdx); err != nil {
			t.Errorf("DeleteRange(%d, %d): %v", tt.minIdx, tt.maxIdx, err)
		}
		assertRange(t, ring, 100, 200)
	}
}

func TestLogRing_DeleteRangeInteriorRejected(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, 100)

	if err := ring.DeleteRange(40, 60); err == nil {
		t.Error("DeleteRange(40, 60): expected an error for an interior range")
	}
	assertRange(t, ring, 1, 100)
	assertReadable(t, ring, 1, 100)
}

// TestLogRing_WrapAround drives the stored range across the end of the buffer,
// which is where the offset arithmetic is easiest to get wrong.
func TestLogRing_WrapAround(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, initialLogRingCap)

	if got := len(ring.buf); got != initialLogRingCap {
		t.Fatalf("capacity: got %d, expected %d", got, initialLogRingCap)
	}

	// Free the first half, then refill it from the far end of the buffer.
	half := uint64(initialLogRingCap / 2)
	if err := ring.DeleteRange(1, half); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	storeRange(t, ring, initialLogRingCap+1, initialLogRingCap+half)

	if got := len(ring.buf); got != initialLogRingCap {
		t.Fatalf("capacity grew to %d; the freed slots should have been reused", got)
	}
	assertRange(t, ring, half+1, initialLogRingCap+half)
	assertReadable(t, ring, half+1, initialLogRingCap+half)

	// A tail delete that wraps back past the start of the buffer.
	if err := ring.DeleteRange(initialLogRingCap-10, initialLogRingCap+half); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	assertRange(t, ring, half+1, initialLogRingCap-11)
	assertReadable(t, ring, half+1, initialLogRingCap-11)
}

func TestLogRing_Growth(t *testing.T) {
	ring := NewLogRing()

	// Start the ring wrapped, so growth has to unwrap it in the right order.
	storeRange(t, ring, 1, initialLogRingCap)
	if err := ring.DeleteRange(1, 100); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	const last = 5000
	storeRange(t, ring, initialLogRingCap+1, last)

	assertRange(t, ring, 101, last)
	assertReadable(t, ring, 101, last)
	if len(ring.buf) < last-100 {
		t.Errorf("capacity %d cannot hold %d entries", len(ring.buf), last-100)
	}
}

// TestLogRing_OverwriteInPlace covers the contract compatibility case: the map in
// raft.InmemStore accepts a store at an index it already holds and keeps the
// entries above it.
func TestLogRing_OverwriteInPlace(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, 10)

	replaced := testLog(5)
	replaced.Term = 9
	if err := ring.StoreLog(replaced); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}

	assertRange(t, ring, 1, 10)

	var log raft.Log
	if err := ring.GetLog(5, &log); err != nil {
		t.Fatalf("GetLog: %v", err)
	}
	if log.Term != 9 {
		t.Errorf("Term: got %d, expected 9", log.Term)
	}
}

func TestLogRing_StoreBelowFirstIndexRejected(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 100, 110)

	if err := ring.StoreLog(testLog(99)); err == nil {
		t.Error("StoreLog(99): expected an error below the first stored index")
	}
}

// TestLogRing_ForwardGapReseeds documents the unreachable-by-raft case: with
// IsMonotonic raft never leaves a hole, and if it ever did the ring restarts at
// the new index rather than failing every following append.
func TestLogRing_ForwardGapReseeds(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, 10)

	if err := ring.StoreLog(testLog(1000)); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	assertRange(t, ring, 1000, 1000)
	assertReadable(t, ring, 1000, 1000)
}

// TestLogRing_TruncatedEntriesReleased covers how truncated payloads are let go:
// a head or tail truncation leaves the slot alone (zeroing it would put the range
// length back into the lock hold), the next append reuses it, and a full delete
// releases the buffer outright.
func TestLogRing_TruncatedEntriesReleased(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, 100)

	truncated := &ring.buf[0]
	if err := ring.DeleteRange(1, 60); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if truncated.Data == nil {
		t.Error("a truncated slot was zeroed; that is work proportional to the range")
	}

	// Appending far enough brings the head round onto the truncated slots.
	storeRange(t, ring, 101, 160)
	if truncated.Index != 129 {
		t.Errorf("the truncated slot was not reused: holds index %d, expected 129", truncated.Index)
	}

	if err := ring.DeleteRange(129, 160); err != nil { // tail, leaving 61-128
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := ring.DeleteRange(61, 128); err != nil { // the rest
		t.Fatalf("DeleteRange: %v", err)
	}
	if ring.buf != nil {
		t.Error("a full delete should release the buffer")
	}
}

func TestLogRing_IsMonotonic(t *testing.T) {
	if !NewLogRing().IsMonotonic() {
		t.Error("IsMonotonic: got false; a ring cannot represent the gap raft leaves otherwise")
	}
}

// TestLogRing_AgainstInmemStore runs the three truncation shapes raft actually
// performs against raft.NewInmemStore as the oracle. This is what catches an
// off-by-one in the wrap arithmetic.
func TestLogRing_AgainstInmemStore(t *testing.T) {
	ring := NewLogRing()
	oracle := raft.NewInmemStore()

	rnd := rand.New(rand.NewSource(1))
	next := uint64(1)

	for step := 0; step < 2000; step++ {
		first, _ := oracle.FirstIndex()
		last, _ := oracle.LastIndex()

		switch op := rnd.Intn(10); {
		case op < 6 || last == 0: // append
			count := uint64(rnd.Intn(50) + 1)
			storeRange(t, ring, next, next+count-1)
			storeRange(t, oracle, next, next+count-1)
			next += count
		case op < 8: // head truncate
			maxIdx := first + uint64(rnd.Int63n(int64(last-first+1)))
			if err := ring.DeleteRange(first, maxIdx); err != nil {
				t.Fatalf("step %d: ring.DeleteRange(%d, %d): %v", step, first, maxIdx, err)
			}
			if err := oracle.DeleteRange(first, maxIdx); err != nil {
				t.Fatalf("step %d: oracle.DeleteRange: %v", step, err)
			}
		case op < 9: // tail truncate
			minIdx := first + uint64(rnd.Int63n(int64(last-first+1)))
			if err := ring.DeleteRange(minIdx, last); err != nil {
				t.Fatalf("step %d: ring.DeleteRange(%d, %d): %v", step, minIdx, last, err)
			}
			if err := oracle.DeleteRange(minIdx, last); err != nil {
				t.Fatalf("step %d: oracle.DeleteRange: %v", step, err)
			}
			next = minIdx // raft re-sends from the conflicting index
		default: // full clear
			if err := ring.DeleteRange(first, last); err != nil {
				t.Fatalf("step %d: ring.DeleteRange(%d, %d): %v", step, first, last, err)
			}
			if err := oracle.DeleteRange(first, last); err != nil {
				t.Fatalf("step %d: oracle.DeleteRange: %v", step, err)
			}
		}

		wantFirst, _ := oracle.FirstIndex()
		wantLast, _ := oracle.LastIndex()
		assertRange(t, ring, wantFirst, wantLast)
		if t.Failed() {
			t.Fatalf("step %d: range diverged from raft.InmemStore", step)
		}
		if wantLast != 0 {
			assertReadable(t, ring, wantFirst, wantLast)
		}
	}
}

// TestLogRing_ConcurrentAccess mirrors how raft uses the store: the main
// goroutine appends, the snapshot goroutine truncates, and the replication
// goroutines read. Fails under -race without internal locking.
func TestLogRing_ConcurrentAccess(t *testing.T) {
	ring := NewLogRing()
	storeRange(t, ring, 1, 1000)

	done := make(chan struct{})
	var writers, readers sync.WaitGroup

	writers.Add(1)
	go func() { // appender
		defer writers.Done()

		for i := uint64(1001); ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := ring.StoreLog(testLog(i)); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	writers.Add(1)
	go func() { // compactor
		defer writers.Done()

		for {
			select {
			case <-done:
				return
			default:
			}
			first, _ := ring.FirstIndex()
			last, _ := ring.LastIndex()
			if last-first > 500 {
				if err := ring.DeleteRange(first, last-500); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()

	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() { // readers; a moving window makes ErrLogNotFound expected
			defer readers.Done()

			for i := 0; i < 20000; i++ {
				first, _ := ring.FirstIndex()
				var log raft.Log
				if err := ring.GetLog(first, &log); err != nil && !errors.Is(err, raft.ErrLogNotFound) {
					t.Error(err)
					return
				}
			}
		}()
	}

	readers.Wait()
	close(done)
	writers.Wait()
}

// TestLogRing_BytesPerEntry is acceptance criterion 3: live heap per entry below
// what the map store costs. Both stores are measured in the same run so the
// comparison does not depend on a hardcoded baseline.
func TestLogRing_BytesPerEntry(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates half a million log entries twice")
	}

	const entries = 500_000

	ringBytes := bytesPerEntry(t, NewLogRing(), entries)
	inmemBytes := bytesPerEntry(t, raft.NewInmemStore(), entries)
	t.Logf("live heap per entry: LogRing %.1f B, raft.InmemStore %.1f B", ringBytes, inmemBytes)

	if ringBytes >= inmemBytes {
		t.Errorf("LogRing costs %.1f B/entry, raft.InmemStore %.1f B/entry", ringBytes, inmemBytes)
	}
}

func bytesPerEntry(t testing.TB, store raft.LogStore, entries uint64) float64 {
	t.Helper()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	for i := uint64(1); i <= entries; i += 1000 {
		storeRange(t, store, i, i+999)
	}

	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(store)

	return float64(after.HeapAlloc-before.HeapAlloc) / float64(entries)
}
