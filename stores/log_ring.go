package stores

import (
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
)

const (
	// initialLogRingCap is the capacity allocated on the first append. The ring
	// doubles from there, so production windows (TrailingLogs plus one snapshot
	// interval of writes) pay a handful of one-time copies, while a short-lived
	// cache or a test never allocates a large buffer.
	initialLogRingCap = 128

	// logRingGrowth is the factor the buffer grows by when it is full.
	logRingGrowth = 2
)

var (
	_ raft.LogStore          = (*LogRing)(nil)
	_ raft.MonotonicLogStore = (*LogRing)(nil)
)

// NewLogRing creates a new in-memory ring-buffer log store for the cache.
func NewLogRing() *LogRing {
	return &LogRing{}
}

// LogRing implements an in-memory raft.LogStore as a ring buffer.
//
// It replaces raft.NewInmemStore, which upstream documents as unfit for
// production: that store keeps a map[uint64]*raft.Log and deletes one key at a
// time in DeleteRange while holding its exclusive lock, so every log compaction
// blocks replication reads and new writes for the length of the truncated range
// — at the write rates gatexcache sees, hundreds of thousands of map deletes per
// snapshot (RT-13771). Here entries are contiguous by index, so GetLog is an
// offset lookup, StoreLogs an append, and truncation at either end a pointer
// move.
//
// Entries are held by value rather than as *raft.Log, which drops both the map
// bucket and the per-entry Log allocation the previous store paid for and leaves
// the caller's Data as the only allocation per entry. GetLog copies the slot,
// aliasing Data exactly as raft.InmemStore's `*log = *l` does.
//
// It is safe for concurrent use: raft reads the log from its replication
// goroutines while the main and snapshot goroutines append to and truncate it.
type LogRing struct {
	mu sync.RWMutex

	buf   []raft.Log // ring buffer; nil until the first append
	head  int        // position in buf of the entry at index first
	n     int        // number of entries held
	first uint64     // raft index of buf[head]; 0 when empty
}

// IsMonotonic implements the raft.MonotonicLogStore interface.
//
// A ring buffer cannot represent a hole, and raft only ever produces one for a
// store that does not declare itself monotonic: on InstallSnapshot and on a user
// snapshot restore it would keep TrailingLogs entries below the snapshot index
// and then move its last index up to that snapshot, leaving the range between
// the two unstored. Declaring monotonic makes raft clear the log on both paths
// instead, dropping exactly the pre-snapshot entries no follower can be caught
// up from anyway.
//
// Any wrapper between raft and this store has to forward this method: a wrapper
// embedding raft.LogStore as an interface does not satisfy
// raft.MonotonicLogStore on its own, and raft's type assertion failing puts the
// gap-producing path back with no error to show for it.
func (s *LogRing) IsMonotonic() bool { return true }

// FirstIndex implements the raft.LogStore interface.
func (s *LogRing) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.n == 0 {
		return 0, nil
	}
	return s.first, nil
}

// LastIndex implements the raft.LogStore interface.
func (s *LogRing) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastIndex(), nil
}

// GetLog implements the raft.LogStore interface.
func (s *LogRing) GetLog(index uint64, log *raft.Log) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.n == 0 || index < s.first || index > s.lastIndex() {
		return raft.ErrLogNotFound
	}

	*log = s.buf[s.pos(index)]
	return nil
}

// StoreLog stores a log entry.
func (s *LogRing) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries. They are expected to continue where the
// stored range ends, which is what raft does on every path once the store
// declares itself monotonic (see IsMonotonic).
func (s *LogRing) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, log := range logs {
		if err := s.store(log); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange implements the raft.LogStore interface.
//
// raft truncates the log at the head (compaction after a snapshot), at the tail
// (clearing a follower's conflicting suffix) or in full (removeOldLogs,
// RecoverCluster) and never in the middle, so every case it needs is a pointer
// move rather than work proportional to the range. An interior range would
// leave a hole this store cannot represent and is refused.
//
// Truncated slots are deliberately left as they are instead of being zeroed:
// zeroing is a memclr over the range, which is what would put the range length
// back into the time the lock is held. Their payloads stay reachable until the
// slot is reused, which the next append does for free, so the retention is
// bounded by the slack between the buffer and the stored range and disappears as
// the log fills back up. GetLog only ever serves [FirstIndex, LastIndex].
func (s *LogRing) DeleteRange(minIdx, maxIdx uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	last := s.lastIndex()
	if s.n == 0 || minIdx > maxIdx || minIdx > last || maxIdx < s.first {
		return nil
	}

	switch {
	case minIdx <= s.first && maxIdx >= last:
		s.reset()
	case minIdx <= s.first:
		count := int(maxIdx - s.first + 1)
		s.head = (s.head + count) % len(s.buf)
		s.first = maxIdx + 1
		s.n -= count
	case maxIdx >= last:
		s.n -= int(last - minIdx + 1)
	default:
		return fmt.Errorf("log ring: refusing interior delete of %d-%d from range %d-%d",
			minIdx, maxIdx, s.first, last)
	}
	return nil
}

// store appends, or overwrites, a single entry. The caller holds the lock.
func (s *LogRing) store(log *raft.Log) error {
	switch {
	case s.n == 0:
		s.reset()
		s.first = log.Index
	case log.Index < s.first:
		return fmt.Errorf("log ring: index %d is below the first stored index %d", log.Index, s.first)
	case log.Index <= s.lastIndex():
		// Overwrite in place and keep the entries above it, as the map
		// assignment in raft.InmemStore does. raft clears a conflicting suffix
		// before storing, so this is contract compatibility, not a live path.
		s.buf[s.pos(log.Index)] = *log
		return nil
	case log.Index > s.lastIndex()+1:
		// A gap is unreachable while raft honours IsMonotonic. Restarting the
		// ring at the new index drops the same entries raft's removeOldLogs
		// would have dropped, which keeps replication going rather than
		// failing every subsequent append.
		s.reset()
		s.first = log.Index
	}

	if s.n == len(s.buf) {
		s.grow()
	}
	s.buf[s.pos(s.first+uint64(s.n))] = *log
	s.n++
	return nil
}

// grow replaces the buffer with a larger one holding the entries from its front.
// The ring never shrinks: the window sits at the TrailingLogs high-water mark by
// design, so capacity released would only have to be re-acquired.
func (s *LogRing) grow() {
	capacity := len(s.buf) * logRingGrowth
	if capacity < initialLogRingCap {
		capacity = initialLogRingCap
	}

	buf := make([]raft.Log, capacity)
	copied := copy(buf, s.buf[s.head:])
	copy(buf[copied:], s.buf[:s.head])

	s.buf = buf
	s.head = 0
}

// reset drops every entry and releases the buffer with them, which is the only
// truncation that can release it: a full delete comes from removeOldLogs or
// RecoverCluster, where the log is being rebuilt from a snapshot anyway. The
// caller holds the lock.
func (s *LogRing) reset() {
	s.buf = nil
	s.head = 0
	s.n = 0
	s.first = 0
}

// lastIndex reports the highest stored index, 0 when empty. The caller holds the
// lock.
func (s *LogRing) lastIndex() uint64 {
	if s.n == 0 {
		return 0
	}
	return s.first + uint64(s.n) - 1
}

// pos maps a raft index to its slot in the buffer. The index must be within
// [first, first+n] and the caller holds the lock.
func (s *LogRing) pos(index uint64) int {
	return (s.head + int(index-s.first)) % len(s.buf)
}
