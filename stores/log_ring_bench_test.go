package stores

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

var benchStores = []struct {
	name string
	new  func() raft.LogStore
}{
	{name: "LogRing", new: func() raft.LogStore { return NewLogRing() }},
	{name: "InmemStore", new: func() raft.LogStore { return raft.NewInmemStore() }},
}

// BenchmarkDeleteRangeHead is acceptance criterion 2: the cost of compaction must
// not scale with the length of the truncated range. Expect flat ns/op for the
// ring and linear growth for the map store.
func BenchmarkDeleteRangeHead(b *testing.B) {
	for _, entries := range []uint64{10_000, 100_000, 1_000_000} {
		for _, tc := range benchStores {
			b.Run(tc.name+"/"+strconv.FormatUint(entries, 10), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					store := tc.new()
					for from := uint64(1); from <= entries; from += 1000 {
						storeRange(b, store, from, from+999)
					}
					b.StartTimer()

					if err := store.DeleteRange(1, entries); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkGetLogDuringCompaction is the same criterion from the reader's side:
// a replication read must not wait out a compaction. A background goroutine keeps
// a fixed window by appending and then truncating the head, exactly as raft does
// under sustained load. The number that matters is worst-ns, the longest single
// read: a truncation that blocks readers shows up there, not in the mean.
func BenchmarkGetLogDuringCompaction(b *testing.B) {
	const (
		window = 200_000
		batch  = 50_000
	)

	for _, tc := range benchStores {
		b.Run(tc.name, func(b *testing.B) {
			store := tc.new()
			for from := uint64(1); from <= window; from += 1000 {
				storeRange(b, store, from, from+999)
			}

			done := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()

				next := uint64(window + 1)
				for {
					select {
					case <-done:
						return
					default:
					}
					for from := next; from < next+batch; from += 1000 {
						storeRange(b, store, from, from+999)
					}
					next += batch

					first, _ := store.FirstIndex()
					if err := store.DeleteRange(first, first+batch-1); err != nil {
						b.Error(err)
						return
					}
				}
			}()

			var worst time.Duration

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				first, _ := store.FirstIndex()
				var log raft.Log

				start := time.Now()
				_ = store.GetLog(first, &log) //nolint:errcheck // a moving window makes ErrLogNotFound expected
				if took := time.Since(start); took > worst {
					worst = took
				}
			}
			b.StopTimer()

			close(done)
			wg.Wait()

			b.ReportMetric(float64(worst.Nanoseconds()), "worst-ns")
		})
	}
}
