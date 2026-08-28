package inflight_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/tehsphinx/quasar/internal/inflight"
)

// syncLog collects log output for the state-transition assertions.
type syncLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (s *syncLog) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncLog) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func newLogger(out *syncLog) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Warn})
}

func TestSemUnboundedAdmitsEverything(t *testing.T) {
	out := &syncLog{}
	sem := inflight.NewApply(newLogger(out), "test", 0)

	for i := 0; i < 1000; i++ {
		if !sem.Acquire() {
			t.Fatalf("unbounded sem shed at acquire %d", i)
		}
	}
	// Release must be safe even though nothing was tracked.
	sem.Release()

	if got := out.String(); got != "" {
		t.Fatalf("unbounded sem logged: %s", got)
	}
}

func TestSemShedsPastBoundAndReadmitsOnRelease(t *testing.T) {
	out := &syncLog{}
	sem := inflight.NewApply(newLogger(out), "test", 2)

	if !sem.Acquire() || !sem.Acquire() {
		t.Fatal("sem shed below its bound")
	}
	if sem.Acquire() {
		t.Fatal("sem admitted past its bound")
	}

	sem.Release()
	if !sem.Acquire() {
		t.Fatal("sem did not re-admit after a release")
	}
}

// TestSemLogsOncePerTransition is the RT-13899 constraint carried over to the
// apply path: the log volume must not scale with the number of shed requests.
func TestSemLogsOncePerTransition(t *testing.T) {
	out := &syncLog{}
	sem := inflight.NewApply(newLogger(out), "test", 1)

	if !sem.Acquire() {
		t.Fatal("sem shed below its bound")
	}
	for i := 0; i < 100; i++ {
		if sem.Acquire() {
			t.Fatal("sem admitted past its bound")
		}
	}
	sem.Release()
	if !sem.Acquire() {
		t.Fatal("sem did not re-admit after a release")
	}

	log := out.String()
	if n := strings.Count(log, "overloaded, shedding cache applies"); n != 1 {
		t.Fatalf("expected 1 shedding log for 100 shed acquires, got %d:\n%s", n, log)
	}
	if n := strings.Count(log, "cache apply overload cleared"); n != 1 {
		t.Fatalf("expected 1 cleared log, got %d:\n%s", n, log)
	}
	if !strings.Contains(log, "shed=100") {
		t.Fatalf("expected the cleared log to carry the shed count, got:\n%s", log)
	}
}

// TestNewRPCKeepsItsOwnWording guards the RT-13902 dashboards: moving the
// semaphore out of transports must not rename what the RPC side emits.
func TestNewRPCKeepsItsOwnWording(t *testing.T) {
	out := &syncLog{}
	sem := inflight.NewRPC(newLogger(out), "quasar.c.n.cache.store", 1)

	if !sem.Acquire() {
		t.Fatal("sem shed below its bound")
	}
	if sem.Acquire() {
		t.Fatal("sem admitted past its bound")
	}
	sem.Release()
	if !sem.Acquire() {
		t.Fatal("sem did not re-admit after a release")
	}

	log := out.String()
	for _, want := range []string{
		"overloaded, shedding cache RPCs",
		"cache RPC overload cleared",
		"subject=quasar.c.n.cache.store",
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("missing %q in RPC sem log:\n%s", want, log)
		}
	}
}
