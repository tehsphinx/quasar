package quasar

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/transports"
)

// newLeaderCache brings up a lone bootstrapped voter, so applyLocal is the
// path every write takes.
func newLeaderCache(ctx context.Context, t *testing.T, fsm FSM, opts ...Option) *Cache {
	t.Helper()

	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, fsm, append([]Option{
		WithLocalID("solo"),
		WithTransport(tr),
		WithBootstrap(true),
	}, opts...)...)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if !c.IsLeader() {
		t.Fatal("lone bootstrapped voter is not leader")
	}
	return c
}

// countingFSM records how many user commands actually reached the FSM.
type countingFSM struct {
	stubFSM

	applies atomic.Int32
}

func (s *countingFSM) ApplyCmd([]byte) error {
	s.applies.Add(1)
	return nil
}

// TestApplyLocalShedsStoresPastBound is the core RT-13906 assertion: past the
// bound a Store fails immediately with ErrOverloaded, distinguishably from
// raft.ErrEnqueueTimeout, and without proposing anything to raft.
func TestApplyLocalShedsStoresPastBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fsm := &countingFSM{}
	c := newLeaderCache(ctx, t, fsm, WithMaxInflightApplies(1))

	// Hold the only slot, standing in for a caller parked in raft.Apply.
	if !c.applySem.Acquire() {
		t.Fatal("could not take the first slot")
	}
	defer c.applySem.Release()

	start := time.Now()
	_, err := c.store(ctx, "key", []byte("value"))
	took := time.Since(start)

	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("expected ErrOverloaded, got %v", err)
	}
	if errors.Is(err, raft.ErrEnqueueTimeout) {
		t.Fatal("a shed must not be reported as a raft enqueue timeout")
	}
	// The point of the bound: microseconds, not applyTimeout. A whole second
	// of slack keeps this from flaking on a loaded CI runner while still
	// failing loudly if the call ever waits out the 5s apply timeout.
	if took > time.Second {
		t.Fatalf("shed took %v, expected it to return immediately", took)
	}
	if got := fsm.applies.Load(); got != 0 {
		t.Fatalf("a shed reached raft: FSM applied %d commands", got)
	}
}

// TestApplyLocalNeverShedsResetOrMembership covers the exemption: the reset,
// membership and liveness commands must survive an overloaded leader, because
// the startup path and the quorum probes depend on them.
func TestApplyLocalNeverShedsResetOrMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fsm := &stubFSM{}
	c := newLeaderCache(ctx, t, fsm, WithMaxInflightApplies(1))

	if !c.applySem.Acquire() {
		t.Fatal("could not take the only slot")
	}
	defer c.applySem.Release()

	if err := c.Reset(ctx); err != nil {
		t.Fatalf("Reset was refused by a shedding leader: %v", err)
	}
	if got := fsm.resets.Load(); got != 1 {
		t.Fatalf("expected the reset to reach the FSM once, got %d", got)
	}
	if _, err := c.masterLastIndex(ctx); err != nil {
		t.Fatalf("liveness probe was refused by a shedding leader: %v", err)
	}
}

// TestApplyLocalBelowBoundNeverSheds: a burst that stays under the bound is
// served normally, and the default (no option) stays unbounded.
func TestApplyLocalBelowBoundNeverSheds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		opts    []Option
		writers int
	}{
		{name: "under the bound", opts: []Option{WithMaxInflightApplies(8)}, writers: 8},
		{name: "unbounded by default", writers: 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			c := newLeaderCache(ctx, t, &stubFSM{}, tc.opts...)

			errCh := make(chan error, tc.writers)
			var wg sync.WaitGroup
			for i := 0; i < tc.writers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := c.store(ctx, "key", []byte("value"))
					errCh <- err
				}()
			}
			wg.Wait()
			close(errCh)

			for err := range errCh {
				if err != nil {
					t.Fatalf("write below the bound failed: %v", err)
				}
			}
		})
	}
}

// blockingFSM parks every ApplyCmd until release is closed, standing in for the
// stalled FSM goroutine of RT-13896.
type blockingFSM struct {
	stubFSM

	entered chan struct{}
	release chan struct{}
}

func newBlockingFSM() *blockingFSM {
	return &blockingFSM{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *blockingFSM) ApplyCmd([]byte) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}

func (s *blockingFSM) Snapshot() (raft.FSMSnapshot, error) { return nil, errors.New("no snapshots") }
func (s *blockingFSM) Restore(io.ReadCloser) error         { return nil }

// TestApplyLocalShedLatencyUnderStall is the acceptance criterion the outage
// produced: with the FSM stalled, callers past the bound must keep paying the
// shed latency instead of the apply timeout, however long the stall lasts.
func TestApplyLocalShedLatencyUnderStall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fsm := newBlockingFSM()
	c := newLeaderCache(ctx, t, fsm, WithMaxInflightApplies(1))
	defer close(fsm.release)

	// One writer occupies the only slot, parked inside the stalled FSM.
	go func() {
		_, _ = c.store(ctx, "key", []byte("parked"))
	}()
	select {
	case <-fsm.entered:
	case <-ctx.Done():
		t.Fatal("the first write never reached the FSM")
	}

	const writers = 20
	start := time.Now()
	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.store(ctx, "key", []byte("shed"))
			errCh <- err
		}()
	}
	wg.Wait()
	took := time.Since(start)
	close(errCh)

	for err := range errCh {
		if !errors.Is(err, ErrOverloaded) {
			t.Fatalf("expected every write past the bound to be shed, got %v", err)
		}
	}
	// Well under applyTimeout: without the bound each of these would have
	// waited out the 5s enqueue timeout instead.
	if took > time.Second {
		t.Fatalf("%d shed writes took %v; expected them to fail immediately", writers, took)
	}
}

// shedItem is a PersistedItem that records how the consumer settled it.
type shedItem struct {
	transports.PersistedItem

	retry     bool
	nacked    atomic.Int32
	replyErr  error
	replyErrs atomic.Int32
}

func (s *shedItem) Command() *pb.Store          { return &pb.Store{Key: "key", Data: []byte("v")} }
func (s *shedItem) Retry() bool                 { return s.retry }
func (s *shedItem) Deadline() (time.Time, bool) { return time.Time{}, false }
func (s *shedItem) Nack(context.Context) error  { s.nacked.Add(1); return nil }
func (s *shedItem) NackWithDelay(ctx context.Context) error {
	return s.Nack(ctx)
}

func (s *shedItem) ReplyError(_ context.Context, err error) error {
	s.replyErr = err
	s.replyErrs.Add(1)
	return nil
}

// TestApplyPersistedItemDefersShedRetryWrite: on the persisted-FIFO path a shed
// must not terminate a write JetStream is still holding for us — that would
// turn a load shed into a dropped write.
func TestApplyPersistedItemDefersShedRetryWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c := newLeaderCache(ctx, t, &stubFSM{}, WithMaxInflightApplies(1))
	if !c.applySem.Acquire() {
		t.Fatal("could not take the only slot")
	}
	defer c.applySem.Release()

	retryItem := &shedItem{retry: true}
	c.applyPersistedItem(ctx, retryItem)
	if got := retryItem.nacked.Load(); got != 1 {
		t.Fatalf("expected the shed retry item to be nacked once, got %d", got)
	}
	if got := retryItem.replyErrs.Load(); got != 0 {
		t.Fatalf("a shed retry item must not be answered with a terminal error, got %d replies", got)
	}

	plainItem := &shedItem{}
	c.applyPersistedItem(ctx, plainItem)
	if got := plainItem.nacked.Load(); got != 0 {
		t.Fatalf("a non-retry item must not be requeued, got %d nacks", got)
	}
	if !errors.Is(plainItem.replyErr, ErrOverloaded) {
		t.Fatalf("expected the publisher to see ErrOverloaded, got %v", plainItem.replyErr)
	}
}
