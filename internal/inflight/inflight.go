// Package inflight provides the semaphore that bounds how much concurrent work
// one part of the cache admits before it starts shedding.
package inflight

import (
	"sync/atomic"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
)

// Sem bounds how many operations run at the same time and sheds the surplus
// instead of queueing it.
//
// Overload is reported as a state transition, not per request: the incident
// this pattern replaces produced 152k dropped-message log lines on one leader,
// and logging every shed request would reproduce that symptom in a new place.
// Per-request visibility comes from go-metrics instead (RT-13902): every shed
// increments a counter and every admit/release updates an in-flight gauge,
// through the same process-global sink raft and tcp_transport.go emit into.
//
// A Sem built with a non-positive size is unbounded: it admits everything,
// emits nothing and never logs. That is the default for the apply path, so an
// embedder that does not opt into a bound keeps the behaviour it had before
// the bound existed (RT-13906).
type Sem struct {
	// logKey / name identify this semaphore in its log lines and label its
	// metrics; msgShed / msgClear are the two state-transition messages.
	logKey   string
	name     string
	msgShed  string
	msgClear string

	labels  []metrics.Label
	keyIn   []string
	keyShed []string

	slots  chan struct{}
	logger hclog.Logger

	shedding atomic.Bool
	shed     atomic.Uint64
}

// NewRPC bounds how many RPCs one subscription serves at the same time.
// A subscription callback that blocks until its RPC is answered serves that
// subject strictly one at a time — nats.go dispatches each async subscription
// from a single waitForMsgs goroutine — so the transport needs its own bound
// once the callback stops blocking (RT-13899).
func NewRPC(logger hclog.Logger, subject string, size int) *Sem {
	return &Sem{
		logKey:   "subject",
		name:     subject,
		msgShed:  "overloaded, shedding cache RPCs",
		msgClear: "cache RPC overload cleared",
		labels:   []metrics.Label{{Name: "subject", Value: subject}},
		keyIn:    []string{"quasar", "cache", "rpc", "inflight"},
		keyShed:  []string{"quasar", "cache", "rpc", "shed"},
		slots:    newSlots(size),
		logger:   logger,
	}
}

// NewApply bounds how many raft applies the leader has in flight at the same
// time. Without it a stalled FSM parks every caller in raft.Apply for the full
// apply timeout, and each of those hangs buys a retry that parks another
// caller — the congestion collapse that turned a raft wedge into a 3h47m
// cluster-wide write outage (RT-13906).
func NewApply(logger hclog.Logger, cache string, size int) *Sem {
	return &Sem{
		logKey:   "cache",
		name:     cache,
		msgShed:  "overloaded, shedding cache applies",
		msgClear: "cache apply overload cleared",
		labels:   []metrics.Label{{Name: "cache", Value: cache}},
		keyIn:    []string{"quasar", "cache", "apply", "inflight"},
		keyShed:  []string{"quasar", "cache", "apply", "shed"},
		slots:    newSlots(size),
		logger:   logger,
	}
}

// newSlots returns nil for a non-positive size, which is how a Sem is made
// unbounded.
func newSlots(size int) chan struct{} {
	if size <= 0 {
		return nil
	}
	return make(chan struct{}, size)
}

// Acquire takes a slot without waiting. It returns false when the bound is
// reached; the caller must then shed the work rather than queue it, since
// queueing is what fills a buffer somewhere until it silently drops — the
// NATS client's per-subscription buffer for RPCs, the leader's own goroutines
// and retry budget for applies.
func (s *Sem) Acquire() bool {
	if s.slots == nil {
		return true
	}

	select {
	case s.slots <- struct{}{}:
		metrics.SetGaugeWithLabels(s.keyIn, float32(len(s.slots)), s.labels)
		if s.shedding.CompareAndSwap(true, false) {
			s.logger.Warn(s.msgClear, s.logKey, s.name, "shed", s.shed.Swap(0))
		}
		return true
	default:
		metrics.IncrCounterWithLabels(s.keyShed, 1, s.labels)
		s.shed.Add(1)
		if s.shedding.CompareAndSwap(false, true) {
			s.logger.Error(s.msgShed, s.logKey, s.name, "in_flight", cap(s.slots))
		}
		return false
	}
}

// Release returns a slot taken by Acquire.
func (s *Sem) Release() {
	if s.slots == nil {
		return
	}

	<-s.slots
	metrics.SetGaugeWithLabels(s.keyIn, float32(len(s.slots)), s.labels)
}
