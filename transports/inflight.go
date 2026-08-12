package transports

import (
	"sync/atomic"

	"github.com/hashicorp/go-hclog"
)

// inflightSem bounds how many RPCs one subscription serves at the same time.
// A subscription callback that blocks until its RPC is answered serves that
// subject strictly one at a time — nats.go dispatches each async subscription
// from a single waitForMsgs goroutine — so the transport needs its own bound
// once the callback stops blocking (RT-13899).
//
// Overload is reported as a state transition, not per request: the incident
// this replaces produced 152k dropped-message log lines on one leader, and
// logging every shed request would reproduce that symptom in a new place.
type inflightSem struct {
	subject string
	slots   chan struct{}
	logger  hclog.Logger

	shedding atomic.Bool
	shed     atomic.Uint64
}

func newInflightSem(logger hclog.Logger, subject string, size int) *inflightSem {
	return &inflightSem{
		subject: subject,
		slots:   make(chan struct{}, size),
		logger:  logger,
	}
}

// acquire takes a slot without waiting. It returns false when the subject is
// at its in-flight bound; the caller must then shed the request rather than
// queue it, since queueing is what fills the NATS client's per-subscription
// buffer until it silently drops.
func (s *inflightSem) acquire() bool {
	select {
	case s.slots <- struct{}{}:
		if s.shedding.CompareAndSwap(true, false) {
			s.logger.Warn("cache RPC overload cleared",
				"subject", s.subject, "shed", s.shed.Swap(0))
		}
		return true
	default:
		s.shed.Add(1)
		if s.shedding.CompareAndSwap(false, true) {
			s.logger.Error("overloaded, shedding cache RPCs",
				"subject", s.subject, "in_flight", cap(s.slots))
		}
		return false
	}
}

func (s *inflightSem) release() {
	<-s.slots
}
