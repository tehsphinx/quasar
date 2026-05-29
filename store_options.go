package quasar

// StoreOption configures a single Store call. Options affect how the
// quasar cache treats the call when the underlying transport supports
// the persisted-FIFO mode; they are no-ops on transports that do not
// support persisted-FIFO.
type StoreOption func(*storeOpts)

// storeOpts is the resolved option set carried through the apply path.
type storeOpts struct {
	// retry asks the cache to treat a reply-timeout on the persisted
	// path as "queued, JetStream will keep redelivering" rather than
	// "failed". When true and the reply doesn't arrive within the
	// caller's timeout, routePersisted returns ErrRetrying
	// wrapping the underlying cause. When false, the underlying
	// error is returned unwrapped (today's behaviour: ErrNoLeader
	// or whatever the transport returned).
	//
	// Callers that set WithRetry() are accepting at-least-once
	// semantics — the payload must be idempotent because a crashed
	// leader may have applied the command before the reply / ack
	// could be sent, and JetStream will redeliver to the next leader.
	retry bool
}

func resolveStoreOpts(opts []StoreOption) storeOpts {
	var o storeOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithRetry asks the cache to treat a reply-timeout on the persisted
// transport as a queued-for-redelivery success rather than an outright
// failure. Only meaningful when the underlying transport reports
// SupportsPersisted() == true; ignored otherwise.
//
// When the persisted publish succeeds but the leader's reply does not
// arrive before the caller's context deadline, the cache returns
// ErrRetrying (wrapping the deadline error) instead of the
// underlying transport / leader error. JetStream-level redelivery
// (MaxDeliver × AckWait) continues independently until either a
// later leader applies the message or the redelivery budget runs out.
//
// The payload must be idempotent: a leader can crash after applying
// the raft command but before publishing the reply / acking the JS
// message, in which case redelivery will apply the same command on
// the next leader. Callers that cannot tolerate at-least-once
// semantics should not pass WithRetry().
func WithRetry() StoreOption {
	return func(o *storeOpts) {
		o.retry = true
	}
}
