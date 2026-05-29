package transports

import (
	"io"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
)

// NATSOption defines an option for NewNATSTransport creation.
type NATSOption func(cfg *natsOptions)

func getNATSOptions(opts []NATSOption) natsOptions {
	cfg := natsOptions{
		timeout:    defaultTimout,
		output:     os.Stderr,
		maxMsgSize: maxPkgSize,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.logger == nil {
		cfg.logger = hclog.New(&hclog.LoggerOptions{
			Name:   "quasar",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	return cfg
}

type natsOptions struct {
	output         io.Writer
	logger         hclog.Logger
	timeout        time.Duration
	maxMsgSize     int
	persistedQueue *natsPersistedQueueConfig
}

// WithNATSTimeout adds a timout for nats requests. Defaults to 5s. Set to 0 to disable the timeout.
func WithNATSTimeout(timeout time.Duration) NATSOption {
	return func(cfg *natsOptions) {
		cfg.timeout = timeout
	}
}

// WithNATSLogger sets the logger. If this is used WithNATSLogOutput will not be considered.
func WithNATSLogger(logger hclog.Logger) NATSOption {
	return func(cfg *natsOptions) {
		cfg.logger = logger
	}
}

// WithNATSLogOutput sets the output writer for logging.
func WithNATSLogOutput(output io.Writer) NATSOption {
	return func(cfg *natsOptions) {
		cfg.output = output
	}
}

func WithNATSMaxMsgSize(size int) NATSOption {
	return func(cfg *natsOptions) {
		cfg.maxMsgSize = size
	}
}

// PersistedQueueOption tunes the persisted-FIFO stream / consumer
// created by WithNATSPersistedQueue. All fields default to safe values
// (see defaultPersistedAckWait / defaultPersistedMaxDeliver).
type PersistedQueueOption func(*natsPersistedQueueConfig)

// WithPersistedAckWait overrides the AckWait on the persisted-FIFO
// consumer. Defaults to 10s — see comment on defaultPersistedAckWait.
func WithPersistedAckWait(d time.Duration) PersistedQueueOption {
	return func(cfg *natsPersistedQueueConfig) {
		cfg.ackWait = d
	}
}

// WithPersistedMaxDeliver overrides MaxDeliver on the persisted-FIFO
// consumer. Defaults to 4 (AckWait × MaxDeliver ≈ 40s redelivery
// horizon).
func WithPersistedMaxDeliver(n int) PersistedQueueOption {
	return func(cfg *natsPersistedQueueConfig) {
		cfg.maxDeliver = n
	}
}

// WithPersistedMaxAge overrides MaxAge on the persisted-FIFO
// stream. Defaults to 1 hour.
func WithPersistedMaxAge(d time.Duration) PersistedQueueOption {
	return func(cfg *natsPersistedQueueConfig) {
		cfg.maxAge = d
	}
}

// WithPersistedReplicas overrides replicas on the persisted-FIFO
// stream. Defaults to 1.
func WithPersistedReplicas(n int) PersistedQueueOption {
	return func(cfg *natsPersistedQueueConfig) {
		cfg.replicas = n
	}
}

// WithPersistedStreamManaged controls whether this node creates and updates
// the JetStream stream backing the persisted-FIFO queue. When false, the node
// only binds to an already-existing stream and never creates or mutates it —
// use this for nonvoters, which can never become leader and must not race the
// voter that owns the stream's configuration (e.g. replicas). Defaults to true
// (backwards-compatible: every node manages the stream).
func WithPersistedStreamManaged(managed bool) PersistedQueueOption {
	return func(cfg *natsPersistedQueueConfig) {
		cfg.manageStream = managed
	}
}

// WithNATSPersistedQueue enables persisted-FIFO mode on the NATS
// transport. streamName names the JetStream WorkQueuePolicy stream
// that backs the queue; pass an empty string to use the default
// `quasar.<cacheName>.queue`.
//
// When enabled, every cache write (leader's own writes included)
// publishes a Store command into the stream and waits for the leader's
// reply via a NATS request-reply inbox. The leader claims the pull
// consumer with MaxAckPending = 1, so writes are applied in strict
// FIFO order. A missing leader is no longer a write blocker — the
// publish lands in the stream and the next leader applies it.
//
// Use only with NATS connections backed by a JetStream-enabled server.
func WithNATSPersistedQueue(streamName string, opts ...PersistedQueueOption) NATSOption {
	return func(cfg *natsOptions) {
		qc := natsPersistedQueueConfig{
			streamName:   streamName,
			ackWait:      defaultPersistedAckWait,
			maxDeliver:   defaultPersistedMaxDeliver,
			maxAge:       defaultPersistedMaxAge,
			manageStream: true,
		}
		for _, o := range opts {
			o(&qc)
		}
		cfg.persistedQueue = &qc
	}
}
