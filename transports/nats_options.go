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
		timeout: defaultTimout,
		output:  os.Stderr,
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
	output  io.Writer
	logger  hclog.Logger
	timeout time.Duration
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
