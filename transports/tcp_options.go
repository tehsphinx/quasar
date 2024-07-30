package transports

import (
	"io"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// TCPOption defines an option for NewTCPTransport creation.
type TCPOption func(cfg *tcpOptions)

func getTCPOptions(opts []TCPOption) tcpOptions {
	const defaultMaxPool = 3

	cfg := tcpOptions{
		maxPool: defaultMaxPool,
		timeout: defaultTimout,
		output:  os.Stderr,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

//nolint:govet // Usually initialized once. Preferring readability to struct optimization here.
type tcpOptions struct {
	maxPool int
	timeout time.Duration
	config  *raft.NetworkTransportConfig

	output io.Writer
	logger hclog.Logger
}

// WithTCPMaxPool limits the max amount of connections in the TCP connection pool.
// If the connection pool is full, unused connections are released. Setting defaults to 3.
func WithTCPMaxPool(maxPool int) TCPOption {
	return func(cfg *tcpOptions) {
		cfg.maxPool = maxPool
	}
}

// WithTCPTimeout adds a timout for tcp requests. Defaults to 5s. Set to 0 to disable the timeout.
func WithTCPTimeout(timeout time.Duration) TCPOption {
	return func(cfg *tcpOptions) {
		cfg.timeout = timeout
	}
}

// WithTCPLogger sets the logger. If this is used WithTCPLogOutput will not be considered.
func WithTCPLogger(logger hclog.Logger) TCPOption {
	return func(cfg *tcpOptions) {
		cfg.logger = logger
	}
}

// WithTCPLogOutput sets the output writer for logging.
func WithTCPLogOutput(output io.Writer) TCPOption {
	return func(cfg *tcpOptions) {
		cfg.output = output
	}
}

// WithTCPConfig uses given configuration to create the transport.
// Other TCPOption parameters are ignored:
// WithTCPMaxPool, WithTCPTimeout, WithTCPLogger, and WithTCPLogOutput.
func WithTCPConfig(config *raft.NetworkTransportConfig) TCPOption {
	return func(cfg *tcpOptions) {
		cfg.config = config
	}
}
