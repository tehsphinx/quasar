package transports

import (
	"time"
)

// DRPCOption defines an option for NewTCPTransport creation.
type DRPCOption func(cfg *drpcOptions)

func getDRPCOptions(opts []DRPCOption) drpcOptions {
	cfg := drpcOptions{
		timeout: defaultTimout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

type drpcOptions struct {
	timeout time.Duration
}

// WithDRPCTimeout adds a timout for drpc requests. Defaults to 5s. Set to 0 to disable the timeout.
func WithDRPCTimeout(timeout time.Duration) DRPCOption {
	return func(cfg *drpcOptions) {
		cfg.timeout = timeout
	}
}
