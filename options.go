package quasar

import (
	"github.com/hashicorp/raft"
)

type Option func(*options)

type options struct {
	tcpPort int
	raft    *raft.Raft
}

func getOptions(opts []Option) options {
	cfg := options{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func WithTCPRaft(port int) Option {
	return func(o *options) {
		o.tcpPort = port
	}
}

func WithCustomRaft(rft *raft.Raft) Option {
	return func(o *options) {
		o.raft = rft
	}
}

type LoadOption func(*loadOptions)

func getLoadOptions(opts []LoadOption) loadOptions {
	var cfg loadOptions
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

type loadOptions struct {
	waitFor uint64
}

func WaitForUID(uid uint64) LoadOption {
	return func(opt *loadOptions) {
		opt.waitFor = uid
	}
}
