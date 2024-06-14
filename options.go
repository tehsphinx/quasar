package quasar

import (
	"net"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

type Option func(*options)

type options struct {
	localID string

	bindAddr      string
	extAddr       net.Addr
	raftTransport raft.Transport
	transport     transports.Transport

	raft      *raft.Raft
	bootstrap bool
	servers   []raft.Server

	kv stores.KVStore
}

func getOptions(opts []Option) options {
	cfg := options{
		localID:  uuid.NewString(),
		bindAddr: ":28224",
		extAddr: &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 28224,
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.kv == nil {
		cfg.kv = stores.NewInMemKVStore()
	}
	return cfg
}

// WithLocalID sets the id of this server. If not set a random UUID is used
// which will reset the RAFT status of this server every time.
func WithLocalID(id string) Option {
	return func(o *options) {
		o.localID = id
	}
}

// WithKVStore sets the kv store to use. If not set a new in memory kv store
// is created with stores.NewInMemKVStore and used.
func WithKVStore(kv stores.KVStore) Option {
	return func(o *options) {
		o.kv = kv
	}
}

// WithTCPTransport provides a simplified way to use tcp based RAFT communication.
// See WithCustomRaft for a completely customizable option.
func WithTCPTransport(bindAddr string, extAddr net.Addr) Option {
	return func(o *options) {
		o.bindAddr = bindAddr
		o.extAddr = extAddr
	}
}

// WithTransport provides a way to set a custom transport. Using this option
// overrides usage of WithTCPTransport.
func WithTransport(transport transports.Transport) Option {
	return func(o *options) {
		o.transport = transport
	}
}

// WithRaftTransport provides a way to set a custom raft transport only.
// The raftPort of WithTCPTransport is ignored if this is provided.
func WithRaftTransport(transport raft.Transport) Option {
	return func(o *options) {
		o.raftTransport = transport
	}
}

// WithBootstrap bootstraps the server just with itself starting a one node
// cluster of the cache.
func WithBootstrap(bootstrap bool) Option {
	return func(o *options) {
		o.bootstrap = bootstrap
	}
}

// WithServers can be used to bootstrap the cluster with multiple nodes.
// See WithBootstrap for starting with a single node.
func WithServers(servers []raft.Server) Option {
	return func(o *options) {
		o.servers = servers
	}
}

// WithCustomRaft allows a completely custom configured RAFT instance to be passed in.
// Using this option disables usage of `WithLocalID`, and `WithTCPTransport`
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
