package quasar

import (
	"net"
	"strconv"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

// Option defines a functional option to be applied to a cache instantiation.
type Option func(*options)

//nolint:govet // struct optimization not worth it. Is not created often. Optimized for readability.
type options struct {
	cacheName string
	localID   string

	bindAddr  string
	extAddr   net.Addr
	nc        *nats.Conn
	transport transports.Transport

	raftConfig *raft.Config
	suffrage   raft.ServerSuffrage
	bootstrap  bool
	servers    []raft.Server
	discovery  Discovery

	kv     stores.KVStore
	pStore stores.PersistentStorage
}

func getOptions(opts []Option) options {
	cfg := options{
		cacheName: "default",
		localID:   uuid.NewString(),
		suffrage:  raft.Voter,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// WithName sets the name of the cache. This can be important for distinguishing
// traffic of multiple caches in the same network, but might not be needed for all
// transports or discoveries.
func WithName(name string) Option {
	return func(o *options) {
		o.cacheName = name
	}
}

// WithLocalID sets the id of this server. If not set a random UUID is used
// which will reset the RAFT status of this server every time.
func WithLocalID(id string) Option {
	return func(o *options) {
		o.localID = id
	}
}

// WithKVStore sets the kv store to use. This is only applicable for the KVCache.
// If not set a new in memory kv store is created with stores.NewInMemKVStore and used.
func WithKVStore(kv stores.KVStore) Option {
	return func(o *options) {
		o.kv = kv
	}
}

// WithPersistentStore sets the persistent store to use. This is only applicable for the Cache.
// If not set no persistence will take place.
func WithPersistentStore(store stores.PersistentStorage) Option {
	return func(o *options) {
		o.pStore = store
	}
}

// WithTransport provides a way to set a custom transport. Using this option
// ignores usage of WithTCPTransport and WithNatsTransport.
func WithTransport(transport transports.Transport) Option {
	return func(o *options) {
		o.transport = transport
	}
}

// WithNatsTransport provides a simplified way to use NATS based RAFT communication.
// Using this option ignores usage of WithTCPTransport.
func WithNatsTransport(nc *nats.Conn) Option {
	return func(o *options) {
		o.nc = nc
	}
}

// WithTCPTransport provides a simplified way to use tcp based RAFT communication.
func WithTCPTransport(bindAddr string, extAddr net.Addr) Option {
	const defaultPort = 28224

	if bindAddr == "" {
		bindAddr = ":" + strconv.Itoa(defaultPort)
	}
	if extAddr == nil {
		extAddr = &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: defaultPort,
		}
	}
	return func(o *options) {
		o.bindAddr = bindAddr
		o.extAddr = extAddr
	}
}

// WithSuffrage can be used to configure a cache instance to be a raft.Nonvoter.
// If not set, it defaults to being a raft.Voter.
func WithSuffrage(suffrage raft.ServerSuffrage) Option {
	return func(o *options) {
		o.suffrage = suffrage
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

// WithDiscovery can be used to pass in a server discovery. It's the discoveries
// job to find existing services, add new ones and potentially remove lost ones.
func WithDiscovery(discovery Discovery) Option {
	return func(opt *options) {
		opt.discovery = discovery
	}
}

// WithRaftConfig allows passing in a custom raft configuration. Only the LocalID will
// still be overwritten which can be set WithLocalID.
func WithRaftConfig(cfg *raft.Config) Option {
	return func(o *options) {
		o.raftConfig = cfg
	}
}

// LoadOption defines functional option to be applied to load functions.
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

// WaitForUID adds a raft uid to be applied before reading a value.
func WaitForUID(uid uint64) LoadOption {
	return func(opt *loadOptions) {
		opt.waitFor = uid
	}
}
