package quasar

import (
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
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

	raftConfig         *raft.Config
	suffrage           raft.ServerSuffrage
	bootstrap          bool
	servers            []raft.Server
	discovery          Discovery
	pruneAfter         time.Duration
	recoverQuorumAfter time.Duration
	bootstrapWait      time.Duration
	noLeaderTimeout    time.Duration
	hclogLogger        hclog.Logger
	slogLogger         *slog.Logger

	kv     stores.KVStore
	pStore stores.PersistentStorage
}

func getOptions(opts []Option) options {
	cfg := options{
		cacheName: "quasar",
		localID:   uuid.NewString(),
		suffrage:  raft.Voter,
		noLeaderTimeout: defaultNoLeaderTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func (o *options) getLogger() hclog.Logger {
	if o.hclogLogger != nil {
		return o.hclogLogger
	}
	if o.slogLogger != nil {
		return &slogAdapter{logger: o.slogLogger}
	}

	return hclog.New(&hclog.LoggerOptions{
		Name:   o.cacheName,
		Level:  hclog.NoLevel,
		Output: os.Stdout,
	})
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

// WithAutoPrune removes peers from the raft cluster after they have not been
// observed through discovery pings for the given duration. If not set, peers
// are never auto-pruned. The minimum value is 6 seconds.
func WithAutoPrune(after time.Duration) Option {
	const minPruneAfter = 6 * time.Second

	return func(o *options) {
		if after < minPruneAfter {
			after = minPruneAfter
		}
		o.pruneAfter = after
	}
}

// WithQuorumRecovery enables automatic recovery of a stranded voter. When the
// local node is a voter, has been without a leader for at least `after`, and
// no other voter in the raft configuration has been observed by discovery
// within the same window, the cache will shut down its raft instance and
// force a new configuration that drops the missing voters and keeps only
// itself plus the still-live nonvoters. After the rebuild it elects itself
// as a single-voter leader; missing voters can rejoin later via discovery.
//
// This is intended for the "two-site" 2-voter deployment where losing one
// voter would otherwise leave the cluster permanently without quorum. If
// `after` is zero (default), recovery is disabled.
//
// WARNING: forced recovery is a controlled split-brain risk. If the missing
// voter is in fact alive on the other side of a transient partition, recent
// unreplicated writes from this side may be silently dropped when the
// partition heals. Pick `after` long enough that genuine link blips do not
// trigger. The minimum effective value is 6 seconds.
func WithQuorumRecovery(after time.Duration) Option {
	const minRecoverAfter = 6 * time.Second

	return func(o *options) {
		if after <= 0 {
			o.recoverQuorumAfter = 0
			return
		}

		if after < minRecoverAfter {
			after = minRecoverAfter
		}
		o.recoverQuorumAfter = after
	}
}

// WithBootstrapWait makes a voter wait up to `after` at startup for any
// peer to announce itself via discovery before bootstrapping a single-node
// cluster. If a peer pings within the window, bootstrap is skipped and the
// existing cluster's leader is expected to pull this node in (via AddVoter
// from the standard new-peer path, or via raft replication when this node
// is already in the leader's configuration). The wait aborts as soon as
// any peer is observed, so it only costs the full `after` in the cold-start
// case where there genuinely is nothing else out there yet.
//
// Only effective when a Discovery is configured. A zero value preserves the
// pre-RT-12775 behavior of unconditionally bootstrapping the local node at
// startup, which can race with an existing cluster and create competing
// single-node universes on restart.
func WithBootstrapWait(after time.Duration) Option {
	return func(o *options) {
		o.bootstrapWait = after
	}
}

// WithNoLeaderTimeout overrides how long applyRemote waits for a leader to be
// elected before returning ErrNoLeader. Also used as the final fallback for the
// discovery alive-window when neither WithAutoPrune nor WithQuorumRecovery is
// set, so the request-shedding path and the recovery captain stay aligned.
// A zero value keeps the built-in default.
func WithNoLeaderTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.noLeaderTimeout = timeout
	}
}

// WithRaftConfig allows passing in a custom raft configuration. Only the LocalID will
// still be overwritten which can be set WithLocalID.
func WithRaftConfig(cfg *raft.Config) Option {
	return func(o *options) {
		o.raftConfig = cfg
	}
}

// WithHclogLogger sets an hclog.Logger to be used for raft logging.
// Priority order: hclog > slog > zerolog.
func WithHclogLogger(logger hclog.Logger) Option {
	return func(o *options) {
		o.hclogLogger = logger
	}
}

// WithSlogLogger sets an slog.Logger to be used for raft logging.
// Priority order: hclog > slog > zerolog.
func WithSlogLogger(logger *slog.Logger) Option {
	return func(o *options) {
		o.slogLogger = logger
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
