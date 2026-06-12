package discoveries

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

const discoverySubj = "raft.%s.discovery.ping"

// NewNATSDiscovery creates a new NATSDiscovery.
func NewNATSDiscovery(nc *nats.Conn) *NATSDiscovery {
	return &NATSDiscovery{
		nc: nc,
	}
}

var _ quasar.Discovery = (*NATSDiscovery)(nil)

// NATSDiscovery is a type that represents a discovery mechanism using NATS messaging system.
// It implements the quasar.Discovery interface.
type NATSDiscovery struct {
	nc    *nats.Conn
	cache *quasar.DiscoveryInjector

	subj       string
	serverInfo raft.Server
}

// Inject is used by the cache to inject access to internal functionality into the discovery.
// Implements quasar.Discovery.
func (s *NATSDiscovery) Inject(cache *quasar.DiscoveryInjector) {
	s.cache = cache
	s.subj = fmt.Sprintf(discoverySubj, cache.Name())
	s.serverInfo = cache.ServerInfo()
}

// Run starts the NATSDiscovery service. This is a non-blocking call.
// Implements quasar.Discovery.
func (s *NATSDiscovery) Run(ctx context.Context) error {
	discoverySub, err := s.nc.Subscribe(s.subj, s.discoveryHandler)
	if err != nil {
		return err
	}

	if r := s.ping(); r != nil {
		// Clean up the subscription before bailing out — previously an early
		// ping failure returned without ever installing the unsubscribe path,
		// leaking the subscription (m18).
		_ = discoverySub.Unsubscribe()
		return r
	}

	go s.runPinging(ctx)

	go func() {
		<-ctx.Done()
		_ = discoverySub.Unsubscribe()
	}()
	return nil
}

func (s *NATSDiscovery) runPinging(ctx context.Context) {
	const minInterval = 2 * time.Second
	const maxInterval = 5 * time.Second

	// pseudo random interval
	//nolint:gosec // math/rand is fine here
	interval := minInterval + time.Duration(rand.Int63n(int64(maxInterval-minInterval)))

	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			if r := s.ping(); r != nil {
				s.logger().Warn("discovery ping failed", "error", r)
			}
		case <-ctx.Done():
			return
		}
	}
}

// logger returns the cache's logger once Inject ran, and a null logger
// before that so early calls cannot panic.
func (s *NATSDiscovery) logger() hclog.Logger {
	if s.cache == nil {
		return hclog.NewNullLogger()
	}
	return s.cache.Logger()
}

func (s *NATSDiscovery) ping() error {
	bts, err := s.marshalPing(false)
	if err != nil {
		return err
	}

	return s.nc.Publish(s.subj, bts)
}

func (s *NATSDiscovery) discoveryHandler(msg *nats.Msg) {
	var ping pb.DiscoverPing
	if err := proto.Unmarshal(msg.Data, &ping); err != nil {
		return
	}

	sender := ping.GetSender().Convert()
	if sender.ID == s.serverInfo.ID {
		// ping sent by myself
		return
	}

	// Process the ping off the dispatcher goroutine. ProcessServerWithStatus
	// can block on raft operations (AddVoter, hard-reset resend) for a new
	// peer, and NATS delivers subscription callbacks serially — running it
	// inline would head-of-line block every subsequent ping to this node,
	// making it look dead to peers' prune sweeps (m17). setServer makes the
	// new-peer decision atomically, so concurrent processing is safe.
	go s.cache.ProcessServerWithStatus(sender, peerStatusFromPB(ping.GetRaftStatus()))

	if ping.GetIsResponse() {
		// don't respond to responses
		return
	}

	bts, err := s.marshalPing(true)
	if err != nil {
		s.logger().Warn("discovery ping response: marshal failed", "error", err)
		return
	}

	// Pings are plain publishes (see ping / runPinging), never request-reply,
	// so msg.Reply is always empty here — the former msg.Respond branch was
	// dead (RT-13042 S8). Responses go back out on the shared subject.
	if r := s.nc.Publish(s.subj, bts); r != nil {
		s.logger().Warn("discovery ping response failed", "error", r)
		return
	}
}

func (s *NATSDiscovery) marshalPing(isResponse bool) ([]byte, error) {
	return proto.Marshal(&pb.DiscoverPing{
		Sender:     pb.ToServerInfo(s.serverInfo),
		IsResponse: isResponse,
		RaftStatus: peerStatusToPB(s.cache.LocalPeerStatus()),
	})
}

func peerStatusToPB(status quasar.PeerStatus) *pb.RaftStatus {
	return &pb.RaftStatus{
		HasLeader:         status.HasLeader,
		NumVotersInConfig: status.NumVotersInConfig,
		InstanceId:        status.InstanceID,
	}
}

func peerStatusFromPB(pbStatus *pb.RaftStatus) quasar.PeerStatus {
	if pbStatus == nil {
		return quasar.PeerStatus{}
	}
	return quasar.PeerStatus{
		HasLeader:         pbStatus.GetHasLeader(),
		NumVotersInConfig: pbStatus.GetNumVotersInConfig(),
		InstanceID:        pbStatus.GetInstanceId(),
	}
}
