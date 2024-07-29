package discoveries

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

const discoverySubj = "raft.%s.discovery.ping"

func NewNATSDiscovery(nc *nats.Conn) *NATSDiscovery {
	return &NATSDiscovery{
		nc: nc,
	}
}

var _ quasar.Discovery = (*NATSDiscovery)(nil)

type NATSDiscovery struct {
	nc    *nats.Conn
	cache *quasar.DiscoveryInjector

	subj       string
	serverInfo raft.Server
}

func (s *NATSDiscovery) Inject(cache *quasar.DiscoveryInjector) {
	s.cache = cache
	s.subj = fmt.Sprintf(discoverySubj, cache.Name())
	s.serverInfo = cache.ServerInfo()
}

func (s *NATSDiscovery) Run(ctx context.Context) error {
	discoverySub, err := s.nc.Subscribe(s.subj, s.discoveryHandler)
	if err != nil {
		return err
	}

	if r := s.ping(); r != nil {
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
	interval := minInterval + time.Duration(rand.Int63n(int64(maxInterval-minInterval)))

	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			//nolint:staticcheck // empty branch to be filled later
			if r := s.ping(); r != nil {
				// TODO: log?
			}
		case <-ctx.Done():
			return
		}
	}
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

	s.cache.ProcessServer(sender)

	if ping.GetIsResponse() {
		// don't respond to responses
		return
	}

	bts, err := s.marshalPing(true)
	if err != nil {
		return
	}

	if msg.Reply != "" {
		if e := msg.Respond(bts); e != nil {
			return
		}
		return
	}
	if r := s.nc.Publish(s.subj, bts); r != nil {
		return
	}
}

func (s *NATSDiscovery) marshalPing(isResponse bool) ([]byte, error) {
	return proto.Marshal(&pb.DiscoverPing{
		Sender:     pb.ToServerInfo(s.serverInfo),
		IsResponse: isResponse,
	})
}
