package discoveries

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/pb/v1"
	"google.golang.org/protobuf/proto"
)

const discoverySubj = "raft.%s.discovery.servers.get"

func NewNATSDiscovery(nc *nats.Conn) *NATSDiscovery {
	return &NATSDiscovery{
		nc: nc,
	}
}

var _ quasar.Discovery = (*NATSDiscovery)(nil)

type NATSDiscovery struct {
	nc    *nats.Conn
	cache *quasar.DiscoveryInjector
	subj  string
}

func (s *NATSDiscovery) Inject(cache *quasar.DiscoveryInjector) {
	s.cache = cache
	s.subj = fmt.Sprintf(discoverySubj, cache.Name())
}

func (s *NATSDiscovery) Run(ctx context.Context) error {
	discoverySub, err := s.nc.Subscribe(s.subj, s.discoveryHandler)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = discoverySub.Unsubscribe()
	}()
	return nil
}

func (s *NATSDiscovery) PingCluster(ctx context.Context, id raft.ServerID, addr raft.ServerAddress, voter bool) error {
	bts, err := proto.Marshal(&pb.DiscoverPing{Sender: &pb.ServerInfo{
		ServerId:      string(id),
		ServerAddress: string(addr),
		Voter:         voter,
	}})
	if err != nil {
		return err
	}

	fmt.Println("sending request for existing servers", id)
	msg, err := s.nc.RequestWithContext(ctx, s.subj, bts)
	if err != nil {
		return err
	}

	var resp pb.DiscoverPong
	if r := proto.Unmarshal(msg.Data, &resp); r != nil {
		return r
	}
	if errMsg := resp.GetError(); errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

func (s *NATSDiscovery) discoveryHandler(msg *nats.Msg) {
	fmt.Println("incoming request for servers")
	var req pb.DiscoverPing
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		return
	}

	sender := req.GetSender()
	known, err := s.knownServer(sender)
	if err != nil {
		return
	}

	if !known {
		if e := s.cache.AddServer(sender.GetServerId(), sender.GetServerAddress(), sender.GetVoter()); e != nil {
			handleDiscoveryError(msg, e)
			fmt.Println("failed to add server to raft", e)
			return
		}
	}

	// TODO: pre-marshal
	bts, err := proto.Marshal(&pb.DiscoverPong{})
	if err != nil {
		return
	}
	if e := msg.Respond(bts); e != nil {
		return
	}
}

func (s *NATSDiscovery) knownServer(sender *pb.ServerInfo) (bool, error) {
	servers, err := s.cache.GetServers()
	if err != nil {
		return false, err
	}

	for _, server := range servers {
		if string(server.ID) == sender.ServerId {
			return true, nil
		}
	}
	return false, nil
}

func handleDiscoveryError(msg *nats.Msg, err error) {
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}

	bts, err := proto.Marshal(&pb.DiscoverPong{
		Error: errMsg,
	})
	if err != nil {
		return
	}

	if e := msg.Respond(bts); e != nil {
		return
	}
}
