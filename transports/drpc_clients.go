package transports

import (
	"net"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"storj.io/drpc/drpcconn"
)

func newClientsPool() *clients {
	return &clients{
		clients: map[raft.ServerAddress]pb.DRPCQuasarServiceClient{},
	}
}

type clients struct {
	clients  map[raft.ServerAddress]pb.DRPCQuasarServiceClient
	m        sync.RWMutex
	mConnect sync.Mutex
}

func (s *clients) GetClient(target raft.ServerAddress) (pb.DRPCQuasarServiceClient, error) {
	serviceClient, ok := s.get(target)
	if ok {
		return serviceClient, nil
	}

	// a mutex to make sure we don't connect to the same target multiple times.
	s.mConnect.Lock()
	defer s.mConnect.Unlock()

	// target could be connected meanwhile
	serviceClient, ok = s.get(target)
	if ok {
		return serviceClient, nil
	}

	client, err := getClient(target)
	if err != nil {
		return nil, err
	}

	s.set(target, client)

	return client, err
}

func (s *clients) get(target raft.ServerAddress) (pb.DRPCQuasarServiceClient, bool) {
	s.m.RLock()
	defer s.m.RUnlock()

	client, ok := s.clients[target]
	return client, ok
}

func (s *clients) set(target raft.ServerAddress, client pb.DRPCQuasarServiceClient) {
	s.m.Lock()
	defer s.m.Unlock()

	s.clients[target] = client
}

func getClient(target raft.ServerAddress) (pb.DRPCQuasarServiceClient, error) {
	conn, err := net.DialTimeout("tcp", string(target), defaultTimout)
	if err != nil {
		return nil, err
	}

	// TODO: If you want TLS, you need to wrap the net.Conn with TLS before making a DRPC conn.

	client := pb.NewDRPCQuasarServiceClient(drpcconn.New(conn))
	return client, nil
}
