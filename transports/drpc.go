package transports

import (
	"context"
	"net"

	"github.com/tehsphinx/quasar/pb/v1"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

// NewDRPCTransport creates a drpc (grpc drop-in replacement) transport layer for the cache.
// The context can be used to shut the server down.s
func NewDRPCTransport(ctx context.Context, bindAddr string, advertise net.Addr) (*DRPCTransport, error) {
	// TODO: Implement a TLS option

	// Verify that we have an advertisable address
	if advertise != nil {
		addr, ok := advertise.(*net.TCPAddr)
		if !ok {
			return nil, errNotTCP
		}
		if addr.IP == nil || addr.IP.IsUnspecified() {
			return nil, errNotAdvertisable
		}
	}

	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	if advertise == nil {
		advertise = list.Addr()
	}

	trans := newDRPCTransport(ctx, list)

	server := newDRPCServer(trans)

	// TODO: use advertise addr

	mux := drpcmux.New()
	if r := pb.DRPCRegisterQuasarService(mux, server); r != nil {
		return nil, r
	}
	srv := drpcserver.New(mux)
	go func() {
		// TODO: think about what to do with this error
		_ = srv.Serve(ctx, list)
	}()

	// Create the network transport
	return trans, nil
}
