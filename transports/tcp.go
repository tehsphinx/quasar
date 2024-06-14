package transports

import (
	"errors"
	"net"
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

// NewTCPTransport creates a tcp transport layer for the cache.
func NewTCPTransport(bindAddr string, advertise net.Addr, opts ...TCPOption) (*TCPTransport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// Create stream
	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}

	// Verify that we have an advertisable address
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		_ = list.Close()
		return nil, errNotTCP
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		_ = list.Close()
		return nil, errNotAdvertisable
	}

	// Create the network transport
	trans := newTPCTransport(stream, opts...)
	return trans, nil
}
