// Package frozennats is a NATS server for tests that stops answering on demand.
package frozennats

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// Server answers the NATS handshake and the client's pings until it is frozen.
// From then on it neither reads nor writes, on the connections it has and on
// new ones, like the NATS server of a host that stopped scheduling: the TCP
// connections stay up. It knows no subjects: what a client publishes is read
// and dropped.
type Server struct {
	ln     net.Listener
	frozen chan struct{}
	done   chan struct{}
}

// Run starts a server on a free loopback port and stops it with the test.
func Run(tb testing.TB) *Server {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("frozennats: listen: %v", err)
	}
	s := &Server{ln: ln, frozen: make(chan struct{}), done: make(chan struct{})}
	tb.Cleanup(func() {
		close(s.done)
		_ = ln.Close()
	})

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(conn)
		}
	}()
	return s
}

// URL is the address clients connect to.
func (s *Server) URL() string {
	return "nats://" + s.ln.Addr().String()
}

// Freeze stops the server from answering. There is no way back.
func (s *Server) Freeze() {
	close(s.frozen)
}

func (s *Server) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	const info = `INFO {"server_id":"frozen","version":"2.10.0","proto":1,"headers":true,"max_payload":1048576}`
	if !s.write(conn, info+"\r\n") {
		return
	}
	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if strings.HasPrefix(line, "PING") && !s.write(conn, "PONG\r\n") {
			return
		}
	}
}

// write sends msg, or holds the connection without a word once the server is
// frozen.
func (s *Server) write(conn net.Conn, msg string) bool {
	select {
	case <-s.frozen:
		<-s.done
		return false
	default:
	}
	_, err := conn.Write([]byte(msg))
	return err == nil
}
