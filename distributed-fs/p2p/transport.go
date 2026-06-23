package p2p

import "net"

// Peer is one remote node connection.
type Peer interface {
	net.Conn
	Send([]byte) error
	CloseStream() error
}

// Transport handles peer-to-peer network traffic.
type Transport interface {
	ListenAndAccept() error

	Addr() string

	Dial(string) error

	Consume() <-chan RPC

	Close() error
}
