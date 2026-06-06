package p2p

import "net"

// Peer is an interface that represents the remote node.
type Peer interface {
	net.Conn
	Send([]byte) error
	CloseStream() error
}

// Transport is anything that handles communication.
// between the nodes in the network.
// TCP\UDP\QUIC\WebRTC\etc
type Transport interface {
	ListenAndAccept() error

	Addr() string

	Dial(string) error

	Consume() <-chan RPC

	Close() error
}
