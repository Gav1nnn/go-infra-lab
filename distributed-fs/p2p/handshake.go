package p2p

// HandshakeFunc is a function type that represents
// the handshake process between two peers.
// receives a peer and returns an error if the handshake fails.
// if not nil, it means the rejection of the connection.
type HandshakeFunc func(Peer) error

// a no-op, always successful handshake function.
func NOPHandshakeFunc(Peer) error {
	return nil
}
