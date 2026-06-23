package p2p

// HandshakeFunc validates a new peer connection.
type HandshakeFunc func(Peer) error

// NOPHandshakeFunc accepts every peer.
func NOPHandshakeFunc(Peer) error {
	return nil
}
