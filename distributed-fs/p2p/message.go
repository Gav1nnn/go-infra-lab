package p2p

const (
	// IncomingMessage marks a framed control message.
	IncomingMessage = 0x1
	// IncomingStream marks raw stream data.
	IncomingStream = 0x2
)

// RPC is one decoded message from a peer.
type RPC struct {
	From    string
	Payload []byte
	Stream  bool
}
