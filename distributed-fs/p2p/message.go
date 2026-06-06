package p2p

const (
	// represents the type is a message.
	IncomingMessage = 0x1
	// represents the type is a stream.
	IncomingStream = 0x2
)

type RPC struct {
	From    string // the remote peer's address.
	Payload []byte // the body of the message.
	Stream  bool   // whether the payload is a stream or not.
}
