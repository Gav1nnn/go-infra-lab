package p2p

import (
	"encoding/gob"
	"io"
)

// Encoder is anything that can encode an RPC into a byte stream.
type Decoder interface {
	Decode(io.Reader, *RPC) error
}

// a decoder that use gob to decode the RPC.
type GOBDecoder struct{}

func (dec GOBDecoder) Decode(r io.Reader, msg *RPC) error {
	return gob.NewDecoder(r).Decode(msg)
}

// the default decoder that we use in the transport layer.
// reads the first byte to determine the type of the message,
// then read the rest of the bytes as the payload.
type DefaultDecoder struct{}

func (dec DefaultDecoder) Decode(r io.Reader, msg *RPC) error {
	// to determine the type of the msg.
	peekBuf := make([]byte, 1)
	if _, err := r.Read(peekBuf); err != nil {
		return nil
	}
	// if the first byte is 0x2, then it's a stream.
	// we dont decode the body.
	stream := peekBuf[0] == IncomingStream
	if stream {
		msg.Stream = true
		return nil
	}
	// otherwise, it`s a msg,
	// we read the rest of the bytes as the payload.
	buf := make([]byte, 1028)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}

	msg.Payload = buf[:n]
	return nil
}
