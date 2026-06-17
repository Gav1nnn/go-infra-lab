package p2p

import (
	"errors"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTCPTransport(t *testing.T) {
	opts := TCPTransportOpts{
		ListenAddr:    "127.0.0.1:0",
		HandshakeFunc: NOPHandshakeFunc,
		Decoder:       DefaultDecoder{},
	}

	tr := NewTCPTransport(opts)
	assert.Equal(t, tr.ListenAddr, "127.0.0.1:0")

	err := tr.ListenAndAccept()
	var opErr *net.OpError
	if errors.As(err, &opErr) || errors.Is(err, os.ErrPermission) {
		t.Skipf("tcp listen is not available in this test environment: %v", err)
	}
	assert.Nil(t, err)
	if err == nil {
		assert.Nil(t, tr.Close())
	}
}
