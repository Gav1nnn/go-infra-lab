package p2p

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

// TCPeer represents the remote node connected via TCP.
type TCPeer struct {
	net.Conn
	outbound bool
	wg       *sync.WaitGroup
}

func NewTCPeer(conn net.Conn, outbound bool) *TCPeer {
	return &TCPeer{
		Conn:     conn,
		outbound: outbound,
		wg:       &sync.WaitGroup{},
	}
}

// CloseStream marks the active stream as complete.
func (p *TCPeer) CloseStream() error {
	p.wg.Done()
	return nil
}

// Send writes bytes to the peer connection.
func (p *TCPeer) Send(b []byte) error {
	_, err := p.Conn.Write(b)
	return err
}

// TCPTransportOpts configures TCP peer transport.
type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error
}

// TCPTransport accepts peer connections and decodes RPC messages.
type TCPTransport struct {
	TCPTransportOpts

	listener net.Listener
	rpcCh    chan RPC
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcCh:            make(chan RPC, 1024),
	}
}

// Addr returns the address that the transport is listening on.
func (t *TCPTransport) Addr() string {
	return t.ListenAddr
}

// Consume returns the channel to receive incoming RPCs.
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcCh
}

// Close closes the transport and all its connections.
func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

// Dial initializes a new connection to the specified address.
func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	go t.handleConn(conn, true)
	return nil
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}
	go t.startAcceptLoop()

	log.Printf("TCP transport is listening on port: %s\n", t.ListenAddr)

	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}

		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
		}

		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error

	defer func() {
		fmt.Printf("dropping peer conn: %s", err)
		conn.Close()
	}()

	peer := NewTCPeer(conn, outbound)

	if err = t.HandshakeFunc(peer); err != nil {
		return
	}

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			return
		}
	}

	for {
		rpc := RPC{}
		err = t.Decoder.Decode(conn, &rpc)
		if err != nil {
			return
		}

		rpc.From = conn.RemoteAddr().String()
		if rpc.Stream {
			peer.wg.Add(1)
			fmt.Printf("[%s] incoming stream, waiting...\n", conn.RemoteAddr())
			peer.wg.Wait()
			fmt.Printf("[%s] stream closed, resuming read loop\n", conn.RemoteAddr())
			continue
		}
		t.rpcCh <- rpc
	}
}
