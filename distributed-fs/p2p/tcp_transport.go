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
	// embed net.Conn so we automatically satisfy the net.Conn part of Peer
	net.Conn
	// if we dial a conn -> outbound = true
	// if we accept a conn -> outbound = false
	outbound bool
	// is used to wait for the peer to close the connection.
	wg *sync.WaitGroup
}

func NewTCPeer(conn net.Conn, outbound bool) *TCPeer {
	return &TCPeer{
		Conn:     conn,
		outbound: outbound,
		wg:       &sync.WaitGroup{},
	}
}

// CloseStream sends a signal that we want to close the connection stream.
func (p *TCPeer) CloseStream() error {
	p.wg.Done()
	return nil
}

// Send byte data to the peer.
func (p *TCPeer) Send(b []byte) error {
	_, err := p.Conn.Write(b)
	return err
}

// settings for the TCP transport layer.
type TCPTransportOpts struct {
	ListenAddr    string        // the address to listen on, e.g. ":8080"
	HandshakeFunc HandshakeFunc // the handshake func
	Decoder       Decoder
	// callback when a new peer is connected,
	// returns an error if the connection should be rejected.
	OnPeer func(Peer) error
}

type TCPTransport struct {
	TCPTransportOpts

	listener net.Listener // the TCP monitor
	rpcCh    chan RPC     // the channel to receive the incoming RPCs.
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
	// handle this in a new goroutine
	// true means this is an outbound connection.
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
		// streaming data needs to streaming to complete before continuing to read.
		if rpc.Stream {
			peer.wg.Add(1)
			fmt.Printf("[%s] incoming stream, waiting...\n", conn.RemoteAddr())
			peer.wg.Wait() // block, wait for CloseStream() be called
			fmt.Printf("[%s] stream closed, resuming read loop\n", conn.RemoteAddr())
			continue
		}
		// normal msgs will be sent to RPC channel
		t.rpcCh <- rpc
	}
}
