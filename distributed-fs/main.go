package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/Gav1nnn/go-infra-lab/distributed-fs/p2p"
)

// makeServer creates a FileServer with the given listen address and bootstrap nodes.
// It sets up a TCP transport, encryption key, and CAS storage root.
func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcptransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc, // skip handshake for dev
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcptransportOpts)

	fileServerOpts := FileServerOpts{
		EncKey:            newEncryptionKey(),       // random AES-256 key
		StorageRoot:       listenAddr + "_network",  // e.g. ":3000_network"
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	s := NewFileServer(fileServerOpts)

	// Wire up the transport's OnPeer callback to the FileServer.
	tcpTransport.OnPeer = s.OnPeer

	return s
}

func main() {
	// Create 3 file server nodes.
	// s3 will use s1 and s2 as bootstrap nodes.
	s1 := makeServer(":3000")            // node 1, seed
	s2 := makeServer(":7000")            // node 2, seed
	s3 := makeServer(":5000", ":3000", ":7000") // node 3, connects to both

	// Start node 1
	go func() { log.Fatal(s1.Start()) }()
	time.Sleep(500 * time.Millisecond)

	// Start node 2
	go func() { log.Fatal(s2.Start()) }()
	time.Sleep(2 * time.Second)

	// Start node 3
	go s3.Start()
	time.Sleep(2 * time.Second)

	// Store 20 files via s3, then delete local copy and fetch from network.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("picture_%d.png", i)
		data := bytes.NewReader([]byte("my big data file here!"))

		// Store the file — it gets written locally and broadcast to peers.
		s3.Store(key, data)

		// Delete local copy so the next Get must fetch from the network.
		if err := s3.store.Delete(s3.ID, key); err != nil {
			log.Fatal(err)
		}

		// Fetch from network (s1 or s2 still has it).
		r, err := s3.Get(key)
		if err != nil {
			log.Fatal(err)
		}

		b, err := ioutil.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(b))
	}
}
