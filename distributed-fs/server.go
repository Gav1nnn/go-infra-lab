package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/Gav1nnn/go-infra-lab/distributed-fs/p2p"
)

// FileServerOpts holds configuration for the FileServer.
type FileServerOpts struct {
	ID                string            // node ID (auto-generated if empty)
	EncKey            []byte            // AES encryption key
	StorageRoot       string            // root directory for file storage
	PathTransformFunc PathTransformFunc // function to transform keys into paths
	Transport         p2p.Transport     // network transport layer
	BootstrapNodes    []string          // addresses of known nodes to connect to
}

// FileServer is the core orchestrator that ties storage, networking, and encryption together.
type FileServer struct {
	FileServerOpts

	peerLock sync.Mutex
	peers    map[string]p2p.Peer // connected remote peers

	store  *Store        // local file storage engine
	quitch chan struct{} // signal channel to stop the server
}

// NewFileServer creates and returns a new FileServer with the given options.
func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = generateID()
	}

	return &FileServer{
		FileServerOpts: opts,
		store:          NewStore(storeOpts),
		quitch:         make(chan struct{}),
		peers:          make(map[string]p2p.Peer),
	}
}

// Message is the wrapper for all messages exchanged between peers.
type Message struct {
	Payload any // one of MessageStoreFile or MessageGetFile
}

// MessageStoreFile is sent when a peer wants to notify others about a stored file.
type MessageStoreFile struct {
	ID   string // sender node ID
	Key  string // hashed file key
	Size int64  // file size (including IV)
}

// MessageGetFile is sent when a peer requests a file from the network.
type MessageGetFile struct {
	ID  string // requester node ID
	Key string // hashed file key
}

func init() {
	// Register message types with gob so they can be encoded/decoded.
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
}

// OnPeer is called by the transport when a new peer connects.
func (s *FileServer) OnPeer(p p2p.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	s.peers[p.RemoteAddr().String()] = p

	log.Printf("connected with remote %s", p.RemoteAddr())
	return nil
}

// Stop signals the server to shut down.
func (s *FileServer) Stop() {
	close(s.quitch)
}

// -------------------------------------------------------------------
// Message broadcasting and event loop
// -------------------------------------------------------------------

// broadcast sends a gob-encoded Message to all connected peers.
func (s *FileServer) broadcast(msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range s.peers {
		// First byte tells the decoder this is a control message, not a stream.
		peer.Send([]byte{p2p.IncomingMessage})

		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// loop is the main event loop that consumes RPCs from the transport layer.
func (s *FileServer) loop() {
	defer func() {
		log.Println("file server stopped due to error or user quit action")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("decoding error: ", err)
			}
			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Println("handle message error: ", err)
			}
		case <-s.quitch:
			return
		}
	}
}

// handleMessage routes an incoming Message to the appropriate handler.
func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return s.handleMessageGetFile(from, v)
	}
	return nil
}

// -------------------------------------------------------------------
// Store (write) flow
// -------------------------------------------------------------------

// Store writes a file locally and broadcasts an encrypted copy to all peers.
func (s *FileServer) Store(key string, r io.Reader) error {
	var (
		fileBuffer = new(bytes.Buffer)
		// TeeReader duplicates the stream:
		//   - one copy goes to store.Write (local disk)
		//   - the other goes to fileBuffer (for network broadcast)
		tee = io.TeeReader(r, fileBuffer)
	)

	// 1. Write to local storage
	size, err := s.store.Write(s.ID, key, tee)
	if err != nil {
		return err
	}

	// 2. Broadcast a store notification to all peers.
	//    size + 16 accounts for the IV added during encryption.
	msg := Message{
		Payload: MessageStoreFile{
			ID:   s.ID,
			Key:  hashKey(key),
			Size: size + 16,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 5)

	// 3. Send the encrypted file data to all peers concurrently.
	peers := []io.Writer{}
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	// MultiWriter writes the same data to every peer at once.
	mw := io.MultiWriter(peers...)
	mw.Write([]byte{p2p.IncomingStream})            // mark as stream data
	n, err := copyEncrypt(s.EncKey, fileBuffer, mw) // encrypt and send
	if err != nil {
		return err
	}

	fmt.Printf("[%s] received and written (%d) bytes to disk\n", s.Transport.Addr(), n)

	return nil
}

// handleMessageStoreFile handles an incoming store-file request from a remote peer.
// It reads the file data from the peer's TCP connection and writes it to disk.
func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	// Read exactly msg.Size bytes from the connection (prevents hanging).
	n, err := s.store.Write(msg.ID, msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written %d bytes to disk\n", s.Transport.Addr(), n)

	// Signal the read loop that the stream is done, so it can continue.
	peer.CloseStream()

	return nil
}

// -------------------------------------------------------------------
// Get (read) flow
// -------------------------------------------------------------------

// Get retrieves a file. It checks local storage first; if not found,
// it requests the file from all connected peers over the network.
func (s *FileServer) Get(key string) (io.Reader, error) {
	// 1. Check local disk first.
	if s.store.Has(s.ID, key) {
		fmt.Printf("[%s] serving file (%s) from local disk\n", s.Transport.Addr(), key)
		_, r, err := s.store.Read(s.ID, key)
		return r, err
	}

	// 2. File not found locally — broadcast a get request to all peers.
	fmt.Printf("[%s] dont have file (%s) locally, fetching from network...\n", s.Transport.Addr(), key)

	msg := Message{
		Payload: MessageGetFile{
			ID:  s.ID,
			Key: hashKey(key),
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return nil, err
	}

	time.Sleep(time.Millisecond * 500)

	// 3. Read from the first responding peer.
	for _, peer := range s.peers {
		// Read the file size first so we know how many bytes to expect.
		var fileSize int64
		binary.Read(peer, binary.LittleEndian, &fileSize)

		// Decrypt the incoming data and write to local disk as a cache.
		n, err := s.store.WriteDecrypt(s.EncKey, s.ID, key, io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}

		fmt.Printf("[%s] received (%d) bytes over the network from (%s)", s.Transport.Addr(), n, peer.RemoteAddr())

		peer.CloseStream()
	}

	// 4. Read the now-local file and return it.
	_, r, err := s.store.Read(s.ID, key)
	return r, err
}

// handleMessageGetFile handles an incoming get-file request from a remote peer.
// It reads the file from local disk and sends it over the network.
func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	if !s.store.Has(msg.ID, msg.Key) {
		return fmt.Errorf("[%s] need to serve file (%s) but it does not exist on disk", s.Transport.Addr(), msg.Key)
	}

	fmt.Printf("[%s] serving file (%s) over the network\n", s.Transport.Addr(), msg.Key)

	fileSize, r, err := s.store.Read(msg.ID, msg.Key)
	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		fmt.Println("closing readCloser")
		defer rc.Close()
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	// Send: stream flag → file size → file data
	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer, binary.LittleEndian, fileSize)
	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written (%d) bytes over the network to %s\n", s.Transport.Addr(), n, from)

	return nil
}

// -------------------------------------------------------------------
// Network bootstrapping and startup
// -------------------------------------------------------------------

// bootstrapNetwork connects the server to its bootstrap (seed) nodes.
func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}

		go func(addr string) {
			fmt.Printf("[%s] attemping to connect with remote %s\n", s.Transport.Addr(), addr)
			if err := s.Transport.Dial(addr); err != nil {
				log.Println("dial error: ", err)
			}
		}(addr)
	}

	return nil
}

// Start begins listening for connections, bootstraps the network,
// and enters the main event loop (blocking).
func (s *FileServer) Start() error {
	fmt.Printf("[%s] starting fileserver...\n", s.Transport.Addr())

	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()

	s.loop()

	return nil
}
