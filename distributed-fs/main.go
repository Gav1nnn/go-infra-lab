package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Gav1nnn/go-infra-lab/distributed-fs/p2p"
)

type nodeFlags map[string]string

func (n nodeFlags) String() string {
	return fmt.Sprint(map[string]string(n))
}

func (n nodeFlags) Set(v string) error {
	parts := strings.SplitN(v, "=", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("node must be id=url")
	}
	n[parts[0]] = parts[1]
	return nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "manager":
		err = runManager(ctx, os.Args[2:])
	case "storage":
		err = runStorage(ctx, os.Args[2:])
	case "put":
		err = runPut(os.Args[2:])
	case "get":
		err = runGet(os.Args[2:])
	case "delete":
		err = runDelete(os.Args[2:])
	case "stat":
		err = runStat(os.Args[2:])
	case "nodes":
		err = runNodes(os.Args[2:])
	case "p2p-demo":
		runP2PDemo()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func runManager(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("manager", flag.ExitOnError)
	addr := fs.String("addr", ":9000", "HTTP listen address")
	metadataDB := fs.String("metadata-db", "data/manager/metadata.db", "bbolt metadata database path")
	replicas := fs.Int("replicas", defaultReplicaCount, "replica count")
	nodes := nodeFlags{}
	fs.Var(nodes, "node", "storage node id=url")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return fmt.Errorf("at least one -node id=url is required")
	}

	if err := os.MkdirAll(filepath.Dir(*metadataDB), 0755); err != nil {
		return err
	}
	metadata, err := OpenDiskMetadataStore(*metadataDB)
	if err != nil {
		return err
	}
	defer metadata.Close()
	tasks, err := NewDiskReplicationTaskQueue(metadata.db)
	if err != nil {
		return err
	}

	objects := NewHTTPObjectStore(nodes)
	service := NewManagedFileServiceWithMetadataAndQueue(*replicas, objects, metadata, tasks)
	for id, addr := range nodes {
		if _, err := service.RegisterNode(id, addr); err != nil {
			return err
		}
	}

	go managerLoop(ctx, service)

	srv := &http.Server{
		Addr:    *addr,
		Handler: NewManagerHTTPHandler(service, objects),
	}
	go shutdownServer(ctx, srv)

	log.Printf("manager listening on %s", *addr)
	err = srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func runStorage(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("storage", flag.ExitOnError)
	id := fs.String("id", "", "storage node ID")
	addr := fs.String("addr", ":9101", "HTTP listen address")
	advertise := fs.String("advertise", "", "address registered with manager")
	manager := fs.String("manager", "", "manager URL for heartbeat")
	root := fs.String("root", "data/storage", "storage root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("-id is required")
	}

	store := NewStore(StoreOpts{
		Root:              *root,
		PathTransformFunc: CASPathTransformFunc,
	})
	srv := &http.Server{
		Addr:    *addr,
		Handler: NewStorageHTTPHandler(*id, NewLocalObjectStore(store)),
	}
	go shutdownServer(ctx, srv)
	if *manager != "" {
		heartbeatAddr := *advertise
		if heartbeatAddr == "" {
			heartbeatAddr = "http://127.0.0.1" + *addr
		}
		go storageHeartbeatLoop(ctx, *manager, *id, heartbeatAddr)
	}

	log.Printf("storage node %s listening on %s", *id, *addr)
	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func runPut(args []string) error {
	fs, manager := clientFlags("put")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: fs put [flags] <key> <path>")
	}
	file, err := os.Open(fs.Arg(1))
	if err != nil {
		return err
	}
	defer file.Close()

	req := mustRequest(http.MethodPut, fileURL(*manager, fs.Arg(0)), file)
	if fi, err := file.Stat(); err == nil {
		req.ContentLength = fi.Size()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return responseError(resp)
	}
	var meta FileMetadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return err
	}
	fmt.Printf("stored key=%s version=%d size=%d checksum=%s\n", meta.Key, meta.Version, meta.Size, meta.Checksum)
	return nil
}

func runGet(args []string) error {
	fs, manager := clientFlags("get")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: fs get [flags] <key> <out>")
	}
	resp, err := http.Get(fileURL(*manager, fs.Arg(0)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return responseError(resp)
	}
	out, err := os.Create(fs.Arg(1))
	if err != nil {
		return err
	}
	n, copyErr := io.Copy(out, resp.Body)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Printf("fetched key=%s bytes=%d\n", fs.Arg(0), n)
	return nil
}

func runDelete(args []string) error {
	fs, manager := clientFlags("delete")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: fs delete [flags] <key>")
	}
	resp, err := http.DefaultClient.Do(mustRequest(http.MethodDelete, fileURL(*manager, fs.Arg(0)), nil))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return responseError(resp)
	}
	fmt.Printf("deleted key=%s\n", fs.Arg(0))
	return nil
}

func runStat(args []string) error {
	fs, manager := clientFlags("stat")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: fs stat [flags] <key>")
	}
	return printJSON(fileURL(*manager, fs.Arg(0)) + "/metadata")
}

func runNodes(args []string) error {
	fs, manager := clientFlags("nodes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: fs nodes [flags]")
	}
	return printJSON(strings.TrimRight(*manager, "/") + "/nodes")
}

func clientFlags(name string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	manager := fs.String("manager", "http://127.0.0.1:9000", "manager URL")
	return fs, manager
}

func managerLoop(ctx context.Context, service *ManagedFileService) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := service.MarkExpiredNodes(10 * time.Second); err != nil {
				log.Printf("mark expired nodes failed: %v", err)
			}
			if _, err := service.RequeueExpiredReplicationTasks(); err != nil {
				log.Printf("requeue expired replication tasks failed: %v", err)
			}
			if _, err := service.PlanRepair(); err != nil {
				log.Printf("plan repair failed: %v", err)
			}
			for {
				if _, err := service.RunReplicationOnce(); err != nil {
					break
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func storageHeartbeatLoop(ctx context.Context, manager, nodeID, addr string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := sendHeartbeat(manager, nodeID, addr); err != nil {
			log.Printf("heartbeat failed: %v", err)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func sendHeartbeat(manager, nodeID, addr string) error {
	body, err := json.Marshal(map[string]string{"addr": addr})
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(manager, "/") + "/internal/nodes/" + url.PathEscape(nodeID)
	resp, err := http.DefaultClient.Do(mustRequest(http.MethodPut, endpoint, bytes.NewReader(body)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return responseError(resp)
	}
	return nil
}

func shutdownServer(ctx context.Context, srv *http.Server) {
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func fileURL(manager, key string) string {
	return strings.TrimRight(manager, "/") + "/files/" + url.PathEscape(key)
}

func mustRequest(method, url string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		panic(err)
	}
	return req
}

func responseError(resp *http.Response) error {
	data, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("request failed: %s %s", resp.Status, strings.TrimSpace(string(data)))
}

func printJSON(endpoint string) error {
	resp, err := http.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return responseError(resp)
	}
	var v any
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return err
	}
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func usage() {
	fmt.Println(`usage:
  fs manager -node node1=http://127.0.0.1:9101 [-addr :9000] [-metadata-db data/manager/metadata.db]
  fs storage -id node1 [-addr :9101] [-root data/node1] [-manager http://127.0.0.1:9000]
  fs put [-manager http://127.0.0.1:9000] <key> <path>
  fs get [-manager http://127.0.0.1:9000] <key> <out>
  fs delete [-manager http://127.0.0.1:9000] <key>
  fs stat [-manager http://127.0.0.1:9000] <key>
  fs nodes [-manager http://127.0.0.1:9000]
  fs p2p-demo`)
}

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
		EncKey:            newEncryptionKey(),      // random AES-256 key
		StorageRoot:       listenAddr + "_network", // e.g. ":3000_network"
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	s := NewFileServer(fileServerOpts)

	// Wire up the transport's OnPeer callback to the FileServer.
	tcpTransport.OnPeer = s.OnPeer

	return s
}

func runP2PDemo() {
	s1 := makeServer(":3000")
	s2 := makeServer(":7000")
	s3 := makeServer(":5000", ":3000", ":7000")

	go func() { log.Fatal(s1.Start()) }()
	time.Sleep(500 * time.Millisecond)

	go func() { log.Fatal(s2.Start()) }()
	time.Sleep(2 * time.Second)

	go s3.Start()
	time.Sleep(2 * time.Second)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("picture_%d.png", i)
		data := bytes.NewReader([]byte("my big data file here!"))

		s3.Store(key, data)
		if err := s3.store.Delete(s3.ID, key); err != nil {
			log.Fatal(err)
		}

		r, err := s3.Get(key)
		if err != nil {
			log.Fatal(err)
		}

		b, err := io.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))
	}
}
