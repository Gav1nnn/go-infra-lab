package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// HTTPObjectStore stores objects by calling remote storage nodes over HTTP.
type HTTPObjectStore struct {
	mu     sync.RWMutex
	nodes  map[string]string
	client *http.Client
}

// NewHTTPObjectStore creates an object store from node ID to base URL.
func NewHTTPObjectStore(nodes map[string]string) *HTTPObjectStore {
	return &HTTPObjectStore{
		nodes:  nodes,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// SetNode adds or updates a remote storage node address.
func (s *HTTPObjectStore) SetNode(nodeID, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodes[nodeID] = addr
}

// WriteObject writes one object version to a remote storage node.
func (s *HTTPObjectStore) WriteObject(nodeID, key string, version uint64, r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest(http.MethodPut, s.objectURL(nodeID, key, version), bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return 0, fmt.Errorf("storage write failed: %s", resp.Status)
	}
	return int64(len(data)), nil
}

// ReadObject reads one object version from a remote storage node.
func (s *HTTPObjectStore) ReadObject(nodeID, key string, version uint64) (int64, io.ReadCloser, error) {
	resp, err := s.client.Get(s.objectURL(nodeID, key, version))
	if err != nil {
		return 0, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return 0, nil, fmt.Errorf("storage read failed: %s", resp.Status)
	}
	return resp.ContentLength, resp.Body, nil
}

// DeleteObject removes one object version from a remote storage node.
func (s *HTTPObjectStore) DeleteObject(nodeID, key string, version uint64) error {
	req, err := http.NewRequest(http.MethodDelete, s.objectURL(nodeID, key, version), nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("storage delete failed: %s", resp.Status)
	}
	return nil
}

// HasObject checks whether one object version exists on a remote storage node.
func (s *HTTPObjectStore) HasObject(nodeID, key string, version uint64) bool {
	req, err := http.NewRequest(http.MethodHead, s.objectURL(nodeID, key, version), nil)
	if err != nil {
		return false
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (s *HTTPObjectStore) objectURL(nodeID, key string, version uint64) string {
	s.mu.RLock()
	base := strings.TrimRight(s.nodes[nodeID], "/")
	s.mu.RUnlock()
	return fmt.Sprintf("%s/objects/%s?version=%d", base, url.PathEscape(key), version)
}
