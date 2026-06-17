package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// ManagerHTTPHandler exposes file APIs backed by ManagedFileService.
type ManagerHTTPHandler struct {
	service   *ManagedFileService
	httpStore *HTTPObjectStore
}

// NewManagerHTTPHandler creates HTTP handlers for file operations.
func NewManagerHTTPHandler(service *ManagedFileService, httpStore ...*HTTPObjectStore) *ManagerHTTPHandler {
	h := &ManagerHTTPHandler{service: service}
	if len(httpStore) > 0 {
		h.httpStore = httpStore[0]
	}
	return h
}

// ServeHTTP routes file and node API requests.
func (h *ManagerHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/files/"):
		h.handleFile(w, r)
	case r.URL.Path == "/nodes":
		h.handleNodes(w, r)
	case r.URL.Path == "/metrics":
		h.handleMetrics(w, r)
	case r.URL.Path == "/replication/run":
		h.handleReplicationRun(w, r)
	case strings.HasPrefix(r.URL.Path, "/internal/nodes/"):
		h.handleNodeHeartbeat(w, r)
	case r.URL.Path == "/healthz":
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

func (h *ManagerHTTPHandler) handleFile(w http.ResponseWriter, r *http.Request) {
	key, metadataOnly, err := fileKeyFromPath(r.URL.Path)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err)
		return
	}
	if metadataOnly {
		h.handleMetadata(w, r, key)
		return
	}

	switch r.Method {
	case http.MethodPut:
		meta, err := h.service.Put(key, r.Body)
		if err != nil {
			writeAPIError(w, http.StatusServiceUnavailable, err)
			return
		}
		writeAPIJSON(w, http.StatusCreated, meta)
	case http.MethodGet:
		data, meta, err := h.service.Get(key)
		if err != nil {
			writeAPIError(w, statusForError(err), err)
			return
		}
		defer data.Close()
		w.Header().Set("X-DFS-Version", strconv.FormatUint(meta.Version, 10))
		w.Header().Set("X-DFS-Checksum", meta.Checksum)
		w.WriteHeader(http.StatusOK)
		io.Copy(w, data)
	case http.MethodDelete:
		if _, err := h.service.Delete(key); err != nil {
			writeAPIError(w, statusForError(err), err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *ManagerHTTPHandler) handleMetadata(w http.ResponseWriter, r *http.Request, key string) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	meta, ok, err := h.service.Metadata(key)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeAPIError(w, http.StatusNotFound, ErrMetadataNotFound)
		return
	}
	writeAPIJSON(w, http.StatusOK, meta)
}

func (h *ManagerHTTPHandler) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	nodes, err := h.service.Nodes()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	writeAPIJSON(w, http.StatusOK, nodes)
}

func (h *ManagerHTTPHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	metrics, err := h.service.Metrics()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	writeAPIJSON(w, http.StatusOK, metrics)
}

func (h *ManagerHTTPHandler) handleNodeHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/internal/nodes/"))
	if err != nil || id == "" {
		writeAPIError(w, http.StatusBadRequest, fmt.Errorf("invalid node id"))
		return
	}

	var req struct {
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, err)
		return
	}
	if req.Addr == "" {
		writeAPIError(w, http.StatusBadRequest, fmt.Errorf("node addr is required"))
		return
	}

	node, err := h.service.RegisterNode(id, req.Addr)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	if h.httpStore != nil {
		h.httpStore.SetNode(id, req.Addr)
	}
	writeAPIJSON(w, http.StatusOK, node)
}

func (h *ManagerHTTPHandler) handleReplicationRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	task, err := h.service.RunReplicationOnce()
	if err != nil {
		writeAPIError(w, statusForError(err), err)
		return
	}
	writeAPIJSON(w, http.StatusOK, task)
}

// StorageHTTPHandler exposes versioned object operations for one storage node.
type StorageHTTPHandler struct {
	nodeID  string
	objects ObjectStore
}

// NewStorageHTTPHandler creates HTTP handlers for one storage node.
func NewStorageHTTPHandler(nodeID string, objects ObjectStore) *StorageHTTPHandler {
	return &StorageHTTPHandler{
		nodeID:  nodeID,
		objects: objects,
	}
}

// ServeHTTP routes object API requests.
func (h *StorageHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/objects/") {
		http.NotFound(w, r)
		return
	}

	key, version, err := objectRequest(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err)
		return
	}

	switch r.Method {
	case http.MethodPut:
		n, err := h.objects.WriteObject(h.nodeID, key, version, r.Body)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeAPIJSON(w, http.StatusCreated, map[string]any{"bytes": n})
	case http.MethodGet:
		size, data, err := h.objects.ReadObject(h.nodeID, key, version)
		if err != nil {
			writeAPIError(w, http.StatusNotFound, err)
			return
		}
		defer data.Close()
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		io.Copy(w, data)
	case http.MethodHead:
		if !h.objects.HasObject(h.nodeID, key, version) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := h.objects.DeleteObject(h.nodeID, key, version); err != nil {
			writeAPIError(w, http.StatusNotFound, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func fileKeyFromPath(path string) (string, bool, error) {
	raw := strings.TrimPrefix(path, "/files/")
	metadataOnly := strings.HasSuffix(raw, "/metadata")
	if metadataOnly {
		raw = strings.TrimSuffix(raw, "/metadata")
	}
	key, err := url.PathUnescape(raw)
	if err != nil {
		return "", false, err
	}
	if key == "" || strings.Contains(key, "..") {
		return "", false, fmt.Errorf("invalid file key")
	}
	return key, metadataOnly, nil
}

func objectRequest(r *http.Request) (string, uint64, error) {
	raw := strings.TrimPrefix(r.URL.Path, "/objects/")
	key, err := url.PathUnescape(raw)
	if err != nil {
		return "", 0, err
	}
	if key == "" || strings.Contains(key, "..") {
		return "", 0, fmt.Errorf("invalid object key")
	}
	version, err := strconv.ParseUint(r.URL.Query().Get("version"), 10, 64)
	if err != nil || version == 0 {
		return "", 0, fmt.Errorf("version is required")
	}
	return key, version, nil
}

func writeAPIJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeAPIError(w http.ResponseWriter, status int, err error) {
	writeAPIJSON(w, status, map[string]string{"error": err.Error()})
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, ErrMetadataNotFound), errors.Is(err, ErrFileDeleted):
		return http.StatusNotFound
	case errors.Is(err, ErrNoPendingTasks):
		return http.StatusNoContent
	case errors.Is(err, ErrNoHealthyNodes), errors.Is(err, ErrNoReadableReplicas):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
