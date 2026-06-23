package main

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// ReplicaState represents the current health of a file replica.
type ReplicaState string

const (
	ReplicaPending ReplicaState = "pending" // replica is waiting to be copied.
	ReplicaHealthy ReplicaState = "healthy" // replica is readable and matches metadata.
	ReplicaStale   ReplicaState = "stale"   // replica exists but may not match the latest version.
	ReplicaMissing ReplicaState = "missing" // replica should exist but is not reachable/readable.
	ReplicaDeleted ReplicaState = "deleted" // replica has been removed after a tombstone.
)

// NodeState represents whether a storage node can receive new work.
type NodeState string

const (
	NodeHealthy NodeState = "healthy"
	NodeDown    NodeState = "down"
)

var (
	ErrMetadataNotFound = errors.New("metadata not found")
	ErrReplicaNotFound  = errors.New("replica not found")
)

// NodeMetadata tracks one storage node in the cluster.
type NodeMetadata struct {
	ID            string
	Addr          string
	State         NodeState
	LastHeartbeat time.Time
}

// ReplicaMetadata tracks one file replica on one storage node.
type ReplicaMetadata struct {
	NodeID    string
	State     ReplicaState
	UpdatedAt time.Time
}

// ChunkMetadata tracks one chunk in a file version.
type ChunkMetadata struct {
	Index    int    `json:"index"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}

// FileMetadata is the authoritative metadata record for one user key.
type FileMetadata struct {
	Key       string
	Version   uint64
	Size      int64
	Checksum  string
	Primary   string
	Deleted   bool
	Chunks    []ChunkMetadata
	Replicas  map[string]ReplicaMetadata
	CreatedAt time.Time
	UpdatedAt time.Time
}

// MetadataStore keeps authoritative file and node metadata.
type MetadataStore struct {
	mu    sync.RWMutex
	files map[string]FileMetadata
	nodes map[string]NodeMetadata
	now   func() time.Time
}

// NewMetadataStore creates an in-memory metadata store.
func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		files: make(map[string]FileMetadata),
		nodes: make(map[string]NodeMetadata),
		now:   time.Now,
	}
}

// UpsertNode creates or refreshes a storage node heartbeat.
func (m *MetadataStore) UpsertNode(id, addr string) NodeMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()

	node := NodeMetadata{
		ID:            id,
		Addr:          addr,
		State:         NodeHealthy,
		LastHeartbeat: m.now().UTC(),
	}
	m.nodes[id] = node
	return node
}

// MarkExpiredNodes marks nodes as down if their heartbeat is too old.
func (m *MetadataStore) MarkExpiredNodes(ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := m.now().UTC().Add(-ttl)
	for id, node := range m.nodes {
		if node.LastHeartbeat.Before(cutoff) {
			node.State = NodeDown
			m.nodes[id] = node
		}
	}
}

// HealthyNodes returns nodes that can accept reads, writes, and repair work.
func (m *MetadataStore) HealthyNodes() []NodeMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]NodeMetadata, 0, len(m.nodes))
	for _, node := range m.nodes {
		if node.State == NodeHealthy {
			nodes = append(nodes, node)
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes
}

// Nodes returns all known nodes sorted by node ID.
func (m *MetadataStore) Nodes() []NodeMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]NodeMetadata, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes
}

// BeginFileVersion records a new primary replica and pending secondary replicas.
func (m *MetadataStore) BeginFileVersion(key string, size int64, checksum string, chunks []ChunkMetadata, primary string, replicas []string) FileMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now().UTC()
	version, createdAt := m.nextVersionLocked(key, now)

	meta := FileMetadata{
		Key:       key,
		Version:   version,
		Size:      size,
		Checksum:  checksum,
		Primary:   primary,
		Deleted:   false,
		Chunks:    cloneChunkMetadata(chunks),
		Replicas:  make(map[string]ReplicaMetadata, len(replicas)),
		CreatedAt: createdAt,
		UpdatedAt: now,
	}

	for _, nodeID := range replicas {
		state := ReplicaPending
		if nodeID == primary {
			state = ReplicaHealthy
		}
		meta.Replicas[nodeID] = ReplicaMetadata{
			NodeID:    nodeID,
			State:     state,
			UpdatedAt: now,
		}
	}

	m.files[key] = meta
	return cloneFileMetadata(meta)
}

// NextVersion returns the next version number for a file key.
func (m *MetadataStore) NextVersion(key string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	version, _ := m.nextVersionLocked(key, m.now().UTC())
	return version
}

// MarkReplica updates a single replica state.
func (m *MetadataStore) MarkReplica(key, nodeID string, state ReplicaState) (FileMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	meta, ok := m.files[key]
	if !ok {
		return FileMetadata{}, ErrMetadataNotFound
	}

	replica, ok := meta.Replicas[nodeID]
	if !ok {
		return FileMetadata{}, ErrReplicaNotFound
	}

	now := m.now().UTC()
	replica.State = state
	replica.UpdatedAt = now
	meta.Replicas[nodeID] = replica
	meta.UpdatedAt = now
	m.files[key] = meta

	return cloneFileMetadata(meta), nil
}

// Tombstone marks a file as deleted while preserving its version history.
func (m *MetadataStore) Tombstone(key string) (FileMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	meta, ok := m.files[key]
	if !ok {
		return FileMetadata{}, ErrMetadataNotFound
	}

	now := m.now().UTC()
	meta.Version++
	meta.Deleted = true
	meta.UpdatedAt = now
	for nodeID, replica := range meta.Replicas {
		replica.State = ReplicaDeleted
		replica.UpdatedAt = now
		meta.Replicas[nodeID] = replica
	}
	m.files[key] = meta

	return cloneFileMetadata(meta), nil
}

// GetFile returns one file metadata record.
func (m *MetadataStore) GetFile(key string) (FileMetadata, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	meta, ok := m.files[key]
	if !ok {
		return FileMetadata{}, false
	}
	return cloneFileMetadata(meta), true
}

// ListFiles returns all file metadata records.
func (m *MetadataStore) ListFiles() []FileMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	files := make([]FileMetadata, 0, len(m.files))
	for _, meta := range m.files {
		files = append(files, cloneFileMetadata(meta))
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Key < files[j].Key
	})
	return files
}

// AddReplica records a new replica target for an existing file.
func (m *MetadataStore) AddReplica(key, nodeID string, state ReplicaState) (FileMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	meta, ok := m.files[key]
	if !ok {
		return FileMetadata{}, ErrMetadataNotFound
	}

	now := m.now().UTC()
	meta.Replicas[nodeID] = ReplicaMetadata{
		NodeID:    nodeID,
		State:     state,
		UpdatedAt: now,
	}
	meta.UpdatedAt = now
	m.files[key] = meta
	return cloneFileMetadata(meta), nil
}

// HealthyReplicas returns healthy replicas sorted by node ID.
func (m FileMetadata) HealthyReplicas() []ReplicaMetadata {
	replicas := make([]ReplicaMetadata, 0, len(m.Replicas))
	for _, replica := range m.Replicas {
		if replica.State == ReplicaHealthy {
			replicas = append(replicas, replica)
		}
	}
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].NodeID < replicas[j].NodeID
	})
	return replicas
}

func cloneFileMetadata(meta FileMetadata) FileMetadata {
	replicas := make(map[string]ReplicaMetadata, len(meta.Replicas))
	for nodeID, replica := range meta.Replicas {
		replicas[nodeID] = replica
	}
	meta.Replicas = replicas
	meta.Chunks = cloneChunkMetadata(meta.Chunks)
	return meta
}

func cloneChunkMetadata(chunks []ChunkMetadata) []ChunkMetadata {
	if chunks == nil {
		return nil
	}
	out := make([]ChunkMetadata, len(chunks))
	copy(out, chunks)
	return out
}

func (m *MetadataStore) nextVersionLocked(key string, now time.Time) (uint64, time.Time) {
	prev, ok := m.files[key]
	if !ok {
		return 1, now
	}
	return prev.Version + 1, prev.CreatedAt
}
