package main

import "time"

// MetadataBackend is the interface used by the coordinator.
type MetadataBackend interface {
	UpsertNode(id, addr string) (NodeMetadata, error)
	MarkExpiredNodes(ttl time.Duration) error
	HealthyNodes() ([]NodeMetadata, error)
	Nodes() ([]NodeMetadata, error)
	BeginFileVersion(key string, size int64, checksum string, chunks []ChunkMetadata, primary string, replicas []string) (FileMetadata, error)
	NextVersion(key string) (uint64, error)
	MarkReplica(key, nodeID string, state ReplicaState) (FileMetadata, error)
	Tombstone(key string) (FileMetadata, error)
	GetFile(key string) (FileMetadata, bool, error)
	ListFiles() ([]FileMetadata, error)
	AddReplica(key, nodeID string, state ReplicaState) (FileMetadata, error)
	Now() time.Time
}

// MemoryMetadataBackend adapts the in-memory MetadataStore to MetadataBackend.
type MemoryMetadataBackend struct {
	store *MetadataStore
}

// NewMemoryMetadataBackend creates an in-memory metadata backend.
func NewMemoryMetadataBackend(store *MetadataStore) *MemoryMetadataBackend {
	return &MemoryMetadataBackend{store: store}
}

// UpsertNode creates or refreshes a storage node heartbeat.
func (m *MemoryMetadataBackend) UpsertNode(id, addr string) (NodeMetadata, error) {
	return m.store.UpsertNode(id, addr), nil
}

// MarkExpiredNodes marks nodes as down if their heartbeat is too old.
func (m *MemoryMetadataBackend) MarkExpiredNodes(ttl time.Duration) error {
	m.store.MarkExpiredNodes(ttl)
	return nil
}

// HealthyNodes returns nodes that can accept reads, writes, and repair work.
func (m *MemoryMetadataBackend) HealthyNodes() ([]NodeMetadata, error) {
	return m.store.HealthyNodes(), nil
}

// Nodes returns all known nodes sorted by node ID.
func (m *MemoryMetadataBackend) Nodes() ([]NodeMetadata, error) {
	return m.store.Nodes(), nil
}

// BeginFileVersion records a new primary replica and pending secondary replicas.
func (m *MemoryMetadataBackend) BeginFileVersion(key string, size int64, checksum string, chunks []ChunkMetadata, primary string, replicas []string) (FileMetadata, error) {
	return m.store.BeginFileVersion(key, size, checksum, chunks, primary, replicas), nil
}

// NextVersion returns the next version number for a file key.
func (m *MemoryMetadataBackend) NextVersion(key string) (uint64, error) {
	return m.store.NextVersion(key), nil
}

// MarkReplica updates a single replica state.
func (m *MemoryMetadataBackend) MarkReplica(key, nodeID string, state ReplicaState) (FileMetadata, error) {
	return m.store.MarkReplica(key, nodeID, state)
}

// Tombstone marks a file as deleted while preserving its version history.
func (m *MemoryMetadataBackend) Tombstone(key string) (FileMetadata, error) {
	return m.store.Tombstone(key)
}

// GetFile returns one file metadata record.
func (m *MemoryMetadataBackend) GetFile(key string) (FileMetadata, bool, error) {
	meta, ok := m.store.GetFile(key)
	return meta, ok, nil
}

// ListFiles returns all file metadata records.
func (m *MemoryMetadataBackend) ListFiles() ([]FileMetadata, error) {
	return m.store.ListFiles(), nil
}

// AddReplica records a new replica target for an existing file.
func (m *MemoryMetadataBackend) AddReplica(key, nodeID string, state ReplicaState) (FileMetadata, error) {
	return m.store.AddReplica(key, nodeID, state)
}

// Now returns the backend clock.
func (m *MemoryMetadataBackend) Now() time.Time {
	return m.store.now()
}
