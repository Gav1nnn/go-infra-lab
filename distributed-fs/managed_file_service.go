package main

import (
	"bytes"
	"io"
	"sync"
	"time"
)

// ServiceMetrics summarizes manager state for observability.
type ServiceMetrics struct {
	Files        int                  `json:"files"`
	DeletedFiles int                  `json:"deletedFiles"`
	PendingTasks int                  `json:"pendingTasks"`
	Nodes        map[NodeState]int    `json:"nodes"`
	Replicas     map[ReplicaState]int `json:"replicas"`
}

// ManagedFileService is a single-process file service built on metadata and object storage.
type ManagedFileService struct {
	coordinator *MetadataCoordinator
	objects     ObjectStore
	worker      *ReplicationWorker
	writeLock   sync.Mutex
}

// NewManagedFileService creates a service for local integration and future APIs.
func NewManagedFileService(replicaCount int, objects ObjectStore) *ManagedFileService {
	return NewManagedFileServiceWithMetadata(replicaCount, objects, NewMemoryMetadataBackend(NewMetadataStore()))
}

// NewManagedFileServiceWithMetadata creates a service with a custom metadata backend.
func NewManagedFileServiceWithMetadata(replicaCount int, objects ObjectStore, metadata MetadataBackend) *ManagedFileService {
	coordinator := NewMetadataCoordinatorWithBackend(replicaCount, metadata)
	return &ManagedFileService{
		coordinator: coordinator,
		objects:     objects,
		worker:      NewReplicationWorker(coordinator, NewLocalObjectReplicator(objects)),
	}
}

// RegisterNode records a storage node that can store objects.
func (s *ManagedFileService) RegisterNode(id, addr string) (NodeMetadata, error) {
	return s.coordinator.RegisterNode(id, addr)
}

// Put writes a file to the primary replica and queues async secondary copies.
func (s *ManagedFileService) Put(key string, r io.Reader) (FileMetadata, error) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	data, err := io.ReadAll(r)
	if err != nil {
		return FileMetadata{}, err
	}

	prepared, err := s.coordinator.PrepareWrite(key, int64(len(data)), checksumBytes(data))
	if err != nil {
		return FileMetadata{}, err
	}

	if _, err := s.objects.WriteObject(prepared.Primary.ID, key, prepared.Version, bytes.NewReader(data)); err != nil {
		return FileMetadata{}, err
	}

	plan, err := s.coordinator.CommitWrite(prepared)
	if err != nil {
		s.objects.DeleteObject(prepared.Primary.ID, key, prepared.Version)
		return FileMetadata{}, err
	}

	return plan.Metadata, nil
}

// Get reads the latest file version from the first healthy replica.
func (s *ManagedFileService) Get(key string) (io.ReadCloser, FileMetadata, error) {
	plan, err := s.coordinator.ReadCandidates(key)
	if err != nil {
		return nil, FileMetadata{}, err
	}

	for _, replica := range plan.Replicas {
		_, r, err := s.objects.ReadObject(replica.NodeID, key, plan.Metadata.Version)
		if err != nil {
			s.coordinator.metadata.MarkReplica(key, replica.NodeID, ReplicaMissing)
			continue
		}

		data, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			s.coordinator.metadata.MarkReplica(key, replica.NodeID, ReplicaStale)
			continue
		}
		if checksumBytes(data) != plan.Metadata.Checksum {
			s.coordinator.metadata.MarkReplica(key, replica.NodeID, ReplicaStale)
			continue
		}
		return io.NopCloser(bytes.NewReader(data)), plan.Metadata, nil
	}

	return nil, FileMetadata{}, ErrNoReadableReplicas
}

// Delete writes a tombstone and removes known local object copies best-effort.
func (s *ManagedFileService) Delete(key string) (FileMetadata, error) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	meta, ok, err := s.coordinator.metadata.GetFile(key)
	if err != nil {
		return FileMetadata{}, err
	}
	if !ok {
		return FileMetadata{}, ErrMetadataNotFound
	}

	deleted, err := s.coordinator.DeleteFile(key)
	if err != nil {
		return FileMetadata{}, err
	}

	for nodeID := range meta.Replicas {
		s.objects.DeleteObject(nodeID, key, meta.Version)
	}
	return deleted, nil
}

// RunReplicationOnce runs one pending async replica copy.
func (s *ManagedFileService) RunReplicationOnce() (ReplicationTask, error) {
	return s.worker.WorkOnce()
}

// Metadata returns the authoritative metadata for a file.
func (s *ManagedFileService) Metadata(key string) (FileMetadata, bool, error) {
	return s.coordinator.metadata.GetFile(key)
}

// PendingTasks returns pending async copy tasks.
func (s *ManagedFileService) PendingTasks() []ReplicationTask {
	return s.coordinator.PendingTasks()
}

// PlanRepair enqueues repair tasks for missing or stale replicas.
func (s *ManagedFileService) PlanRepair() ([]ReplicationTask, error) {
	return s.coordinator.PlanRepair()
}

// MarkExpiredNodes marks nodes as down when heartbeats expire.
func (s *ManagedFileService) MarkExpiredNodes(ttl time.Duration) error {
	return s.coordinator.metadata.MarkExpiredNodes(ttl)
}

// Nodes returns all nodes known by the metadata coordinator.
func (s *ManagedFileService) Nodes() ([]NodeMetadata, error) {
	return s.coordinator.metadata.Nodes()
}

// Metrics returns a compact view of manager health.
func (s *ManagedFileService) Metrics() (ServiceMetrics, error) {
	nodes, err := s.coordinator.metadata.Nodes()
	if err != nil {
		return ServiceMetrics{}, err
	}
	files, err := s.coordinator.metadata.ListFiles()
	if err != nil {
		return ServiceMetrics{}, err
	}

	metrics := ServiceMetrics{
		PendingTasks: len(s.PendingTasks()),
		Nodes:        make(map[NodeState]int),
		Replicas:     make(map[ReplicaState]int),
	}
	for _, node := range nodes {
		metrics.Nodes[node.State]++
	}
	for _, file := range files {
		metrics.Files++
		if file.Deleted {
			metrics.DeletedFiles++
		}
		for _, replica := range file.Replicas {
			metrics.Replicas[replica.State]++
		}
	}
	return metrics, nil
}
