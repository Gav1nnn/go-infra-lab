package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

const defaultChunkSize int64 = 4 * 1024 * 1024

// ServiceMetrics summarizes manager state for observability.
type ServiceMetrics struct {
	Files            int                          `json:"files"`
	DeletedFiles     int                          `json:"deletedFiles"`
	PendingTasks     int                          `json:"pendingTasks"`
	ReplicationTasks map[ReplicationTaskState]int `json:"replicationTasks"`
	Nodes            map[NodeState]int            `json:"nodes"`
	Replicas         map[ReplicaState]int         `json:"replicas"`
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
	return NewManagedFileServiceWithMetadataAndQueue(replicaCount, objects, metadata, NewMemoryReplicationTaskQueue())
}

// NewManagedFileServiceWithMetadataAndQueue creates a service with custom metadata and task backends.
func NewManagedFileServiceWithMetadataAndQueue(replicaCount int, objects ObjectStore, metadata MetadataBackend, tasks ReplicationTaskQueue) *ManagedFileService {
	coordinator := NewMetadataCoordinatorWithBackendAndQueue(replicaCount, metadata, tasks)
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

	staged, size, checksum, chunks, err := stageReader(r)
	if err != nil {
		return FileMetadata{}, err
	}
	defer staged.Close()

	prepared, err := s.coordinator.PrepareWrite(key, size, checksum, chunks)
	if err != nil {
		return FileMetadata{}, err
	}

	if err := s.writeChunks(prepared.Primary.ID, key, prepared.Version, staged, chunks); err != nil {
		return FileMetadata{}, err
	}

	plan, err := s.coordinator.CommitWrite(prepared)
	if err != nil {
		s.deleteObjects(prepared.Primary.ID, key, prepared.Version, chunks)
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
		staged, err := s.stageReplica(replica.NodeID, plan.Metadata)
		if err != nil {
			if errors.Is(err, ErrChecksumMismatch) {
				s.coordinator.metadata.MarkReplica(key, replica.NodeID, ReplicaStale)
			} else {
				s.coordinator.metadata.MarkReplica(key, replica.NodeID, ReplicaMissing)
			}
			continue
		}
		if _, err := staged.Seek(0, io.SeekStart); err != nil {
			staged.Close()
			return nil, FileMetadata{}, err
		}
		return staged, plan.Metadata, nil
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
		s.deleteObjects(nodeID, key, meta.Version, meta.Chunks)
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
	tasks, err := s.coordinator.PendingTasks()
	if err != nil {
		return nil
	}
	return tasks
}

// PlanRepair enqueues repair tasks for missing or stale replicas.
func (s *ManagedFileService) PlanRepair() ([]ReplicationTask, error) {
	return s.coordinator.PlanRepair()
}

// RequeueExpiredReplicationTasks recovers replication tasks stuck in running.
func (s *ManagedFileService) RequeueExpiredReplicationTasks() ([]ReplicationTask, error) {
	return s.coordinator.RequeueExpiredReplicationTasks()
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
	taskStats, err := s.coordinator.ReplicationTaskStats()
	if err != nil {
		return ServiceMetrics{}, err
	}

	metrics := ServiceMetrics{
		PendingTasks:     len(s.PendingTasks()),
		ReplicationTasks: taskStats,
		Nodes:            make(map[NodeState]int),
		Replicas:         make(map[ReplicaState]int),
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

// writeChunks writes staged content to one storage node by chunk.
func (s *ManagedFileService) writeChunks(nodeID, key string, version uint64, staged *tempFileReadCloser, chunks []ChunkMetadata) error {
	if len(chunks) == 0 {
		if _, err := staged.Seek(0, io.SeekStart); err != nil {
			return err
		}
		_, err := s.objects.WriteObject(nodeID, key, version, staged)
		return err
	}

	for _, chunk := range chunks {
		if _, err := staged.Seek(chunk.Offset, io.SeekStart); err != nil {
			s.deleteObjects(nodeID, key, version, chunks)
			return err
		}
		objectKey := chunkObjectKey(key, chunk.Index)
		n, err := s.objects.WriteObject(nodeID, objectKey, version, io.LimitReader(staged, chunk.Size))
		if err != nil {
			s.deleteObjects(nodeID, key, version, chunks)
			return err
		}
		if n != chunk.Size {
			s.deleteObjects(nodeID, key, version, chunks)
			return io.ErrShortWrite
		}
	}
	return nil
}

// stageReplica rebuilds one file version from a replica and verifies checksums.
func (s *ManagedFileService) stageReplica(nodeID string, meta FileMetadata) (*tempFileReadCloser, error) {
	if len(meta.Chunks) == 0 {
		_, r, err := s.objects.ReadObject(nodeID, meta.Key, meta.Version)
		if err != nil {
			return nil, err
		}
		staged, size, checksum, _, err := stageReader(r)
		closeErr := r.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			staged.Close()
			return nil, closeErr
		}
		if size != meta.Size || checksum != meta.Checksum {
			staged.Close()
			return nil, ErrChecksumMismatch
		}
		return staged, nil
	}

	f, err := os.CreateTemp("", "dfs-object-*")
	if err != nil {
		return nil, err
	}
	staged := &tempFileReadCloser{File: f}
	fullHash := sha256.New()
	var total int64

	for _, chunk := range meta.Chunks {
		_, r, err := s.objects.ReadObject(nodeID, chunkObjectKey(meta.Key, chunk.Index), meta.Version)
		if err != nil {
			staged.Close()
			return nil, err
		}

		chunkHash := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(staged, fullHash, chunkHash), r)
		closeErr := r.Close()
		if copyErr != nil {
			staged.Close()
			return nil, copyErr
		}
		if closeErr != nil {
			staged.Close()
			return nil, closeErr
		}
		if n != chunk.Size || hex.EncodeToString(chunkHash.Sum(nil)) != chunk.Checksum {
			staged.Close()
			return nil, ErrChecksumMismatch
		}
		total += n
	}

	if total != meta.Size || hex.EncodeToString(fullHash.Sum(nil)) != meta.Checksum {
		staged.Close()
		return nil, ErrChecksumMismatch
	}
	return staged, nil
}

// deleteObjects removes either a legacy object or all chunk objects.
func (s *ManagedFileService) deleteObjects(nodeID, key string, version uint64, chunks []ChunkMetadata) {
	if len(chunks) == 0 {
		s.objects.DeleteObject(nodeID, key, version)
		return
	}
	for _, chunk := range chunks {
		s.objects.DeleteObject(nodeID, chunkObjectKey(key, chunk.Index), version)
	}
}

// stageReader copies a stream to a temp file and builds its chunk manifest.
func stageReader(r io.Reader) (*tempFileReadCloser, int64, string, []ChunkMetadata, error) {
	f, err := os.CreateTemp("", "dfs-object-*")
	if err != nil {
		return nil, 0, "", nil, err
	}

	h := sha256.New()
	chunkHash := sha256.New()
	chunks := []ChunkMetadata{}
	buf := make([]byte, 32*1024)
	var total int64
	var chunkSize int64
	chunkIndex := 0
	chunkOffset := int64(0)

	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			data := buf[:n]
			if _, err := f.Write(data); err != nil {
				f.Close()
				os.Remove(f.Name())
				return nil, 0, "", nil, err
			}
			if _, err := h.Write(data); err != nil {
				f.Close()
				os.Remove(f.Name())
				return nil, 0, "", nil, err
			}

			remaining := data
			for len(remaining) > 0 {
				space := int(defaultChunkSize - chunkSize)
				if space > len(remaining) {
					space = len(remaining)
				}
				part := remaining[:space]
				if _, err := chunkHash.Write(part); err != nil {
					f.Close()
					os.Remove(f.Name())
					return nil, 0, "", nil, err
				}
				chunkSize += int64(len(part))
				total += int64(len(part))
				remaining = remaining[space:]

				if chunkSize == defaultChunkSize {
					chunks = append(chunks, ChunkMetadata{
						Index:    chunkIndex,
						Offset:   chunkOffset,
						Size:     chunkSize,
						Checksum: hex.EncodeToString(chunkHash.Sum(nil)),
					})
					chunkIndex++
					chunkOffset = total
					chunkSize = 0
					chunkHash = sha256.New()
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			f.Close()
			os.Remove(f.Name())
			return nil, 0, "", nil, readErr
		}
	}

	if chunkSize > 0 || total == 0 {
		chunks = append(chunks, ChunkMetadata{
			Index:    chunkIndex,
			Offset:   chunkOffset,
			Size:     chunkSize,
			Checksum: hex.EncodeToString(chunkHash.Sum(nil)),
		})
	}

	return &tempFileReadCloser{File: f}, total, hex.EncodeToString(h.Sum(nil)), chunks, nil
}

// tempFileReadCloser removes the temp file when it is closed.
type tempFileReadCloser struct {
	*os.File
}

func (f *tempFileReadCloser) Close() error {
	name := f.Name()
	err := f.File.Close()
	if removeErr := os.Remove(name); err == nil {
		err = removeErr
	}
	return err
}
