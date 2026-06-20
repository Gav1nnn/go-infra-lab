package main

import "errors"

var (
	ErrFileDeleted        = errors.New("file is deleted")
	ErrNoReadableReplicas = errors.New("no readable replicas available")
)

// WritePlan describes where the caller should write a new file version.
type WritePlan struct {
	Metadata FileMetadata
	Primary  NodeMetadata
	Replicas []NodeMetadata
	Tasks    []ReplicationTask
}

// PreparedWrite describes a write before it is published to metadata.
type PreparedWrite struct {
	Key      string
	Version  uint64
	Size     int64
	Checksum string
	Primary  NodeMetadata
	Replicas []NodeMetadata
}

// ReadPlan describes which replicas can serve the latest readable version.
type ReadPlan struct {
	Metadata FileMetadata
	Replicas []ReplicaMetadata
}

// MetadataCoordinator wires metadata, replica placement, and async copy tasks.
type MetadataCoordinator struct {
	metadata MetadataBackend
	planner  *ReplicationPlanner
	tasks    ReplicationTaskQueue
}

// NewMetadataCoordinator creates the pure metadata control plane.
func NewMetadataCoordinator(replicaCount int) *MetadataCoordinator {
	return NewMetadataCoordinatorWithBackend(replicaCount, NewMemoryMetadataBackend(NewMetadataStore()))
}

// NewMetadataCoordinatorWithBackend creates a coordinator with a metadata backend.
func NewMetadataCoordinatorWithBackend(replicaCount int, metadata MetadataBackend) *MetadataCoordinator {
	return NewMetadataCoordinatorWithBackendAndQueue(replicaCount, metadata, NewMemoryReplicationTaskQueue())
}

// NewMetadataCoordinatorWithBackendAndQueue creates a coordinator with custom metadata and task backends.
func NewMetadataCoordinatorWithBackendAndQueue(replicaCount int, metadata MetadataBackend, tasks ReplicationTaskQueue) *MetadataCoordinator {
	return &MetadataCoordinator{
		metadata: metadata,
		planner:  NewReplicationPlanner(replicaCount),
		tasks:    tasks,
	}
}

// RegisterNode records a storage node heartbeat.
func (c *MetadataCoordinator) RegisterNode(id, addr string) (NodeMetadata, error) {
	return c.metadata.UpsertNode(id, addr)
}

// PrepareWrite chooses replicas without publishing readable metadata.
func (c *MetadataCoordinator) PrepareWrite(key string, size int64, checksum string) (PreparedWrite, error) {
	nodes, err := c.metadata.HealthyNodes()
	if err != nil {
		return PreparedWrite{}, err
	}

	plan, err := c.planner.Plan(nodes)
	if err != nil {
		return PreparedWrite{}, err
	}
	version, err := c.metadata.NextVersion(key)
	if err != nil {
		return PreparedWrite{}, err
	}

	return PreparedWrite{
		Key:      key,
		Version:  version,
		Size:     size,
		Checksum: checksum,
		Primary:  plan.Primary,
		Replicas: plan.Replicas,
	}, nil
}

// CommitWrite publishes metadata after the primary object has been written.
func (c *MetadataCoordinator) CommitWrite(prepared PreparedWrite) (WritePlan, error) {
	replicaIDs := make([]string, 0, len(prepared.Replicas))
	for _, node := range prepared.Replicas {
		replicaIDs = append(replicaIDs, node.ID)
	}

	meta, err := c.metadata.BeginFileVersion(prepared.Key, prepared.Size, prepared.Checksum, prepared.Primary.ID, replicaIDs)
	if err != nil {
		return WritePlan{}, err
	}
	tasks, err := TasksForFile(meta, c.metadata.Now())
	if err != nil {
		return WritePlan{}, err
	}
	if err := c.tasks.Enqueue(tasks...); err != nil {
		return WritePlan{}, err
	}

	return WritePlan{
		Metadata: meta,
		Primary:  prepared.Primary,
		Replicas: prepared.Replicas,
		Tasks:    tasks,
	}, nil
}

// BeginWrite creates metadata for a new version and queues async replica work.
func (c *MetadataCoordinator) BeginWrite(key string, size int64, checksum string) (WritePlan, error) {
	prepared, err := c.PrepareWrite(key, size, checksum)
	if err != nil {
		return WritePlan{}, err
	}
	return c.CommitWrite(prepared)
}

// FinishReplication marks a copy task done and makes the target replica healthy.
func (c *MetadataCoordinator) FinishReplication(taskID string) (FileMetadata, ReplicationTask, error) {
	task, err := c.tasks.MarkDone(taskID)
	if err != nil {
		return FileMetadata{}, ReplicationTask{}, err
	}

	meta, err := c.metadata.MarkReplica(task.Key, task.Target, ReplicaHealthy)
	if err != nil {
		return FileMetadata{}, ReplicationTask{}, err
	}
	return meta, task, nil
}

// StartReplication marks a copy task running before a worker copies data.
func (c *MetadataCoordinator) StartReplication(taskID string) (ReplicationTask, error) {
	return c.tasks.MarkRunning(taskID)
}

// FailReplication marks a copy task failed and records the target replica missing.
func (c *MetadataCoordinator) FailReplication(taskID string) (FileMetadata, ReplicationTask, error) {
	task, err := c.tasks.MarkFailed(taskID)
	if err != nil {
		return FileMetadata{}, ReplicationTask{}, err
	}

	meta, err := c.metadata.MarkReplica(task.Key, task.Target, ReplicaMissing)
	if err != nil {
		return FileMetadata{}, ReplicationTask{}, err
	}
	return meta, task, nil
}

// ReadCandidates returns healthy replicas for the latest readable version.
func (c *MetadataCoordinator) ReadCandidates(key string) (ReadPlan, error) {
	meta, ok, err := c.metadata.GetFile(key)
	if err != nil {
		return ReadPlan{}, err
	}
	if !ok {
		return ReadPlan{}, ErrMetadataNotFound
	}
	if meta.Deleted {
		return ReadPlan{}, ErrFileDeleted
	}

	replicas := prioritizePrimary(meta, meta.HealthyReplicas())
	if len(replicas) == 0 {
		return ReadPlan{}, ErrNoReadableReplicas
	}

	return ReadPlan{
		Metadata: meta,
		Replicas: replicas,
	}, nil
}

// DeleteFile writes a tombstone into metadata.
func (c *MetadataCoordinator) DeleteFile(key string) (FileMetadata, error) {
	return c.metadata.Tombstone(key)
}

// PendingTasks returns copy tasks that are waiting for a worker.
func (c *MetadataCoordinator) PendingTasks() ([]ReplicationTask, error) {
	return c.tasks.Pending()
}

// PlanRepair scans metadata and enqueues tasks for missing or stale replicas.
func (c *MetadataCoordinator) PlanRepair() ([]ReplicationTask, error) {
	nodes, err := c.metadata.HealthyNodes()
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, nil
	}

	tasks := []ReplicationTask{}
	files, err := c.metadata.ListFiles()
	if err != nil {
		return nil, err
	}
	for _, meta := range files {
		if meta.Deleted {
			continue
		}

		source := firstHealthyReplica(meta)
		if source == "" {
			continue
		}

		tasks = append(tasks, c.repairExistingReplicas(meta, source)...)
		tasks = append(tasks, c.repairMissingReplicas(meta, source, nodes)...)
	}

	if err := c.tasks.Enqueue(tasks...); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (c *MetadataCoordinator) repairExistingReplicas(meta FileMetadata, source string) []ReplicationTask {
	tasks := []ReplicationTask{}
	for nodeID, replica := range meta.Replicas {
		if nodeID == source || replica.State == ReplicaHealthy || replica.State == ReplicaDeleted {
			continue
		}
		tasks = append(tasks, ReplicationTask{
			ID:        replicationTaskID(meta.Key, meta.Version, source, nodeID),
			Key:       meta.Key,
			Version:   meta.Version,
			Checksum:  meta.Checksum,
			Source:    source,
			Target:    nodeID,
			State:     ReplicationTaskPending,
			CreatedAt: c.metadata.Now().UTC(),
			UpdatedAt: c.metadata.Now().UTC(),
		})
	}
	return tasks
}

func (c *MetadataCoordinator) repairMissingReplicas(meta FileMetadata, source string, nodes []NodeMetadata) []ReplicationTask {
	tasks := []ReplicationTask{}
	if len(meta.Replicas) >= c.planner.ReplicaCount {
		return tasks
	}

	for _, node := range nodes {
		if len(meta.Replicas)+len(tasks) >= c.planner.ReplicaCount {
			break
		}
		if _, ok := meta.Replicas[node.ID]; ok {
			continue
		}
		if _, err := c.metadata.AddReplica(meta.Key, node.ID, ReplicaPending); err != nil {
			continue
		}
		tasks = append(tasks, ReplicationTask{
			ID:        replicationTaskID(meta.Key, meta.Version, source, node.ID),
			Key:       meta.Key,
			Version:   meta.Version,
			Checksum:  meta.Checksum,
			Source:    source,
			Target:    node.ID,
			State:     ReplicationTaskPending,
			CreatedAt: c.metadata.Now().UTC(),
			UpdatedAt: c.metadata.Now().UTC(),
		})
	}
	return tasks
}

func prioritizePrimary(meta FileMetadata, replicas []ReplicaMetadata) []ReplicaMetadata {
	if len(replicas) == 0 || meta.Primary == "" {
		return replicas
	}

	out := make([]ReplicaMetadata, 0, len(replicas))
	for _, replica := range replicas {
		if replica.NodeID == meta.Primary {
			out = append(out, replica)
			break
		}
	}
	for _, replica := range replicas {
		if replica.NodeID != meta.Primary {
			out = append(out, replica)
		}
	}
	return out
}

func firstHealthyReplica(meta FileMetadata) string {
	replicas := meta.HealthyReplicas()
	if len(replicas) == 0 {
		return ""
	}
	return replicas[0].NodeID
}
