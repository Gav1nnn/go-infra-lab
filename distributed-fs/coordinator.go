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

// ReadPlan describes which replicas can serve the latest readable version.
type ReadPlan struct {
	Metadata FileMetadata
	Replicas []ReplicaMetadata
}

// MetadataCoordinator wires metadata, replica placement, and async copy tasks.
type MetadataCoordinator struct {
	metadata *MetadataStore
	planner  *ReplicationPlanner
	tasks    *ReplicationTaskQueue
}

// NewMetadataCoordinator creates the pure metadata control plane.
func NewMetadataCoordinator(replicaCount int) *MetadataCoordinator {
	return &MetadataCoordinator{
		metadata: NewMetadataStore(),
		planner:  NewReplicationPlanner(replicaCount),
		tasks:    NewReplicationTaskQueue(),
	}
}

// RegisterNode records a storage node heartbeat.
func (c *MetadataCoordinator) RegisterNode(id, addr string) NodeMetadata {
	return c.metadata.UpsertNode(id, addr)
}

// BeginWrite creates metadata for a new version and queues async replica work.
func (c *MetadataCoordinator) BeginWrite(key string, size int64, checksum string) (WritePlan, error) {
	plan, err := c.planner.Plan(c.metadata.HealthyNodes())
	if err != nil {
		return WritePlan{}, err
	}

	replicaIDs := make([]string, 0, len(plan.Replicas))
	for _, node := range plan.Replicas {
		replicaIDs = append(replicaIDs, node.ID)
	}

	meta := c.metadata.BeginFileVersion(key, size, checksum, plan.Primary.ID, replicaIDs)
	tasks, err := TasksForFile(meta, c.metadata.now())
	if err != nil {
		return WritePlan{}, err
	}
	c.tasks.Enqueue(tasks...)

	return WritePlan{
		Metadata: meta,
		Primary:  plan.Primary,
		Replicas: plan.Replicas,
		Tasks:    tasks,
	}, nil
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
	meta, ok := c.metadata.GetFile(key)
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
func (c *MetadataCoordinator) PendingTasks() []ReplicationTask {
	return c.tasks.Pending()
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
