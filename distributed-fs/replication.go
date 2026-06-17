package main

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

const defaultReplicaCount = 3

// ReplicationTaskState represents the lifecycle of an async copy task.
type ReplicationTaskState string

const (
	ReplicationTaskPending ReplicationTaskState = "pending"
	ReplicationTaskRunning ReplicationTaskState = "running"
	ReplicationTaskDone    ReplicationTaskState = "done"
	ReplicationTaskFailed  ReplicationTaskState = "failed"
)

var (
	ErrNoHealthyNodes     = errors.New("no healthy nodes available")
	ErrPrimaryUnavailable = errors.New("primary replica is not available")
	ErrTaskNotFound       = errors.New("replication task not found")
)

// ReplicaPlan describes where a new file version should live.
type ReplicaPlan struct {
	Primary  NodeMetadata
	Replicas []NodeMetadata
}

// ReplicationTask describes one async copy from a healthy source to a target.
type ReplicationTask struct {
	ID        string
	Key       string
	Version   uint64
	Checksum  string
	Source    string
	Target    string
	State     ReplicationTaskState
	Attempts  int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ReplicationPlanner chooses primary and secondary replicas.
type ReplicationPlanner struct {
	ReplicaCount int
}

// NewReplicationPlanner creates a planner with the default replica count.
func NewReplicationPlanner(replicaCount int) *ReplicationPlanner {
	if replicaCount == 0 {
		replicaCount = defaultReplicaCount
	}
	return &ReplicationPlanner{
		ReplicaCount: replicaCount,
	}
}

// Plan chooses healthy nodes for a new file version.
func (p *ReplicationPlanner) Plan(nodes []NodeMetadata) (ReplicaPlan, error) {
	healthy := make([]NodeMetadata, 0, len(nodes))
	for _, node := range nodes {
		if node.State == NodeHealthy {
			healthy = append(healthy, node)
		}
	}
	if len(healthy) == 0 {
		return ReplicaPlan{}, ErrNoHealthyNodes
	}

	sort.Slice(healthy, func(i, j int) bool {
		return healthy[i].ID < healthy[j].ID
	})

	count := p.ReplicaCount
	if len(healthy) < count {
		count = len(healthy)
	}

	replicas := make([]NodeMetadata, count)
	copy(replicas, healthy[:count])

	return ReplicaPlan{
		Primary:  replicas[0],
		Replicas: replicas,
	}, nil
}

// TasksForFile creates pending tasks for every non-primary replica.
func TasksForFile(meta FileMetadata, now time.Time) ([]ReplicationTask, error) {
	if meta.Primary == "" {
		return nil, ErrPrimaryUnavailable
	}
	if _, ok := meta.Replicas[meta.Primary]; !ok {
		return nil, ErrPrimaryUnavailable
	}

	tasks := []ReplicationTask{}
	nodeIDs := make([]string, 0, len(meta.Replicas))
	for nodeID := range meta.Replicas {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	for _, nodeID := range nodeIDs {
		if nodeID == meta.Primary {
			continue
		}
		replica := meta.Replicas[nodeID]
		if replica.State != ReplicaPending && replica.State != ReplicaStale && replica.State != ReplicaMissing {
			continue
		}
		tasks = append(tasks, ReplicationTask{
			ID:        replicationTaskID(meta.Key, meta.Version, meta.Primary, nodeID),
			Key:       meta.Key,
			Version:   meta.Version,
			Checksum:  meta.Checksum,
			Source:    meta.Primary,
			Target:    nodeID,
			State:     ReplicationTaskPending,
			CreatedAt: now.UTC(),
			UpdatedAt: now.UTC(),
		})
	}

	return tasks, nil
}

// ReplicationTaskQueue stores async copy tasks in memory.
type ReplicationTaskQueue struct {
	tasks map[string]ReplicationTask
	now   func() time.Time
}

// NewReplicationTaskQueue creates an empty task queue.
func NewReplicationTaskQueue() *ReplicationTaskQueue {
	return &ReplicationTaskQueue{
		tasks: make(map[string]ReplicationTask),
		now:   time.Now,
	}
}

// Enqueue inserts tasks if they do not already exist.
func (q *ReplicationTaskQueue) Enqueue(tasks ...ReplicationTask) {
	for _, task := range tasks {
		if _, ok := q.tasks[task.ID]; ok {
			continue
		}
		q.tasks[task.ID] = task
	}
}

// Pending returns tasks that are waiting to run.
func (q *ReplicationTaskQueue) Pending() []ReplicationTask {
	tasks := make([]ReplicationTask, 0, len(q.tasks))
	for _, task := range q.tasks {
		if task.State == ReplicationTaskPending {
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID < tasks[j].ID
	})
	return tasks
}

// MarkRunning marks a task as currently being copied.
func (q *ReplicationTaskQueue) MarkRunning(id string) (ReplicationTask, error) {
	task, ok := q.tasks[id]
	if !ok {
		return ReplicationTask{}, ErrTaskNotFound
	}
	task.State = ReplicationTaskRunning
	task.Attempts++
	task.UpdatedAt = q.now().UTC()
	q.tasks[id] = task
	return task, nil
}

// MarkDone marks a task as successfully copied.
func (q *ReplicationTaskQueue) MarkDone(id string) (ReplicationTask, error) {
	task, ok := q.tasks[id]
	if !ok {
		return ReplicationTask{}, ErrTaskNotFound
	}
	task.State = ReplicationTaskDone
	task.UpdatedAt = q.now().UTC()
	q.tasks[id] = task
	return task, nil
}

// MarkFailed marks a task as failed and eligible for retry later.
func (q *ReplicationTaskQueue) MarkFailed(id string) (ReplicationTask, error) {
	task, ok := q.tasks[id]
	if !ok {
		return ReplicationTask{}, ErrTaskNotFound
	}
	task.State = ReplicationTaskFailed
	task.UpdatedAt = q.now().UTC()
	q.tasks[id] = task
	return task, nil
}

func replicationTaskID(key string, version uint64, source string, target string) string {
	return fmt.Sprintf("%s:%d:%s:%s", key, version, source, target)
}
