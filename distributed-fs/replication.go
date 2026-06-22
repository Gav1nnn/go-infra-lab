package main

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

const defaultReplicaCount = 3

// ReplicationTaskQueue stores async copy tasks.
type ReplicationTaskQueue interface {
	Enqueue(...ReplicationTask) error
	Pending() ([]ReplicationTask, error)
	MarkRunning(string) (ReplicationTask, error)
	MarkDone(string) (ReplicationTask, error)
	MarkFailed(string, error) (ReplicationTask, error)
	RequeueExpiredRunning() ([]ReplicationTask, error)
	Stats() (map[ReplicationTaskState]int, error)
}

// ReplicationTaskState represents the lifecycle of an async copy task.
type ReplicationTaskState string

const (
	ReplicationTaskPending ReplicationTaskState = "pending"
	ReplicationTaskRunning ReplicationTaskState = "running"
	ReplicationTaskDone    ReplicationTaskState = "done"
	ReplicationTaskFailed  ReplicationTaskState = "failed"
	ReplicationTaskDead    ReplicationTaskState = "dead"
)

const (
	defaultReplicationTaskLease       = 30 * time.Second
	defaultReplicationTaskMaxAttempts = 5
	defaultReplicationTaskMaxBackoff  = time.Minute
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
	ID         string
	Key        string
	Version    uint64
	Checksum   string
	Source     string
	Target     string
	State      ReplicationTaskState
	Attempts   int
	LastError  string
	RunAfter   time.Time
	LeaseUntil time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
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

// MemoryReplicationTaskQueue stores async copy tasks in memory.
type MemoryReplicationTaskQueue struct {
	tasks map[string]ReplicationTask
	now   func() time.Time
}

// NewMemoryReplicationTaskQueue creates an empty in-memory task queue.
func NewMemoryReplicationTaskQueue() *MemoryReplicationTaskQueue {
	return &MemoryReplicationTaskQueue{
		tasks: make(map[string]ReplicationTask),
		now:   time.Now,
	}
}

// Enqueue inserts tasks if they do not already exist.
func (q *MemoryReplicationTaskQueue) Enqueue(tasks ...ReplicationTask) error {
	for _, task := range tasks {
		if _, ok := q.tasks[task.ID]; ok {
			continue
		}
		q.tasks[task.ID] = task
	}
	return nil
}

// Pending returns tasks that are waiting to run.
func (q *MemoryReplicationTaskQueue) Pending() ([]ReplicationTask, error) {
	now := q.now().UTC()
	tasks := make([]ReplicationTask, 0, len(q.tasks))
	for _, task := range q.tasks {
		if task.State == ReplicationTaskPending && readyToRun(task, now) {
			tasks = append(tasks, task)
		}
	}
	sortReplicationTasks(tasks)
	return tasks, nil
}

// MarkRunning marks a task as currently being copied.
func (q *MemoryReplicationTaskQueue) MarkRunning(id string) (ReplicationTask, error) {
	task, ok := q.tasks[id]
	if !ok {
		return ReplicationTask{}, ErrTaskNotFound
	}
	task.State = ReplicationTaskRunning
	task.Attempts++
	now := q.now().UTC()
	task.LeaseUntil = now.Add(defaultReplicationTaskLease)
	task.UpdatedAt = now
	q.tasks[id] = task
	return task, nil
}

// MarkDone marks a task as successfully copied.
func (q *MemoryReplicationTaskQueue) MarkDone(id string) (ReplicationTask, error) {
	task, ok := q.tasks[id]
	if !ok {
		return ReplicationTask{}, ErrTaskNotFound
	}
	task.State = ReplicationTaskDone
	task.LeaseUntil = time.Time{}
	task.RunAfter = time.Time{}
	task.LastError = ""
	task.UpdatedAt = q.now().UTC()
	q.tasks[id] = task
	return task, nil
}

// MarkFailed marks a task as failed and eligible for retry later.
func (q *MemoryReplicationTaskQueue) MarkFailed(id string, cause error) (ReplicationTask, error) {
	task, ok := q.tasks[id]
	if !ok {
		return ReplicationTask{}, ErrTaskNotFound
	}
	task = failReplicationTask(task, q.now().UTC(), cause)
	q.tasks[id] = task
	return task, nil
}

// RequeueExpiredRunning returns expired running tasks to pending or marks them dead.
func (q *MemoryReplicationTaskQueue) RequeueExpiredRunning() ([]ReplicationTask, error) {
	now := q.now().UTC()
	requeued := []ReplicationTask{}
	for id, task := range q.tasks {
		if task.State != ReplicationTaskRunning || task.LeaseUntil.IsZero() || task.LeaseUntil.After(now) {
			continue
		}
		task = expireReplicationTask(task, now)
		q.tasks[id] = task
		requeued = append(requeued, task)
	}
	sortReplicationTasks(requeued)
	return requeued, nil
}

// Stats returns task counts by state.
func (q *MemoryReplicationTaskQueue) Stats() (map[ReplicationTaskState]int, error) {
	stats := make(map[ReplicationTaskState]int)
	for _, task := range q.tasks {
		stats[task.State]++
	}
	return stats, nil
}

func replicationTaskID(key string, version uint64, source string, target string) string {
	return fmt.Sprintf("%s:%d:%s:%s", key, version, source, target)
}

func readyToRun(task ReplicationTask, now time.Time) bool {
	return task.RunAfter.IsZero() || !task.RunAfter.After(now)
}

func failReplicationTask(task ReplicationTask, now time.Time, cause error) ReplicationTask {
	task.LeaseUntil = time.Time{}
	task.LastError = ""
	if cause != nil {
		task.LastError = cause.Error()
	}
	if task.Attempts >= defaultReplicationTaskMaxAttempts {
		task.State = ReplicationTaskDead
		task.RunAfter = time.Time{}
	} else {
		task.State = ReplicationTaskPending
		task.RunAfter = now.Add(replicationBackoff(task.Attempts))
	}
	task.UpdatedAt = now
	return task
}

func expireReplicationTask(task ReplicationTask, now time.Time) ReplicationTask {
	task.LeaseUntil = time.Time{}
	task.LastError = "replication task lease expired"
	if task.Attempts >= defaultReplicationTaskMaxAttempts {
		task.State = ReplicationTaskDead
		task.RunAfter = time.Time{}
	} else {
		task.State = ReplicationTaskPending
		task.RunAfter = now
	}
	task.UpdatedAt = now
	return task
}

func replicationBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	backoff := time.Second * time.Duration(1<<attempts)
	if backoff > defaultReplicationTaskMaxBackoff {
		return defaultReplicationTaskMaxBackoff
	}
	return backoff
}

func sortReplicationTasks(tasks []ReplicationTask) {
	sort.Slice(tasks, func(i, j int) bool {
		if !tasks[i].RunAfter.Equal(tasks[j].RunAfter) {
			if tasks[i].RunAfter.IsZero() {
				return true
			}
			if tasks[j].RunAfter.IsZero() {
				return false
			}
			return tasks[i].RunAfter.Before(tasks[j].RunAfter)
		}
		return tasks[i].ID < tasks[j].ID
	})
}
