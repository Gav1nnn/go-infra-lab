package main

import (
	"encoding/json"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

var replicationTasksBucket = []byte("replication_tasks")

// DiskReplicationTaskQueue stores async copy tasks in bbolt.
type DiskReplicationTaskQueue struct {
	db  *bolt.DB
	now func() time.Time
}

// NewDiskReplicationTaskQueue creates a bbolt-backed task queue.
func NewDiskReplicationTaskQueue(db *bolt.DB) (*DiskReplicationTaskQueue, error) {
	q := &DiskReplicationTaskQueue{
		db:  db,
		now: time.Now,
	}
	if err := q.init(); err != nil {
		return nil, err
	}
	if err := q.restoreRetryableTasks(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *DiskReplicationTaskQueue) init() error {
	return q.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(replicationTasksBucket)
		return err
	})
}

func (q *DiskReplicationTaskQueue) restoreRetryableTasks() error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationTasksBucket)
		return b.ForEach(func(k, v []byte) error {
			task, err := decodeReplicationTask(v)
			if err != nil {
				return err
			}
			if task.State != ReplicationTaskRunning && task.State != ReplicationTaskFailed {
				return nil
			}
			task.State = ReplicationTaskPending
			task.UpdatedAt = q.now().UTC()
			return putReplicationTask(b, string(k), task)
		})
	})
}

// Enqueue inserts tasks if they do not already exist.
func (q *DiskReplicationTaskQueue) Enqueue(tasks ...ReplicationTask) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationTasksBucket)
		for _, task := range tasks {
			if b.Get([]byte(task.ID)) != nil {
				continue
			}
			if err := putReplicationTask(b, task.ID, task); err != nil {
				return err
			}
		}
		return nil
	})
}

// Pending returns tasks that are waiting to run.
func (q *DiskReplicationTaskQueue) Pending() ([]ReplicationTask, error) {
	tasks := []ReplicationTask{}
	err := q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationTasksBucket)
		return b.ForEach(func(_, v []byte) error {
			task, err := decodeReplicationTask(v)
			if err != nil {
				return err
			}
			if task.State == ReplicationTaskPending {
				tasks = append(tasks, task)
			}
			return nil
		})
	})
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID < tasks[j].ID
	})
	return tasks, err
}

// MarkRunning marks a task as currently being copied.
func (q *DiskReplicationTaskQueue) MarkRunning(id string) (ReplicationTask, error) {
	return q.updateTask(id, func(task ReplicationTask) ReplicationTask {
		task.State = ReplicationTaskRunning
		task.Attempts++
		task.UpdatedAt = q.now().UTC()
		return task
	})
}

// MarkDone marks a task as successfully copied.
func (q *DiskReplicationTaskQueue) MarkDone(id string) (ReplicationTask, error) {
	return q.updateTask(id, func(task ReplicationTask) ReplicationTask {
		task.State = ReplicationTaskDone
		task.UpdatedAt = q.now().UTC()
		return task
	})
}

// MarkFailed marks a task as failed and eligible for retry after restart.
func (q *DiskReplicationTaskQueue) MarkFailed(id string) (ReplicationTask, error) {
	return q.updateTask(id, func(task ReplicationTask) ReplicationTask {
		task.State = ReplicationTaskFailed
		task.UpdatedAt = q.now().UTC()
		return task
	})
}

func (q *DiskReplicationTaskQueue) updateTask(id string, update func(ReplicationTask) ReplicationTask) (ReplicationTask, error) {
	var task ReplicationTask
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationTasksBucket)
		raw := b.Get([]byte(id))
		if raw == nil {
			return ErrTaskNotFound
		}

		var err error
		task, err = decodeReplicationTask(raw)
		if err != nil {
			return err
		}

		task = update(task)
		return putReplicationTask(b, id, task)
	})
	return task, err
}

func decodeReplicationTask(raw []byte) (ReplicationTask, error) {
	var task ReplicationTask
	if err := json.Unmarshal(raw, &task); err != nil {
		return ReplicationTask{}, err
	}
	return task, nil
}

func putReplicationTask(b *bolt.Bucket, id string, task ReplicationTask) error {
	raw, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return b.Put([]byte(id), raw)
}
