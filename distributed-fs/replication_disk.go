package main

import (
	"encoding/json"
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
			task.LeaseUntil = time.Time{}
			task.RunAfter = time.Time{}
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
	now := q.now().UTC()
	err := q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationTasksBucket)
		return b.ForEach(func(_, v []byte) error {
			task, err := decodeReplicationTask(v)
			if err != nil {
				return err
			}
			if task.State == ReplicationTaskPending && readyToRun(task, now) {
				tasks = append(tasks, task)
			}
			return nil
		})
	})
	sortReplicationTasks(tasks)
	return tasks, err
}

// MarkRunning marks a task as currently being copied.
func (q *DiskReplicationTaskQueue) MarkRunning(id string) (ReplicationTask, error) {
	return q.updateTask(id, func(task ReplicationTask) ReplicationTask {
		now := q.now().UTC()
		task.State = ReplicationTaskRunning
		task.Attempts++
		task.LeaseUntil = now.Add(defaultReplicationTaskLease)
		task.UpdatedAt = now
		return task
	})
}

// MarkDone marks a task as successfully copied.
func (q *DiskReplicationTaskQueue) MarkDone(id string) (ReplicationTask, error) {
	return q.updateTask(id, func(task ReplicationTask) ReplicationTask {
		task.State = ReplicationTaskDone
		task.LeaseUntil = time.Time{}
		task.RunAfter = time.Time{}
		task.LastError = ""
		task.UpdatedAt = q.now().UTC()
		return task
	})
}

// MarkFailed marks a task as failed and schedules a retry or marks it dead.
func (q *DiskReplicationTaskQueue) MarkFailed(id string, cause error) (ReplicationTask, error) {
	return q.updateTask(id, func(task ReplicationTask) ReplicationTask {
		return failReplicationTask(task, q.now().UTC(), cause)
	})
}

// RequeueExpiredRunning returns expired running tasks to pending or marks them dead.
func (q *DiskReplicationTaskQueue) RequeueExpiredRunning() ([]ReplicationTask, error) {
	now := q.now().UTC()
	requeued := []ReplicationTask{}
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationTasksBucket)
		return b.ForEach(func(k, v []byte) error {
			task, err := decodeReplicationTask(v)
			if err != nil {
				return err
			}
			if task.State != ReplicationTaskRunning || task.LeaseUntil.IsZero() || task.LeaseUntil.After(now) {
				return nil
			}
			task = expireReplicationTask(task, now)
			if err := putReplicationTask(b, string(k), task); err != nil {
				return err
			}
			requeued = append(requeued, task)
			return nil
		})
	})
	sortReplicationTasks(requeued)
	return requeued, err
}

// Stats returns task counts by state.
func (q *DiskReplicationTaskQueue) Stats() (map[ReplicationTaskState]int, error) {
	stats := make(map[ReplicationTaskState]int)
	err := q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationTasksBucket)
		return b.ForEach(func(_, v []byte) error {
			task, err := decodeReplicationTask(v)
			if err != nil {
				return err
			}
			stats[task.State]++
			return nil
		})
	})
	return stats, err
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
