package main

import "errors"

var ErrNoPendingTasks = errors.New("no pending replication tasks")

// ObjectReplicator copies one object version from source node to target node.
type ObjectReplicator interface {
	Replicate(ReplicationTask) error
}

// ReplicationWorker consumes async replication tasks.
type ReplicationWorker struct {
	coordinator *MetadataCoordinator
	replicator  ObjectReplicator
}

// NewReplicationWorker creates a worker with a copy implementation.
func NewReplicationWorker(coordinator *MetadataCoordinator, replicator ObjectReplicator) *ReplicationWorker {
	return &ReplicationWorker{
		coordinator: coordinator,
		replicator:  replicator,
	}
}

// WorkOnce runs one pending replication task.
func (w *ReplicationWorker) WorkOnce() (ReplicationTask, error) {
	pending, err := w.coordinator.PendingTasks()
	if err != nil {
		return ReplicationTask{}, err
	}
	if len(pending) == 0 {
		return ReplicationTask{}, ErrNoPendingTasks
	}

	task, err := w.coordinator.StartReplication(pending[0].ID)
	if err != nil {
		return ReplicationTask{}, err
	}

	if err := w.replicator.Replicate(task); err != nil {
		_, failedTask, failErr := w.coordinator.FailReplication(task.ID)
		if failErr != nil {
			return ReplicationTask{}, failErr
		}
		return failedTask, err
	}

	_, doneTask, err := w.coordinator.FinishReplication(task.ID)
	if err != nil {
		return ReplicationTask{}, err
	}
	return doneTask, nil
}
