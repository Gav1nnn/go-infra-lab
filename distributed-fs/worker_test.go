package main

import (
	"errors"
	"testing"
)

func TestReplicationWorkerNoPendingTasks(t *testing.T) {
	c := newTestCoordinator()
	w := NewReplicationWorker(c, &fakeReplicator{})

	if _, err := w.WorkOnce(); err != ErrNoPendingTasks {
		t.Fatalf("have err %v want ErrNoPendingTasks", err)
	}
}

func TestReplicationWorkerSuccess(t *testing.T) {
	c := newTestCoordinator()
	c.RegisterNode("node1", ":3000")
	c.RegisterNode("node2", ":5000")

	plan, err := c.BeginWrite("foo.txt", 12, "checksum")
	if err != nil {
		t.Fatal(err)
	}

	replicator := &fakeReplicator{}
	w := NewReplicationWorker(c, replicator)

	task, err := w.WorkOnce()
	if err != nil {
		t.Fatal(err)
	}
	if task.State != ReplicationTaskDone {
		t.Fatalf("have task state %s want done", task.State)
	}
	if task.Attempts != 1 {
		t.Fatalf("have attempts %d want 1", task.Attempts)
	}
	if len(replicator.tasks) != 1 {
		t.Fatalf("have %d copied tasks want 1", len(replicator.tasks))
	}

	meta, ok, err := c.metadata.GetFile("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("metadata should exist")
	}
	if meta.Replicas[plan.Tasks[0].Target].State != ReplicaHealthy {
		t.Fatalf("target replica should be healthy")
	}
	pending, err := c.PendingTasks()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending tasks should be empty")
	}
}

func TestReplicationWorkerFailure(t *testing.T) {
	c := newTestCoordinator()
	c.RegisterNode("node1", ":3000")
	c.RegisterNode("node2", ":5000")

	plan, err := c.BeginWrite("foo.txt", 12, "checksum")
	if err != nil {
		t.Fatal(err)
	}

	copyErr := errors.New("copy failed")
	w := NewReplicationWorker(c, &fakeReplicator{err: copyErr})

	task, err := w.WorkOnce()
	if err != copyErr {
		t.Fatalf("have err %v want copyErr", err)
	}
	if task.State != ReplicationTaskPending {
		t.Fatalf("have task state %s want pending", task.State)
	}
	if task.LastError != copyErr.Error() {
		t.Fatalf("have last error %q want %q", task.LastError, copyErr.Error())
	}
	if task.RunAfter.IsZero() {
		t.Fatalf("run_after should be set after failure")
	}

	meta, ok, err := c.metadata.GetFile("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("metadata should exist")
	}
	if meta.Replicas[plan.Tasks[0].Target].State != ReplicaMissing {
		t.Fatalf("target replica should be missing")
	}
	pending, err := c.PendingTasks()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending tasks should be empty")
	}
}

type fakeReplicator struct {
	tasks []ReplicationTask
	err   error
}

func (r *fakeReplicator) Replicate(task ReplicationTask) error {
	r.tasks = append(r.tasks, task)
	return r.err
}
