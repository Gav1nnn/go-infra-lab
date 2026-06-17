package main

import (
	"testing"
	"time"
)

func TestMetadataCoordinatorBeginWrite(t *testing.T) {
	c := newTestCoordinator()
	c.RegisterNode("node3", ":7000")
	c.RegisterNode("node1", ":3000")
	c.RegisterNode("node2", ":5000")

	plan, err := c.BeginWrite("foo.txt", 12, "checksum")
	if err != nil {
		t.Fatal(err)
	}

	if plan.Primary.ID != "node1" {
		t.Fatalf("have primary %s want node1", plan.Primary.ID)
	}
	if plan.Metadata.Version != 1 {
		t.Fatalf("have version %d want 1", plan.Metadata.Version)
	}
	if plan.Metadata.Replicas["node1"].State != ReplicaHealthy {
		t.Fatalf("primary replica should be healthy")
	}
	if plan.Metadata.Replicas["node2"].State != ReplicaPending {
		t.Fatalf("secondary replica should be pending")
	}
	if len(plan.Tasks) != 2 {
		t.Fatalf("have %d tasks want 2", len(plan.Tasks))
	}
	if len(c.PendingTasks()) != 2 {
		t.Fatalf("have %d pending tasks want 2", len(c.PendingTasks()))
	}
}

func TestMetadataCoordinatorBeginWriteNoHealthyNodes(t *testing.T) {
	c := newTestCoordinator()

	if _, err := c.BeginWrite("foo.txt", 12, "checksum"); err != ErrNoHealthyNodes {
		t.Fatalf("have err %v want ErrNoHealthyNodes", err)
	}
}

func TestMetadataCoordinatorFinishReplication(t *testing.T) {
	c := newTestCoordinator()
	c.RegisterNode("node1", ":3000")
	c.RegisterNode("node2", ":5000")

	plan, err := c.BeginWrite("foo.txt", 12, "checksum")
	if err != nil {
		t.Fatal(err)
	}

	meta, task, err := c.FinishReplication(plan.Tasks[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != ReplicationTaskDone {
		t.Fatalf("have task state %s want done", task.State)
	}
	if meta.Replicas[task.Target].State != ReplicaHealthy {
		t.Fatalf("target replica should be healthy")
	}
}

func TestMetadataCoordinatorFailReplication(t *testing.T) {
	c := newTestCoordinator()
	c.RegisterNode("node1", ":3000")
	c.RegisterNode("node2", ":5000")

	plan, err := c.BeginWrite("foo.txt", 12, "checksum")
	if err != nil {
		t.Fatal(err)
	}

	meta, task, err := c.FailReplication(plan.Tasks[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != ReplicationTaskFailed {
		t.Fatalf("have task state %s want failed", task.State)
	}
	if meta.Replicas[task.Target].State != ReplicaMissing {
		t.Fatalf("target replica should be missing")
	}
}

func TestMetadataCoordinatorReadCandidates(t *testing.T) {
	c := newTestCoordinator()
	c.RegisterNode("node1", ":3000")
	c.RegisterNode("node2", ":5000")

	plan, err := c.BeginWrite("foo.txt", 12, "checksum")
	if err != nil {
		t.Fatal(err)
	}

	read, err := c.ReadCandidates("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if read.Replicas[0].NodeID != "node1" {
		t.Fatalf("have first replica %s want node1", read.Replicas[0].NodeID)
	}

	if _, _, err := c.FinishReplication(plan.Tasks[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.metadata.MarkReplica("foo.txt", "node1", ReplicaMissing); err != nil {
		t.Fatal(err)
	}

	read, err = c.ReadCandidates("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if read.Replicas[0].NodeID != "node2" {
		t.Fatalf("have first replica %s want node2", read.Replicas[0].NodeID)
	}
}

func TestMetadataCoordinatorDeleteFile(t *testing.T) {
	c := newTestCoordinator()
	c.RegisterNode("node1", ":3000")

	if _, err := c.BeginWrite("foo.txt", 12, "checksum"); err != nil {
		t.Fatal(err)
	}
	meta, err := c.DeleteFile("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Deleted {
		t.Fatalf("file should be deleted")
	}

	if _, err := c.ReadCandidates("foo.txt"); err != ErrFileDeleted {
		t.Fatalf("have err %v want ErrFileDeleted", err)
	}
}

func TestMetadataCoordinatorPlanRepair(t *testing.T) {
	c := newTestCoordinator()
	c.RegisterNode("node1", ":3000")
	c.RegisterNode("node2", ":5000")

	plan, err := c.BeginWrite("foo.txt", 12, "checksum")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.FailReplication(plan.Tasks[0].ID); err != nil {
		t.Fatal(err)
	}

	tasks, err := c.PlanRepair()
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("have %d repair tasks want 1", len(tasks))
	}
	if tasks[0].Target != "node2" {
		t.Fatalf("have target %s want node2", tasks[0].Target)
	}
}

func newTestCoordinator() *MetadataCoordinator {
	store := NewMetadataStore()
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	c := NewMetadataCoordinatorWithBackend(3, NewMemoryMetadataBackend(store))
	c.tasks.now = func() time.Time { return now }
	return c
}
