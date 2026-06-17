package main

import (
	"testing"
	"time"
)

func TestReplicationPlannerPlan(t *testing.T) {
	planner := NewReplicationPlanner(3)
	nodes := []NodeMetadata{
		{ID: "node3", State: NodeHealthy},
		{ID: "node1", State: NodeHealthy},
		{ID: "node2", State: NodeHealthy},
		{ID: "node4", State: NodeDown},
	}

	plan, err := planner.Plan(nodes)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Primary.ID != "node1" {
		t.Fatalf("have primary %s want node1", plan.Primary.ID)
	}
	if len(plan.Replicas) != 3 {
		t.Fatalf("have %d replicas want 3", len(plan.Replicas))
	}
	if plan.Replicas[2].ID != "node3" {
		t.Fatalf("have replica %s want node3", plan.Replicas[2].ID)
	}
}

func TestReplicationPlannerPlanWithFewNodes(t *testing.T) {
	planner := NewReplicationPlanner(3)
	nodes := []NodeMetadata{
		{ID: "node1", State: NodeHealthy},
		{ID: "node2", State: NodeHealthy},
	}

	plan, err := planner.Plan(nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Replicas) != 2 {
		t.Fatalf("have %d replicas want 2", len(plan.Replicas))
	}
}

func TestReplicationPlannerNoHealthyNodes(t *testing.T) {
	planner := NewReplicationPlanner(3)
	nodes := []NodeMetadata{
		{ID: "node1", State: NodeDown},
	}

	if _, err := planner.Plan(nodes); err != ErrNoHealthyNodes {
		t.Fatalf("have err %v want ErrNoHealthyNodes", err)
	}
}

func TestTasksForFile(t *testing.T) {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	meta := FileMetadata{
		Key:      "foo.txt",
		Version:  2,
		Checksum: "checksum",
		Primary:  "node1",
		Replicas: map[string]ReplicaMetadata{
			"node1": {NodeID: "node1", State: ReplicaHealthy},
			"node2": {NodeID: "node2", State: ReplicaPending},
			"node3": {NodeID: "node3", State: ReplicaStale},
			"node4": {NodeID: "node4", State: ReplicaMissing},
			"node5": {NodeID: "node5", State: ReplicaHealthy},
		},
	}

	tasks, err := TasksForFile(meta, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Fatalf("have %d tasks want 3", len(tasks))
	}
	if tasks[0].Target != "node2" {
		t.Fatalf("have target %s want node2", tasks[0].Target)
	}
	if tasks[0].Source != "node1" {
		t.Fatalf("have source %s want node1", tasks[0].Source)
	}
	if tasks[0].State != ReplicationTaskPending {
		t.Fatalf("have state %s want pending", tasks[0].State)
	}
}

func TestTasksForFileMissingPrimary(t *testing.T) {
	meta := FileMetadata{
		Key:      "foo.txt",
		Version:  1,
		Primary:  "node1",
		Replicas: map[string]ReplicaMetadata{},
	}

	if _, err := TasksForFile(meta, time.Now()); err != ErrPrimaryUnavailable {
		t.Fatalf("have err %v want ErrPrimaryUnavailable", err)
	}
}

func TestReplicationTaskQueue(t *testing.T) {
	q := NewReplicationTaskQueue()
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	q.now = func() time.Time { return now }

	task := ReplicationTask{
		ID:        "task1",
		Key:       "foo.txt",
		Version:   1,
		Source:    "node1",
		Target:    "node2",
		State:     ReplicationTaskPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	q.Enqueue(task, task)

	pending := q.Pending()
	if len(pending) != 1 {
		t.Fatalf("have %d pending tasks want 1", len(pending))
	}

	running, err := q.MarkRunning("task1")
	if err != nil {
		t.Fatal(err)
	}
	if running.State != ReplicationTaskRunning {
		t.Fatalf("have state %s want running", running.State)
	}
	if running.Attempts != 1 {
		t.Fatalf("have attempts %d want 1", running.Attempts)
	}

	done, err := q.MarkDone("task1")
	if err != nil {
		t.Fatal(err)
	}
	if done.State != ReplicationTaskDone {
		t.Fatalf("have state %s want done", done.State)
	}

	if _, err := q.MarkDone("missing"); err != ErrTaskNotFound {
		t.Fatalf("have err %v want ErrTaskNotFound", err)
	}
}
