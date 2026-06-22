package main

import (
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestDiskReplicationTaskQueuePersistenceAndRestore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewDiskReplicationTaskQueue(db)
	if err != nil {
		t.Fatal(err)
	}
	q.now = func() time.Time { return now }

	tasks := []ReplicationTask{
		testReplicationTask("done", ReplicationTaskDone),
		testReplicationTask("failed", ReplicationTaskFailed),
		testReplicationTask("pending", ReplicationTaskPending),
		testReplicationTask("running", ReplicationTaskRunning),
	}
	if err := q.Enqueue(tasks...); err != nil {
		t.Fatal(err)
	}
	if _, err := q.MarkRunning("running"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.MarkFailed("failed", errTestCopyFailed); err != nil {
		t.Fatal(err)
	}
	if _, err := q.MarkDone("done"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := NewDiskReplicationTaskQueue(reopened)
	if err != nil {
		t.Fatal(err)
	}

	pending, err := recovered.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 3 {
		t.Fatalf("have %d pending tasks want 3", len(pending))
	}
	if pending[0].ID != "pending" || pending[1].ID != "running" || pending[2].ID != "failed" {
		t.Fatalf("unexpected pending tasks: %+v", pending)
	}
}

func testReplicationTask(id string, state ReplicationTaskState) ReplicationTask {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	return ReplicationTask{
		ID:        id,
		Key:       "foo.txt",
		Version:   1,
		Checksum:  "checksum",
		Source:    "node1",
		Target:    "node2",
		State:     state,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
