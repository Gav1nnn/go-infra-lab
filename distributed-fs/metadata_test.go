package main

import (
	"testing"
	"time"
)

func TestMetadataStoreNodeHeartbeat(t *testing.T) {
	m := NewMetadataStore()
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	m.UpsertNode("node1", ":3000")
	m.UpsertNode("node2", ":5000")

	nodes := m.HealthyNodes()
	if len(nodes) != 2 {
		t.Fatalf("have %d healthy nodes want 2", len(nodes))
	}

	m.now = func() time.Time { return now.Add(30 * time.Second) }
	m.MarkExpiredNodes(10 * time.Second)

	nodes = m.HealthyNodes()
	if len(nodes) != 0 {
		t.Fatalf("have %d healthy nodes want 0", len(nodes))
	}
}

func TestMetadataStoreBeginFileVersion(t *testing.T) {
	m := NewMetadataStore()
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	meta := m.BeginFileVersion(
		"picture.png",
		128,
		"checksum",
		"node1",
		[]string{"node1", "node2", "node3"},
	)

	if meta.Version != 1 {
		t.Fatalf("have version %d want 1", meta.Version)
	}
	if meta.Primary != "node1" {
		t.Fatalf("have primary %s want node1", meta.Primary)
	}
	if meta.Replicas["node1"].State != ReplicaHealthy {
		t.Fatalf("primary replica should be healthy")
	}
	if meta.Replicas["node2"].State != ReplicaPending {
		t.Fatalf("secondary replica should be pending")
	}

	next := m.BeginFileVersion(
		"picture.png",
		256,
		"checksum2",
		"node2",
		[]string{"node2", "node3"},
	)
	if next.Version != 2 {
		t.Fatalf("have version %d want 2", next.Version)
	}
	if next.CreatedAt != meta.CreatedAt {
		t.Fatalf("createdAt should be preserved across versions")
	}
}

func TestMetadataStoreReplicaState(t *testing.T) {
	m := NewMetadataStore()
	m.BeginFileVersion("foo.txt", 10, "checksum", "node1", []string{"node1", "node2"})

	meta, err := m.MarkReplica("foo.txt", "node2", ReplicaHealthy)
	if err != nil {
		t.Fatal(err)
	}

	replicas := meta.HealthyReplicas()
	if len(replicas) != 2 {
		t.Fatalf("have %d healthy replicas want 2", len(replicas))
	}

	if _, err := m.MarkReplica("foo.txt", "node3", ReplicaHealthy); err != ErrReplicaNotFound {
		t.Fatalf("have err %v want ErrReplicaNotFound", err)
	}
}

func TestMetadataStoreTombstone(t *testing.T) {
	m := NewMetadataStore()
	m.BeginFileVersion("foo.txt", 10, "checksum", "node1", []string{"node1", "node2"})

	meta, err := m.Tombstone("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Deleted {
		t.Fatalf("file should be marked deleted")
	}
	if meta.Version != 2 {
		t.Fatalf("have version %d want 2", meta.Version)
	}
	for _, replica := range meta.Replicas {
		if replica.State != ReplicaDeleted {
			t.Fatalf("have replica state %s want deleted", replica.State)
		}
	}
}
