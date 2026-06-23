package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDiskMetadataStorePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)

	store, err := OpenDiskMetadataStore(path)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	if _, err := store.UpsertNode("node1", ":3000"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertNode("node2", ":5000"); err != nil {
		t.Fatal(err)
	}

	meta, err := store.BeginFileVersion(
		"picture.png",
		128,
		"checksum",
		[]ChunkMetadata{{Index: 0, Offset: 0, Size: 128, Checksum: "chunk-checksum"}},
		"node1",
		[]string{"node1", "node2"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Replicas["node2"].State != ReplicaPending {
		t.Fatalf("secondary replica should be pending")
	}

	if _, err := store.MarkReplica("picture.png", "node2", ReplicaHealthy); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDiskMetadataStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	nodes, err := reopened.HealthyNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("have %d healthy nodes want 2", len(nodes))
	}

	got, ok, err := reopened.GetFile("picture.png")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("metadata should exist after reopen")
	}
	if got.Version != 1 {
		t.Fatalf("have version %d want 1", got.Version)
	}
	if got.Replicas["node2"].State != ReplicaHealthy {
		t.Fatalf("replica state should persist")
	}
	if len(got.Chunks) != 1 || got.Chunks[0].Checksum != "chunk-checksum" {
		t.Fatalf("chunk metadata should persist")
	}
}

func TestDiskMetadataStoreTombstonePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")

	store, err := OpenDiskMetadataStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginFileVersion("foo.txt", 10, "checksum", nil, "node1", []string{"node1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Tombstone("foo.txt"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDiskMetadataStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	meta, ok, err := reopened.GetFile("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("metadata should exist after reopen")
	}
	if !meta.Deleted {
		t.Fatalf("file should remain tombstoned")
	}
	if meta.Version != 2 {
		t.Fatalf("have version %d want 2", meta.Version)
	}
}

func TestDiskMetadataStoreMarkExpiredNodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	store, err := OpenDiskMetadataStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.UpsertNode("node1", ":3000"); err != nil {
		t.Fatal(err)
	}

	store.now = func() time.Time { return now.Add(30 * time.Second) }
	if err := store.MarkExpiredNodes(10 * time.Second); err != nil {
		t.Fatal(err)
	}

	nodes, err := store.HealthyNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("have %d healthy nodes want 0", len(nodes))
	}
}
