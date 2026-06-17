package main

import (
	"bytes"
	"io"
	"testing"
)

func TestManagedFileServicePutGet(t *testing.T) {
	svc := newTestManagedFileService(t, 3)
	svc.RegisterNode("node1", ":3000")
	svc.RegisterNode("node2", ":5000")
	svc.RegisterNode("node3", ":7000")

	data := []byte("hello managed service")
	meta, err := svc.Put("foo.txt", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if meta.Primary != "node1" {
		t.Fatalf("have primary %s want node1", meta.Primary)
	}
	if len(svc.PendingTasks()) != 2 {
		t.Fatalf("have %d pending tasks want 2", len(svc.PendingTasks()))
	}

	r, gotMeta, err := svc.Get("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("have %s want %s", got, data)
	}
	if gotMeta.Version != 1 {
		t.Fatalf("have version %d want 1", gotMeta.Version)
	}
}

func TestManagedFileServiceReplication(t *testing.T) {
	svc := newTestManagedFileService(t, 3)
	svc.RegisterNode("node1", ":3000")
	svc.RegisterNode("node2", ":5000")
	svc.RegisterNode("node3", ":7000")

	data := []byte("replicate through service")
	if _, err := svc.Put("foo.txt", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	task, err := svc.RunReplicationOnce()
	if err != nil {
		t.Fatal(err)
	}
	if task.State != ReplicationTaskDone {
		t.Fatalf("have task state %s want done", task.State)
	}

	meta, ok := svc.Metadata("foo.txt")
	if !ok {
		t.Fatalf("metadata should exist")
	}
	if meta.Replicas[task.Target].State != ReplicaHealthy {
		t.Fatalf("target replica should be healthy")
	}
}

func TestManagedFileServiceReadsReplicaWhenPrimaryMissing(t *testing.T) {
	svc := newTestManagedFileService(t, 2)
	svc.RegisterNode("node1", ":3000")
	svc.RegisterNode("node2", ":5000")

	data := []byte("read from secondary")
	meta, err := svc.Put("foo.txt", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RunReplicationOnce(); err != nil {
		t.Fatal(err)
	}

	if err := svc.objects.DeleteObject(meta.Primary, "foo.txt", meta.Version); err != nil {
		t.Fatal(err)
	}

	r, _, err := svc.Get("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("have %s want %s", got, data)
	}

	updated, ok := svc.Metadata("foo.txt")
	if !ok {
		t.Fatalf("metadata should exist")
	}
	if updated.Replicas[meta.Primary].State != ReplicaMissing {
		t.Fatalf("primary replica should be marked missing")
	}
}

func TestManagedFileServiceDelete(t *testing.T) {
	svc := newTestManagedFileService(t, 2)
	svc.RegisterNode("node1", ":3000")
	svc.RegisterNode("node2", ":5000")

	if _, err := svc.Put("foo.txt", bytes.NewReader([]byte("delete me"))); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Delete("foo.txt"); err != nil {
		t.Fatal(err)
	}

	if _, _, err := svc.Get("foo.txt"); err != ErrFileDeleted {
		t.Fatalf("have err %v want ErrFileDeleted", err)
	}
}

func newTestManagedFileService(t *testing.T, replicaCount int) *ManagedFileService {
	t.Helper()

	store := NewStore(StoreOpts{
		Root:              t.TempDir(),
		PathTransformFunc: CASPathTransformFunc,
	})
	return NewManagedFileService(replicaCount, NewLocalObjectStore(store))
}
