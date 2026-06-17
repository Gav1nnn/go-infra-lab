package main

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestLocalObjectStoreWriteRead(t *testing.T) {
	objects := newTestObjectStore(t)
	data := []byte("hello object")

	n, err := objects.WriteObject("node1", "foo.txt", 1, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(data)) {
		t.Fatalf("have written %d want %d", n, len(data))
	}
	if !objects.HasObject("node1", "foo.txt", 1) {
		t.Fatalf("object should exist")
	}

	size, r, err := objects.ReadObject("node1", "foo.txt", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Fatalf("have size %d want %d", size, len(data))
	}
	if string(got) != string(data) {
		t.Fatalf("have %s want %s", got, data)
	}
}

func TestLocalObjectReplicatorReplicate(t *testing.T) {
	objects := newTestObjectStore(t)
	data := []byte("replicated object")

	if _, err := objects.WriteObject("node1", "foo.txt", 1, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	replicator := NewLocalObjectReplicator(objects)
	task := ReplicationTask{
		Key:      "foo.txt",
		Version:  1,
		Checksum: checksumBytes(data),
		Source:   "node1",
		Target:   "node2",
	}
	if err := replicator.Replicate(task); err != nil {
		t.Fatal(err)
	}
	if !objects.HasObject("node2", "foo.txt", 1) {
		t.Fatalf("target object should exist")
	}

	_, r, err := objects.ReadObject("node2", "foo.txt", 1)
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
}

func TestLocalObjectReplicatorChecksumMismatch(t *testing.T) {
	objects := newTestObjectStore(t)
	data := []byte("replicated object")

	if _, err := objects.WriteObject("node1", "foo.txt", 1, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	replicator := NewLocalObjectReplicator(objects)
	task := ReplicationTask{
		Key:      "foo.txt",
		Version:  1,
		Checksum: "bad-checksum",
		Source:   "node1",
		Target:   "node2",
	}
	if err := replicator.Replicate(task); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("have err %v want ErrChecksumMismatch", err)
	}
	if objects.HasObject("node2", "foo.txt", 1) {
		t.Fatalf("target object should be removed after checksum mismatch")
	}
}

func TestReplicationWorkerWithLocalObjectReplicator(t *testing.T) {
	c := newTestCoordinator()

	c.RegisterNode("node1", ":3000")
	c.RegisterNode("node2", ":5000")

	data := []byte("single process replication")
	plan, err := c.BeginWrite("foo.txt", int64(len(data)), checksumBytes(data))
	if err != nil {
		t.Fatal(err)
	}

	objects := newTestObjectStore(t)
	if _, err := objects.WriteObject(plan.Primary.ID, "foo.txt", plan.Metadata.Version, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	worker := NewReplicationWorker(c, NewLocalObjectReplicator(objects))
	task, err := worker.WorkOnce()
	if err != nil {
		t.Fatal(err)
	}
	if task.State != ReplicationTaskDone {
		t.Fatalf("have task state %s want done", task.State)
	}
	if !objects.HasObject(task.Target, "foo.txt", plan.Metadata.Version) {
		t.Fatalf("target object should exist")
	}

	meta, ok, err := c.metadata.GetFile("foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("metadata should exist")
	}
	if meta.Replicas[task.Target].State != ReplicaHealthy {
		t.Fatalf("target replica should be healthy")
	}
}

func newTestObjectStore(t *testing.T) *LocalObjectStore {
	t.Helper()

	store := NewStore(StoreOpts{
		Root:              t.TempDir(),
		PathTransformFunc: CASPathTransformFunc,
	})
	return NewLocalObjectStore(store)
}
