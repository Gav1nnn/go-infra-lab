package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

var ErrChecksumMismatch = errors.New("object checksum mismatch")

// ObjectStore stores one versioned object on a storage node.
type ObjectStore interface {
	WriteObject(nodeID, key string, version uint64, r io.Reader) (int64, error)
	ReadObject(nodeID, key string, version uint64) (int64, io.ReadCloser, error)
	DeleteObject(nodeID, key string, version uint64) error
	HasObject(nodeID, key string, version uint64) bool
}

// LocalObjectStore adapts Store to versioned object operations.
type LocalObjectStore struct {
	store *Store
}

// NewLocalObjectStore creates an object store backed by the local CAS store.
func NewLocalObjectStore(store *Store) *LocalObjectStore {
	return &LocalObjectStore{store: store}
}

// WriteObject writes a versioned object under the given node ID.
func (s *LocalObjectStore) WriteObject(nodeID, key string, version uint64, r io.Reader) (int64, error) {
	return s.store.Write(nodeID, versionedObjectKey(key, version), r)
}

// ReadObject opens a versioned object under the given node ID.
func (s *LocalObjectStore) ReadObject(nodeID, key string, version uint64) (int64, io.ReadCloser, error) {
	return s.store.Read(nodeID, versionedObjectKey(key, version))
}

// DeleteObject removes a versioned object under the given node ID.
func (s *LocalObjectStore) DeleteObject(nodeID, key string, version uint64) error {
	return s.store.Delete(nodeID, versionedObjectKey(key, version))
}

// HasObject checks whether a versioned object exists locally.
func (s *LocalObjectStore) HasObject(nodeID, key string, version uint64) bool {
	return s.store.Has(nodeID, versionedObjectKey(key, version))
}

// LocalObjectReplicator copies objects between local node directories.
type LocalObjectReplicator struct {
	objects ObjectStore
}

// NewLocalObjectReplicator creates a local replicator for tests and single-process demos.
func NewLocalObjectReplicator(objects ObjectStore) *LocalObjectReplicator {
	return &LocalObjectReplicator{objects: objects}
}

// Replicate copies one object version from source node to target node.
func (r *LocalObjectReplicator) Replicate(task ReplicationTask) error {
	_, src, err := r.objects.ReadObject(task.Source, task.Key, task.Version)
	if err != nil {
		return err
	}
	defer src.Close()

	h := sha256.New()
	tee := io.TeeReader(src, h)
	if _, err := r.objects.WriteObject(task.Target, task.Key, task.Version, tee); err != nil {
		return err
	}

	checksum := hex.EncodeToString(h.Sum(nil))
	if task.Checksum != "" && checksum != task.Checksum {
		r.objects.DeleteObject(task.Target, task.Key, task.Version)
		return ErrChecksumMismatch
	}
	return nil
}

func versionedObjectKey(key string, version uint64) string {
	return fmt.Sprintf("%s.v%d", key, version)
}

func checksumBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
