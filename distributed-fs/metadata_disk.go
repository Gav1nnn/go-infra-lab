package main

import (
	"encoding/json"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	metadataFilesBucket = []byte("files")
	metadataNodesBucket = []byte("nodes")
)

// DiskMetadataStore persists authoritative metadata with bbolt.
type DiskMetadataStore struct {
	db  *bolt.DB
	now func() time.Time
}

// OpenDiskMetadataStore opens a bbolt-backed metadata store.
func OpenDiskMetadataStore(path string) (*DiskMetadataStore, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}

	store := &DiskMetadataStore{
		db:  db,
		now: time.Now,
	}
	if err := store.init(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

// Close closes the underlying bbolt database.
func (m *DiskMetadataStore) Close() error {
	return m.db.Close()
}

func (m *DiskMetadataStore) init() error {
	return m.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(metadataFilesBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(metadataNodesBucket)
		return err
	})
}

// UpsertNode creates or refreshes a storage node heartbeat.
func (m *DiskMetadataStore) UpsertNode(id, addr string) (NodeMetadata, error) {
	node := NodeMetadata{
		ID:            id,
		Addr:          addr,
		State:         NodeHealthy,
		LastHeartbeat: m.now().UTC(),
	}

	err := m.db.Update(func(tx *bolt.Tx) error {
		return putMetadataJSON(tx.Bucket(metadataNodesBucket), id, node)
	})
	return node, err
}

// MarkExpiredNodes marks nodes as down if their heartbeat is too old.
func (m *DiskMetadataStore) MarkExpiredNodes(ttl time.Duration) error {
	cutoff := m.now().UTC().Add(-ttl)
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataNodesBucket)
		return b.ForEach(func(k, v []byte) error {
			var node NodeMetadata
			if err := json.Unmarshal(v, &node); err != nil {
				return err
			}
			if node.LastHeartbeat.Before(cutoff) {
				node.State = NodeDown
				return putMetadataJSON(b, string(k), node)
			}
			return nil
		})
	})
}

// HealthyNodes returns nodes that can accept reads, writes, and repair work.
func (m *DiskMetadataStore) HealthyNodes() ([]NodeMetadata, error) {
	nodes := []NodeMetadata{}
	err := m.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataNodesBucket).ForEach(func(_, v []byte) error {
			var node NodeMetadata
			if err := json.Unmarshal(v, &node); err != nil {
				return err
			}
			if node.State == NodeHealthy {
				nodes = append(nodes, node)
			}
			return nil
		})
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes, err
}

// BeginFileVersion records a new primary replica and pending secondary replicas.
func (m *DiskMetadataStore) BeginFileVersion(key string, size int64, checksum, primary string, replicas []string) (FileMetadata, error) {
	var meta FileMetadata
	err := m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataFilesBucket)

		now := m.now().UTC()
		prev, ok, err := getFileFromBucket(b, key)
		if err != nil {
			return err
		}

		version := uint64(1)
		createdAt := now
		if ok {
			version = prev.Version + 1
			createdAt = prev.CreatedAt
		}

		meta = FileMetadata{
			Key:       key,
			Version:   version,
			Size:      size,
			Checksum:  checksum,
			Primary:   primary,
			Deleted:   false,
			Replicas:  make(map[string]ReplicaMetadata, len(replicas)),
			CreatedAt: createdAt,
			UpdatedAt: now,
		}

		for _, nodeID := range replicas {
			state := ReplicaPending
			if nodeID == primary {
				state = ReplicaHealthy
			}
			meta.Replicas[nodeID] = ReplicaMetadata{
				NodeID:    nodeID,
				State:     state,
				UpdatedAt: now,
			}
		}

		return putMetadataJSON(b, key, meta)
	})
	return cloneFileMetadata(meta), err
}

// MarkReplica updates a single replica state.
func (m *DiskMetadataStore) MarkReplica(key, nodeID string, state ReplicaState) (FileMetadata, error) {
	var meta FileMetadata
	err := m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataFilesBucket)

		var ok bool
		var err error
		meta, ok, err = getFileFromBucket(b, key)
		if err != nil {
			return err
		}
		if !ok {
			return ErrMetadataNotFound
		}

		replica, ok := meta.Replicas[nodeID]
		if !ok {
			return ErrReplicaNotFound
		}

		now := m.now().UTC()
		replica.State = state
		replica.UpdatedAt = now
		meta.Replicas[nodeID] = replica
		meta.UpdatedAt = now

		return putMetadataJSON(b, key, meta)
	})
	return cloneFileMetadata(meta), err
}

// Tombstone marks a file as deleted while preserving its version history.
func (m *DiskMetadataStore) Tombstone(key string) (FileMetadata, error) {
	var meta FileMetadata
	err := m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataFilesBucket)

		var ok bool
		var err error
		meta, ok, err = getFileFromBucket(b, key)
		if err != nil {
			return err
		}
		if !ok {
			return ErrMetadataNotFound
		}

		now := m.now().UTC()
		meta.Version++
		meta.Deleted = true
		meta.UpdatedAt = now
		for nodeID, replica := range meta.Replicas {
			replica.State = ReplicaDeleted
			replica.UpdatedAt = now
			meta.Replicas[nodeID] = replica
		}

		return putMetadataJSON(b, key, meta)
	})
	return cloneFileMetadata(meta), err
}

// GetFile returns one file metadata record.
func (m *DiskMetadataStore) GetFile(key string) (FileMetadata, bool, error) {
	var meta FileMetadata
	found := false
	err := m.db.View(func(tx *bolt.Tx) error {
		var err error
		meta, found, err = getFileFromBucket(tx.Bucket(metadataFilesBucket), key)
		return err
	})
	return cloneFileMetadata(meta), found, err
}

func getFileFromBucket(b *bolt.Bucket, key string) (FileMetadata, bool, error) {
	raw := b.Get([]byte(key))
	if raw == nil {
		return FileMetadata{}, false, nil
	}

	var meta FileMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return FileMetadata{}, false, err
	}
	return meta, true, nil
}

func putMetadataJSON(b *bolt.Bucket, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put([]byte(key), raw)
}
