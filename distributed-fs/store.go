package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const defaultRootFolderName = "dfs-data"

// CASPathTransformFunc converts a key into a content-addressable storage path.
// It SHA-1 hashes the key, then splits the hex string into 5-char segments
// to create a nested directory structure (e.g. "abc12/3def4/...").
func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize := 5
	sliceLen := len(hashStr) / blocksize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blocksize, (i*blocksize)+blocksize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		FileName: hashStr,
	}
}

// PathTransformFunc is a function type that maps a key to a PathKey.
type PathTransformFunc func(string) PathKey

// PathKey holds the transformed file path components.
type PathKey struct {
	PathName string
	FileName string
}

// FullPath returns the complete relative path: "PathName/FileName".
func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.FileName)
}

// FirstPathName returns the top-level directory name from the path tree.
func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName, "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

// StoreOpts configures the Store.
type StoreOpts struct {
	Root              string
	PathTransformFunc PathTransformFunc
}

// DefaultPathTransformFunc is a fallback that uses the key itself as the path.
var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		FileName: key,
	}
}

// Store handles local file storage on disk with CAS path layout.
type Store struct {
	StoreOpts
}

// NewStore creates a new Store with the given options.
func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}

	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}

	return &Store{
		StoreOpts: opts,
	}
}

// Has checks if a file with the given key exists on disk.
func (s *Store) Has(id string, key string) bool {
	pathkey := s.PathTransformFunc(key)

	_, err := os.Stat(filepath.Join(s.Root, id, pathkey.FullPath()))
	return !errors.Is(err, os.ErrNotExist)
}

// Clear removes the entire storage root directory.
func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

// Delete removes a file and cleans up empty parent directories.
func (s *Store) Delete(id string, key string) error {
	pathkey := s.PathTransformFunc(key)
	fullPath := filepath.Join(s.Root, id, pathkey.FullPath())

	if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return s.removeEmptyParents(filepath.Dir(fullPath), filepath.Join(s.Root, id))
}

// Write stores data from a reader to disk under the given id and key.
func (s *Store) Write(id string, key string, r io.Reader) (int64, error) {
	return s.writeStream(id, key, r)
}

// WriteDecrypt reads encrypted data, decrypts it, and writes to disk.
func (s *Store) WriteDecrypt(encKey []byte, id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := copyDecrypt(encKey, r, f)
	return int64(n), err
}

// openFileForWriting creates all parent directories and opens the file for writing.
func (s *Store) openFileForWriting(id string, key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)

	pathNameWithRoot := filepath.Join(s.Root, id, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	return os.Create(filepath.Join(s.Root, id, pathKey.FullPath()))
}

// writeStream writes a data stream directly to a file on disk.
func (s *Store) writeStream(id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(f, r)
}

// Read opens a file and returns its size and a ReadCloser.
func (s *Store) Read(id string, key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)

	file, err := os.Open(filepath.Join(s.Root, id, pathKey.FullPath()))
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), file, nil
}

func (s *Store) removeEmptyParents(dir, stop string) error {
	for dir != stop && strings.HasPrefix(dir, stop) {
		if err := os.Remove(dir); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST) {
				return nil
			}
			return err
		}
		dir = filepath.Dir(dir)
	}
	return nil
}
