package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const defaultRootFolderName = "ggnetwork"

// CASPathTransformFunc converts a key into a content-addressable storage path.
// It SHA-1 hashes the key, then splits the hex string into 5-char segments
// to create a nested directory structure (e.g. "abc12/3def4/...").
func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))          // SHA-1 hash, 20 bytes
	hashStr := hex.EncodeToString(hash[:]) // 40-char hex string

	blocksize := 5
	sliceLen := len(hashStr) / blocksize // 40 / 5 = 8 segments
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blocksize, (i*blocksize)+blocksize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"), // e.g. "68044/29f74/181a6/..."
		FileName: hashStr,                  // e.g. "6804429f74181a63..."
	}
}

// PathTransformFunc is a function type that maps a key to a PathKey.
type PathTransformFunc func(string) PathKey

// PathKey holds the transformed file path components.
type PathKey struct {
	PathName string // nested directory path, e.g. "68044/29f74"
	FileName string // the full filename (hash), e.g. "6804429f74..."
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

// ======================================================================

// StoreOpts configures the Store.
type StoreOpts struct {
	Root              string            // root directory for all stored files
	PathTransformFunc PathTransformFunc // function to transform keys into paths
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
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathkey.FullPath())

	_, err := os.Stat(fullPathWithRoot)
	return !errors.Is(err, os.ErrNotExist)
}

// Clear removes the entire storage root directory.
func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

// Delete removes a file and its parent directory tree from disk.
func (s *Store) Delete(id string, key string) error {
	pathkey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathkey.FileName)
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathkey.FirstPathName())
	return os.RemoveAll(firstPathNameWithRoot)
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
	n, err := copyDecrypt(encKey, r, f)
	return int64(n), err
}

// openFileForWriting creates all parent directories and opens the file for writing.
func (s *Store) openFileForWriting(id string, key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)

	pathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())
	return os.Create(fullPathWithRoot)
}

// writeStream writes a data stream directly to a file on disk.
func (s *Store) writeStream(id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	return io.Copy(f, r)
}

// Read opens a file and returns its size and a ReadCloser.
func (s *Store) Read(id string, key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	file, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), file, nil
}
