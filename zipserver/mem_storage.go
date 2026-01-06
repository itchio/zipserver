package zipserver

// MemStorage implements Storage interface in memory, storing objects in a map.
// This is used for the serving of a zip file over http. Keep in mind extracted
// zips are stored forever, this should only be used for testing or one-off use

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type memObject struct {
	data    []byte
	headers http.Header
}

// MemStorage implements Storage on a directory
// it stores things in `baseDir/bucket/prefix...`
type MemStorage struct {
	mutex        sync.Mutex
	objects      map[string]memObject
	failingPaths map[string]struct{}
	putDelay     time.Duration
}

// interface guard
var _ Storage = (*MemStorage)(nil)

// namedMemStorages is a registry of named MemStorage instances for testing
var (
	namedMemStorages      = make(map[string]*MemStorage)
	namedMemStoragesMutex sync.Mutex
)

// NewMemStorage creates a new MemStorage instance
func NewMemStorage() (*MemStorage, error) {
	return &MemStorage{
		objects:      make(map[string]memObject),
		failingPaths: make(map[string]struct{}),
	}, nil
}

// GetNamedMemStorage returns a shared MemStorage instance by name, creating it if needed.
// This allows tests to access the same storage instance that operations use.
func GetNamedMemStorage(name string) *MemStorage {
	namedMemStoragesMutex.Lock()
	defer namedMemStoragesMutex.Unlock()

	if storage, ok := namedMemStorages[name]; ok {
		return storage
	}

	storage := &MemStorage{
		objects:      make(map[string]memObject),
		failingPaths: make(map[string]struct{}),
	}
	namedMemStorages[name] = storage
	return storage
}

// ClearNamedMemStorage removes a named storage instance from the registry
func ClearNamedMemStorage(name string) {
	namedMemStoragesMutex.Lock()
	defer namedMemStoragesMutex.Unlock()
	delete(namedMemStorages, name)
}

// ClearAllNamedMemStorages removes all named storage instances from the registry
func ClearAllNamedMemStorages() {
	namedMemStoragesMutex.Lock()
	defer namedMemStoragesMutex.Unlock()
	namedMemStorages = make(map[string]*MemStorage)
}

func (fs *MemStorage) objectPath(bucket, key string) string {
	return fmt.Sprintf("%s/%s", bucket, key)
}

func (fs *MemStorage) GetFile(ctx context.Context, bucket, key string) (io.ReadCloser, http.Header, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)

	if obj, ok := fs.objects[objectPath]; ok {
		return io.NopCloser(bytes.NewReader(obj.data)), obj.headers, nil
	}

	return nil, nil, fmt.Errorf("%s: object not found", objectPath)
}

// memReaderAt wraps bytes.Reader to implement ReaderAtCloser with read limits
type memReaderAt struct {
	*bytes.Reader
	maxBytes  uint64
	bytesRead uint64
}

func (r *memReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	toRead := uint64(len(p))
	if r.maxBytes > 0 && r.bytesRead+toRead > r.maxBytes {
		return 0, fmt.Errorf("max read limit exceeded (%d bytes)", r.maxBytes)
	}
	n, err = r.Reader.ReadAt(p, off)
	r.bytesRead += uint64(n)
	return n, err
}

func (r *memReaderAt) Close() error {
	return nil
}

func (r *memReaderAt) BytesRead() uint64 {
	return r.bytesRead
}

func (fs *MemStorage) GetReaderAt(ctx context.Context, bucket, key string, maxBytes uint64) (ReaderAtCloser, int64, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)

	if obj, ok := fs.objects[objectPath]; ok {
		return &memReaderAt{Reader: bytes.NewReader(obj.data), maxBytes: maxBytes}, int64(len(obj.data)), nil
	}

	return nil, 0, fmt.Errorf("%s: object not found", objectPath)
}

// HeadFile retrieves metadata headers for a file without downloading the body
func (fs *MemStorage) HeadFile(ctx context.Context, bucket, key string) (http.Header, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)

	if obj, ok := fs.objects[objectPath]; ok {
		headers := obj.headers.Clone()
		headers.Set("Content-Length", fmt.Sprintf("%d", len(obj.data)))
		return headers, nil
	}

	return nil, fmt.Errorf("%s: object not found", objectPath)
}

func (fs *MemStorage) PutFile(ctx context.Context, bucket, key string, contents io.Reader, opts PutOptions) (PutResult, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)
	if _, ok := fs.failingPaths[objectPath]; ok {
		return PutResult{}, errors.New("intentional failure")
	}

	time.Sleep(fs.putDelay)

	data, err := io.ReadAll(contents)
	if err != nil {
		return PutResult{}, err
	}

	// Build headers from options for test verification
	headers := http.Header{}
	if opts.ContentType != "" {
		headers.Set("Content-Type", opts.ContentType)
	}
	if opts.ContentEncoding != "" {
		headers.Set("Content-Encoding", opts.ContentEncoding)
	}
	if opts.ContentDisposition != "" {
		headers.Set("Content-Disposition", opts.ContentDisposition)
	}
	if opts.ACL != "" {
		headers.Set("x-acl", string(opts.ACL))
	}

	fs.objects[objectPath] = memObject{
		data,
		headers,
	}

	// Compute MD5 checksum
	checksum := md5.Sum(data)

	return PutResult{
		MD5: fmt.Sprintf("%x", checksum),
	}, nil
}

func (fs *MemStorage) DeleteFile(ctx context.Context, bucket, key string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	delete(fs.objects, fs.objectPath(bucket, key))
	return nil
}

func (fs *MemStorage) planForFailure(bucket, key string) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)

	fs.failingPaths[objectPath] = struct{}{}
}
