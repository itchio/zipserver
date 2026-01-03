package zipserver

// MemStorage implements Storage interface in memory, storing objects in a map.
// This is used for the serving of a zip file over http. Keep in mind extracted
// zips are stored forever, this should only be used for testing or one-off use

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	errors "github.com/go-errors/errors"
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

// NewMemStorage creates a new fs storage working in the given directory
func NewMemStorage() (*MemStorage, error) {
	return &MemStorage{
		objects:      make(map[string]memObject),
		failingPaths: make(map[string]struct{}),
	}, nil
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

	err := fmt.Errorf("%s: object not found", objectPath)
	return nil, nil, errors.Wrap(err, 0)
}

func (fs *MemStorage) getHeaders(bucket, key string) (http.Header, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)

	if obj, ok := fs.objects[objectPath]; ok {
		return obj.headers, nil
	}

	err := fmt.Errorf("%s: object not found", objectPath)
	return nil, errors.Wrap(err, 0)
}

func (fs *MemStorage) PutFile(ctx context.Context, bucket, key string, contents io.Reader, opts PutOptions) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)
	if _, ok := fs.failingPaths[objectPath]; ok {
		return errors.Wrap(errors.New("intentional failure"), 0)
	}

	time.Sleep(fs.putDelay)

	data, err := io.ReadAll(contents)
	if err != nil {
		return errors.Wrap(err, 0)
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

	return nil
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
