package zipserver

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

// GetFile implements Storage.GetFile for FsStorage
func (fs *MemStorage) GetFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)

	if obj, ok := fs.objects[objectPath]; ok {
		return io.NopCloser(bytes.NewReader(obj.data)), nil
	}

	err := fmt.Errorf("%s: object not found", objectPath)
	return nil, errors.Wrap(err, 0)
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

// PutFile implements Storage.PutFile for FsStorage
func (fs *MemStorage) PutFile(ctx context.Context, bucket, key string, contents io.Reader, mimeType string) error {
	return fs.PutFileWithSetup(ctx, bucket, key, contents, func(req *http.Request) error {
		req.Header.Set("Content-Type", mimeType)
		return nil
	})
}

// PutFileWithSetup implements Storage.PutFileWithSetup for FsStorage
func (fs *MemStorage) PutFileWithSetup(ctx context.Context, bucket, key string, contents io.Reader, setup StorageSetupFunc) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)
	if _, ok := fs.failingPaths[objectPath]; ok {
		return errors.Wrap(errors.New("intentional failure"), 0)
	}

	time.Sleep(fs.putDelay)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://127.0.0.1/dummy", nil)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	err = setup(req)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	data, err := io.ReadAll(contents)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	fs.objects[objectPath] = memObject{
		data,
		req.Header,
	}

	return nil
}

// DeleteFile implements Storage.DeleteFile for FsStorage
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
