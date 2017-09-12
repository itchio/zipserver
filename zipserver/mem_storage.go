package zipserver

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
)

type memObject struct {
	data    []byte
	headers http.Header
}

// MemStorage implements Storage on a directory
// it stores things in `baseDir/bucket/prefix...`
type MemStorage struct {
	mutex   sync.Mutex
	objects map[string]memObject
}

// interface guard
var _ Storage = (*MemStorage)(nil)

// NewMemStorage creates a new fs storage working in the given directory
func NewMemStorage() (*MemStorage, error) {
	return &MemStorage{
		objects: make(map[string]memObject),
	}, nil
}

func (fs *MemStorage) objectPath(bucket, key string) string {
	return fmt.Sprintf("%s/%s", bucket, key)
}

// GetFile implements Storage.GetFile for FsStorage
func (fs *MemStorage) GetFile(bucket, key string) (io.ReadCloser, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)

	if obj, ok := fs.objects[objectPath]; ok {
		return ioutil.NopCloser(bytes.NewReader(obj.data)), nil
	}

	return nil, fmt.Errorf("%s: object not found", objectPath)
}

func (fs *MemStorage) getHeaders(bucket, key string) (http.Header, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	objectPath := fs.objectPath(bucket, key)

	if obj, ok := fs.objects[objectPath]; ok {
		return obj.headers, nil
	}

	return nil, fmt.Errorf("%s: object not found", objectPath)
}

// PutFile implements Storage.PutFile for FsStorage
func (fs *MemStorage) PutFile(bucket, key string, contents io.Reader, mimeType string) error {
	return fs.PutFileWithSetup(bucket, key, contents, func(req *http.Request) error {
		req.Header.Set("Content-Type", mimeType)
		return nil
	})
}

// PutFileWithSetup implements Storage.PutFileWithSetup for FsStorage
func (fs *MemStorage) PutFileWithSetup(bucket, key string, contents io.Reader, setup StorageSetupFunc) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	req, err := http.NewRequest("PUT", "http://127.0.0.1/dummy", nil)
	if err != nil {
		return err
	}

	err = setup(req)
	if err != nil {
		return err
	}

	data, err := ioutil.ReadAll(contents)
	if err != nil {
		return err
	}

	objectPath := fs.objectPath(bucket, key)
	fs.objects[objectPath] = memObject{
		data,
		req.Header,
	}

	return nil
}

// DeleteFile implements Storage.DeleteFile for FsStorage
func (fs *MemStorage) DeleteFile(bucket, key string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	delete(fs.objects, fs.objectPath(bucket, key))
	return nil
}
