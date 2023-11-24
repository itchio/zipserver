package zipserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// StorageSetupFunc gives the consumer a chance to set HTTP headers before storing something
type StorageSetupFunc func(*http.Request) error

// Storage is a place we can get files from, put files into, or delete files from
type Storage interface {
	GetFile(ctx context.Context, bucket, key string) (io.ReadCloser, http.Header, error)
	PutFile(ctx context.Context, bucket, key string, contents io.Reader, mimeType string) error
	PutFileWithSetup(ctx context.Context, bucket, key string, contents io.Reader, setup StorageSetupFunc) error
	DeleteFile(ctx context.Context, bucket, key string) error
}

// GetStorage returns a Storage object based on the given storage name and config.
// TODO: eventually this should be a factory that can return different storage types
func NewStorageByName(config *Config, name string) (*S3Storage, error) {
	targetConfig := config.GetStorageTargetByName(name)
	if targetConfig == nil {
		return nil, fmt.Errorf("no config found for name: %s", name)
	}

	switch targetConfig.Type {
	case S3:
		return NewS3Storage(targetConfig)
	case GCS:
		return nil, fmt.Errorf("GCS storage type is not supported yet")
	default:
		return nil, fmt.Errorf("unsupported storage type")
	}
}
