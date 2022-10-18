package zipserver

import (
	"context"
	"io"
	"net/http"
)

// StorageSetupFunc gives the consumer a chance to set HTTP headers before storing something
type StorageSetupFunc func(*http.Request) error

// Storage is a place we can get files from, put files into, or delete files from
type Storage interface {
	GetFile(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	PutFile(ctx context.Context, bucket, key string, contents io.Reader, mimeType string) error
	PutFileWithSetup(ctx context.Context, bucket, key string, contents io.Reader, setup StorageSetupFunc) error
	DeleteFile(ctx context.Context, bucket, key string) error
}
