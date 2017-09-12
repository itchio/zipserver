package zipserver

import (
	"io"
	"net/http"
)

// StorageSetupFunc gives the consumer a chance to set HTTP headers before storing something
type StorageSetupFunc func(*http.Request) error

// Storage is a place we can get files from, put files into, or delete files from
type Storage interface {
	GetFile(bucket, key string) (io.ReadCloser, error)
	PutFile(bucket, key string, contents io.Reader, mimeType string) error
	PutFileWithSetup(bucket, key string, contents io.Reader, setup StorageSetupFunc) error
	DeleteFile(bucket, key string) error
}
