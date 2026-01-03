package zipserver

import (
	"context"
	"io"
	"net/http"
)

// ACL represents storage access control level
type ACL string

const (
	ACLPublicRead ACL = "public-read"
	ACLPrivate    ACL = "private"
)

// PutOptions contains configuration for uploading a file
type PutOptions struct {
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	ACL                ACL
}

// PutResult contains the result of a PutFile operation
type PutResult struct {
	MD5 string // hex-encoded MD5 checksum of uploaded bytes
}

// Storage is a place we can get files from, put files into, or delete files from
type Storage interface {
	GetFile(ctx context.Context, bucket, key string) (io.ReadCloser, http.Header, error)
	PutFile(ctx context.Context, bucket, key string, contents io.Reader, opts PutOptions) (PutResult, error)
	DeleteFile(ctx context.Context, bucket, key string) error
}
