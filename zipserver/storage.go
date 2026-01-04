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

// ReaderAtCloser combines io.ReaderAt with io.Closer for seekable reads
type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
	BytesRead() uint64 // Returns total bytes read so far
}

// Storage is a place we can get files from, put files into, or delete files from
type Storage interface {
	GetFile(ctx context.Context, bucket, key string) (io.ReadCloser, http.Header, error)
	// GetReaderAt returns a ReaderAt for random access reads. maxBytes limits total bytes read (0 = unlimited).
	GetReaderAt(ctx context.Context, bucket, key string, maxBytes uint64) (ReaderAtCloser, int64, error)
	PutFile(ctx context.Context, bucket, key string, contents io.Reader, opts PutOptions) (PutResult, error)
	DeleteFile(ctx context.Context, bucket, key string) error
}
