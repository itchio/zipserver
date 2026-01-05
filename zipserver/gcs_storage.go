package zipserver

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
)

var (
	baseURL = "https://storage.googleapis.com/"
	scope   = "https://www.googleapis.com/auth/devstorage.full_control"
)

// GcsStorage is a simple interface to Google Cloud Storage
//
// Example usage:
//
//	storage := NewStorageClient(config)
//	readCloser, err = storage.GetFile("my_bucket", "my_file")
type GcsStorage struct {
	jwtConfig *jwt.Config
}

// interface guard
var _ Storage = (*GcsStorage)(nil)

// NewGcsStorage returns a new GCS-backed storage using main config credentials
func NewGcsStorage(config *Config) (*GcsStorage, error) {
	return NewGcsStorageWithCredentials(config.PrivateKeyPath, config.ClientEmail)
}

// NewGcsStorageWithCredentials returns a new GCS-backed storage using explicit credentials
func NewGcsStorageWithCredentials(privateKeyPath, clientEmail string) (*GcsStorage, error) {
	if privateKeyPath == "" || clientEmail == "" {
		return nil, fmt.Errorf("GCS credentials require both privateKeyPath and clientEmail")
	}

	pemBytes, err := os.ReadFile(resolveCredentialPath(privateKeyPath))

	if err != nil {
		return nil, err
	}

	jwtConfig := &jwt.Config{
		Email:      clientEmail,
		PrivateKey: pemBytes,
		TokenURL:   google.JWTTokenURL,
		Scopes:     []string{scope},
	}

	return &GcsStorage{
		jwtConfig: jwtConfig,
	}, nil
}

func (c *GcsStorage) httpClient() (*http.Client, error) {
	return c.jwtConfig.Client(context.Background()), nil
}

func (c *GcsStorage) url(bucket, key, logName string) string {
	// return "http://127.0.0.1:5656"
	url := baseURL + bucket + "/" + key
	log.Print(logName + " " + url)
	return url
}

// GetFile returns a reader for the contents of resource at bucket/key
func (c *GcsStorage) GetFile(ctx context.Context, bucket, key string) (io.ReadCloser, http.Header, error) {
	httpClient, err := c.httpClient()
	if err != nil {
		return nil, nil, err
	}

	url := c.url(bucket, key, "GET")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}

	if res.StatusCode != 200 {
		res.Body.Close()
		return nil, res.Header, errors.New(res.Status + " " + url)
	}

	trackedBody := metricsReadCloser{res.Body, &globalMetrics.TotalBytesDownloaded}

	return trackedBody, res.Header, nil
}

// PutFile uploads a file to GCS with the given options
func (c *GcsStorage) PutFile(ctx context.Context, bucket, key string, contents io.Reader, opts PutOptions) (PutResult, error) {
	httpClient, err := c.httpClient()
	if err != nil {
		return PutResult{}, err
	}

	contents = metricsReader(contents, &globalMetrics.TotalBytesUploaded)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(bucket, key, "PUT"), contents)
	if err != nil {
		return PutResult{}, err
	}

	if opts.ContentType != "" {
		req.Header.Set("Content-Type", opts.ContentType)
	}
	if opts.ContentEncoding != "" {
		req.Header.Set("Content-Encoding", opts.ContentEncoding)
	}
	if opts.ContentDisposition != "" {
		req.Header.Set("Content-Disposition", opts.ContentDisposition)
	}
	if opts.ACL != "" {
		req.Header.Set("x-goog-acl", string(opts.ACL))
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return PutResult{}, err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return PutResult{}, err
		}
		return PutResult{}, fmt.Errorf("%s: %s", res.Status, body)
	}

	result := PutResult{}

	// Parse MD5 from x-goog-hash header(s)
	// GCS may return multiple X-Goog-Hash headers (one for crc32c, one for md5)
	for _, googHash := range res.Header.Values("x-goog-hash") {
		if strings.HasPrefix(googHash, "md5=") {
			b64 := strings.TrimPrefix(googHash, "md5=")
			if decoded, err := base64.StdEncoding.DecodeString(b64); err == nil {
				result.MD5 = hex.EncodeToString(decoded)
			}
			break
		}
	}

	return result, nil
}

// gcsReaderAt implements ReaderAtCloser using HTTP Range requests
type gcsReaderAt struct {
	client    *http.Client
	url       string
	size      int64
	maxBytes  uint64 // maximum total bytes to read (0 = unlimited)
	bytesRead uint64 // total bytes read so far
	ctx       context.Context
}

func (r *gcsReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	toRead := uint64(end - off + 1)
	if r.maxBytes > 0 && r.bytesRead+toRead > r.maxBytes {
		return 0, fmt.Errorf("max read limit exceeded (%d bytes)", r.maxBytes)
	}

	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("range request failed: %s", resp.Status)
	}

	n, err = io.ReadFull(resp.Body, p[:end-off+1])
	r.bytesRead += uint64(n)
	return n, err
}

func (r *gcsReaderAt) Close() error {
	return nil // No resources to release
}

func (r *gcsReaderAt) BytesRead() uint64 {
	return r.bytesRead
}

// GetReaderAt returns a ReaderAt for the file, suitable for random access reads.
// This is more efficient than GetFile for operations that only need partial file access.
// maxBytes limits the total bytes that can be read (0 = unlimited).
func (c *GcsStorage) GetReaderAt(ctx context.Context, bucket, key string, maxBytes uint64) (ReaderAtCloser, int64, error) {
	httpClient, err := c.httpClient()
	if err != nil {
		return nil, 0, err
	}

	url := c.url(bucket, key, "HEAD")
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return nil, 0, err
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, 0, errors.New(res.Status + " " + url)
	}

	size := res.ContentLength
	if size < 0 {
		return nil, 0, errors.New("server did not return Content-Length")
	}

	// Use GET URL for the reader
	getURL := c.url(bucket, key, "GET(range)")

	return &gcsReaderAt{
		client:   httpClient,
		url:      getURL,
		size:     size,
		maxBytes: maxBytes,
		ctx:      ctx,
	}, size, nil
}

// DeleteFile removes a file from a GCS bucket
func (c *GcsStorage) DeleteFile(ctx context.Context, bucket, key string) error {
	httpClient, err := c.httpClient()
	if err != nil {
		return err
	}

	url := c.url(bucket, key, "DELETE")
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 && res.StatusCode != 204 {
		return errors.New(res.Status + " " + url)
	}

	return nil
}
