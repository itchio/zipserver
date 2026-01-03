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

// NewGcsStorage returns a new GCS-backed storage
func NewGcsStorage(config *Config) (*GcsStorage, error) {
	pemBytes, err := os.ReadFile(config.PrivateKeyPath)

	if err != nil {
		return nil, err
	}

	jwtConfig := &jwt.Config{
		Email:      config.ClientEmail,
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
