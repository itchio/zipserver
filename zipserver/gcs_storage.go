package zipserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"golang.org/x/oauth2"
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
	return c.jwtConfig.Client(oauth2.NoContext), nil
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
		return nil, res.Header, errors.New(res.Status + " " + url)
	}

	return res.Body, res.Header, nil
}

// PutFile uploads a file to GCS simply
func (c *GcsStorage) PutFile(ctx context.Context, bucket, key string, contents io.Reader, mimeType string) error {
	return c.PutFileWithSetup(ctx, bucket, key, contents, func(req *http.Request) error {
		req.Header.Add("Content-Type", mimeType)
		req.Header.Add("x-goog-acl", "public-read")
		return nil
	})
}

// PutFileWithSetup uploads a file to GCS letting the user set up the request first
func (c *GcsStorage) PutFileWithSetup(ctx context.Context, bucket, key string, contents io.Reader, setup StorageSetupFunc) error {
	httpClient, err := c.httpClient()
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(bucket, key, "PUT"), contents)
	if err != nil {
		return err
	}

	err = setup(req)
	if err != nil {
		return err
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%s: %s", res.Status, body)
	}

	return nil
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
