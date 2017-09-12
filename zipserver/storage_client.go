package zipserver

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"os"
	"path"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
)

var (
	baseURL = "https://storage.googleapis.com/"
	scope   = "https://www.googleapis.com/auth/devstorage.full_control"
)

// StorageClient is a simple interface to Google Cloud Storage
//
// Example usage:
//   client := NewStorageClient(config)
//   readCloser, err = client.GetFile("my_bucket", "my_file")
type StorageClient struct {
	jwtConfig *jwt.Config
}

// NewStorageClient returns a new GCS storage client
func NewStorageClient(config *Config) (*StorageClient, error) {
	pemBytes, err := ioutil.ReadFile(config.PrivateKeyPath)

	if err != nil {
		return nil, err
	}

	jwtConfig := &jwt.Config{
		Email:      config.ClientEmail,
		PrivateKey: pemBytes,
		TokenURL:   google.JWTTokenURL,
		Scopes:     []string{scope},
	}

	return &StorageClient{
		jwtConfig: jwtConfig,
	}, nil
}

func (c *StorageClient) httpClient() (*http.Client, error) {
	return c.jwtConfig.Client(oauth2.NoContext), nil
}

func (c *StorageClient) url(bucket, key, logName string) string {
	// return "http://127.0.0.1:5656"
	url := baseURL + bucket + "/" + key
	log.Print(logName + " " + url)
	return url
}

// GetFile returns a reader for the contents of resource at bucket/key
func (c *StorageClient) GetFile(bucket, key string) (io.ReadCloser, error) {
	httpClient, err := c.httpClient()

	if err != nil {
		return nil, err
	}

	url := c.url(bucket, key, "GET")

	res, err := httpClient.Get(url)

	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, errors.New(res.Status + " " + url)
	}

	return res.Body, nil
}

// PutFile uploads a file to GCS simply
func (c *StorageClient) PutFile(bucket, key string, contents io.Reader, mimeType string) error {
	return c.PutFileWithSetup(bucket, key, contents, func(req *http.Request) error {
		req.Header.Add("Content-Type", mimeType)
		req.Header.Add("x-goog-acl", "public-read")
		return nil
	})
}

// PutFileWithSetup uploads a file to GCS letting the user set up the request first
func (c *StorageClient) PutFileWithSetup(bucket, key string, contents io.Reader, setup func(*http.Request) error) error {
	httpClient, err := c.httpClient()

	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", c.url(bucket, key, "PUT"), contents)

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
	return nil
}

// PutFileFromFname uploads a file from disk. It detects mime type from the file extension
func (c *StorageClient) PutFileFromFname(bucket, key, fname string) error {
	file, err := os.Open(fname)

	if err != nil {
		return err
	}

	mimeType := mime.TypeByExtension(path.Ext(fname))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	return c.PutFile(bucket, key, file, mimeType)
}

// DeleteFile removes a file from a GCS bucket
func (c *StorageClient) DeleteFile(bucket, key string) error {
	httpClient, err := c.httpClient()

	if err != nil {
		return err
	}

	url := c.url(bucket, key, "DELETE")
	req, err := http.NewRequest("DELETE", url, nil)

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
