package zip_server

import (
	"os"
	"path"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"errors"

	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/goauth2/oauth/jwt"
)

var (
	baseUrl = "https://storage.googleapis.com/"
	scope = "https://www.googleapis.com/auth/devstorage.full_control"
)

// Simple interface to Google Cloud Storage
//   client := NewStorageClient(config)
//   readCloser, err = client.GetFile("my_bucket", "my_file")
type StorageClient struct {
	PrivateKeyPath string
	ClientEmail string

	jwtToken *jwt.Token
	oauthToken *oauth.Token
}

func NewStorageClient(config *Config) *StorageClient {
	return &StorageClient{
		PrivateKeyPath: config.PrivateKeyPath,
		ClientEmail: config.ClientEmail,
	}
}

func (c *StorageClient) refreshTokenIfNecessary() error {
	if c.oauthToken == nil || c.oauthToken.Expired() {
		return c.refreshToken()
	}

	return nil
}

func (c *StorageClient) refreshToken() error {
	if c.jwtToken == nil {
		pemBytes, err := ioutil.ReadFile(c.PrivateKeyPath)

		if err != nil {
			return err
		}

		c.jwtToken = jwt.NewToken(c.ClientEmail, scope, pemBytes)
	}

	newToken, err := c.jwtToken.Assert(&http.Client{})

	if err != nil {
		return err
	}

	c.oauthToken = newToken
	return nil
}

func (c *StorageClient) httpClient() (*http.Client, error) {
	err := c.refreshTokenIfNecessary()

	if err != nil {
		return nil, err
	}

	transport := &oauth.Transport{nil, c.oauthToken, nil}
	return transport.Client(), nil
}

func (c *StorageClient) url(bucket, key, logName string) string {
	// return "http://127.0.0.1:5656"
	url := baseUrl + bucket + "/" + key
	log.Print(logName + " " + url)
	return url
}

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

func (c *StorageClient) GetFileToString(bucket, key string) (string, error) {
	reader, err := c.GetFile(bucket, key)

	if err != nil {
		return "", err
	}

	defer reader.Close()
	body, err := ioutil.ReadAll(reader)

	if err != nil {
		return "", err
	}

	return string(body), nil
}

func (c *StorageClient) PutFile(bucket, key string, contents io.Reader, mimeType string) error {
	return c.PutFileWithSetup(bucket, key, contents, func(req *http.Request) error {
		req.Header.Add("Content-Type", mimeType)
		req.Header.Add("x-goog-acl", "public-read")
		return nil
	})
}

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

	if res.StatusCode != 200 {
		return errors.New(res.Status + " " + url)
	}

	return nil
}

