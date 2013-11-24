// Simple interface to Google Cloud Storage
//   client := &StorageClient{
//   	PrivateKeyPath: "private_key.pem",
//   	ClientEmail: "1111111@developer.gserviceaccount.com",
//   }
//
//   file, err = client.GetFile("my_bucket", "my_file")
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

type StorageClient struct {
	PrivateKeyPath string
	ClientEmail string

	jwtToken *jwt.Token
	oauthToken *oauth.Token
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

func (self *StorageClient) url(bucket, key string) string {
	// return "http://127.0.0.1:5656"
	url := baseUrl + bucket + "/" + key
	log.Print(url)
	return url
}

func (c *StorageClient) GetFile(bucket, key string) (io.ReadCloser, error) {
	httpClient, err := c.httpClient()

	if err != nil {
		return nil, err
	}

	url := c.url(bucket, key)

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
		return "", nil
	}

	defer reader.Close()
	body, err := ioutil.ReadAll(reader)

	if err != nil {
		return "", nil
	}

	return string(body), nil
}

func (c *StorageClient) PutFile(bucket, key string, contents io.Reader, mimeType string) error {
	httpClient, err := c.httpClient()

	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", c.url(bucket, key), contents)

	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", mimeType)
	req.Header.Add("x-goog-acl", "public-read")

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

	c.PutFile(bucket, key, file, mimeType)
	return nil
}

