package zipserver

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

const testPrivateKey = "/home/leafo/code/go/cf45ea3f8a5f730a4b9702d11236439d9b014b20-privatekey.pem"

type ClientFunc func(storage Storage, config *Config)

func withGoogleCloudStorage(t *testing.T, cb ClientFunc) {
	_, err := os.Lstat(testPrivateKey)
	if err != nil {
		t.Logf("Skipping (no private key)")
		return
	}

	config := &Config{
		PrivateKeyPath: testPrivateKey,
		ClientEmail:    "507810471102@developer.gserviceaccount.com",
		Bucket:         "leafo",
		JobTimeout:     Duration(10 * time.Second),
		FileGetTimeout: Duration(10 * time.Second),
		FilePutTimeout: Duration(10 * time.Second),
	}

	storage, err := NewGcsStorage(config)

	if err != nil {
		t.Fatal(err)
	}

	cb(storage, config)
}

func TestGetFile(t *testing.T) {
	ctx := context.Background()

	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		reader, _, err := storage.GetFile(ctx, config.Bucket, "text.txt")
		if err != nil {
			t.Fatal(err)
		}

		defer reader.Close()
		bytesContent, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}

		str := string(bytesContent)

		if !strings.Contains(str, "Gravity") {
			t.Fatal("Expected to get string from text.txt")
		}
	})
}

func TestPutAndDeleteFile(t *testing.T) {
	ctx := context.Background()

	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		err := storage.PutFile(ctx, config.Bucket, "zipserver_test.txt", strings.NewReader("hello zipserver!"), PutOptions{
			ContentType: "text/plain",
			ACL:         ACLPublicRead,
		})

		if err != nil {
			t.Fatal(err)
		}

		err = storage.DeleteFile(ctx, config.Bucket, "zipserver_test.txt")

		if err != nil {
			t.Fatal(err)
		}
	})
}
