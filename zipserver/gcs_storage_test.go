package zipserver

import (
	"context"
	"crypto/md5"
	"fmt"
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
		content := "hello zipserver!"
		expectedMD5 := fmt.Sprintf("%x", md5.Sum([]byte(content)))

		result, err := storage.PutFile(ctx, config.Bucket, "zipserver_test.txt", strings.NewReader(content), PutOptions{
			ContentType: "text/plain",
			ACL:         ACLPublicRead,
		})

		if err != nil {
			t.Fatal(err)
		}

		if result.MD5 != expectedMD5 {
			t.Fatalf("MD5 mismatch: got %s, expected %s", result.MD5, expectedMD5)
		}

		t.Logf("Upload MD5 verified: %s", result.MD5)

		err = storage.DeleteFile(ctx, config.Bucket, "zipserver_test.txt")

		if err != nil {
			t.Fatal(err)
		}
	})
}
