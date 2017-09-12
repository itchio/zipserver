package zipserver

import (
	"os"
	"strings"
	"testing"
)

const testPrivateKey = "/home/leafo/code/go/cf45ea3f8a5f730a4b9702d11236439d9b014b20-privatekey.pem"

type ClientFunc func(client *StorageClient, config *Config)

func withClient(t *testing.T, cb ClientFunc) {
	_, err := os.Lstat(testPrivateKey)
	if err != nil {
		t.Logf("Skipping %s (no private key)", t.Name())
		return
	}

	config := &Config{
		PrivateKeyPath: testPrivateKey,
		ClientEmail:    "507810471102@developer.gserviceaccount.com",
		Bucket:         "leafo",
	}

	client, err := NewStorageClient(config)

	if err != nil {
		t.Fatal(err)
	}

	cb(client, config)
}

func TestGetFile(t *testing.T) {
	withClient(t, func(client *StorageClient, config *Config) {
		str, err := client.GetFileToString(config.Bucket, "text.txt")

		if err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(str, "Gravity") {
			t.Fatal("Expected to get string from text.txt")
		}
	})
}

func TestPutAndDeleteFile(t *testing.T) {
	withClient(t, func(client *StorageClient, config *Config) {
		err := client.PutFile(config.Bucket, "zipserver_test.txt", strings.NewReader("hello zipserver!"), "text/plain")

		if err != nil {
			t.Fatal(err)
		}

		err = client.DeleteFile(config.Bucket, "zipserver_test.txt")

		if err != nil {
			t.Fatal(err)
		}
	})
}
