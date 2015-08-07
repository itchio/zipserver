package zipserver

import (
	"strings"
	"testing"
)

func getClient(t *testing.T) (*StorageClient, *Config) {
	config := &Config{
		PrivateKeyPath: "/home/leafo/code/go/cf45ea3f8a5f730a4b9702d11236439d9b014b20-privatekey.pem",
		ClientEmail:    "507810471102@developer.gserviceaccount.com",
		Bucket:         "leafo",
	}

	client, err := NewStorageClient(config)

	if err != nil {
		t.Fatal(err)
	}

	return client, config
}

func TestGetFile(t *testing.T) {
	client, config := getClient(t)

	str, err := client.GetFileToString(config.Bucket, "text.txt")

	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(str, "Gravity") {
		t.Fatal("Expected to get string from text.txt")
	}
}

func TestPutAndDeleteFile(t *testing.T) {
	client, config := getClient(t)
	err := client.PutFile(config.Bucket, "zipserver_test.txt", strings.NewReader("hello zipserver!"), "text/plain")

	if err != nil {
		t.Fatal(err)
	}

	err = client.DeleteFile(config.Bucket, "zipserver_test.txt")

	if err != nil {
		t.Fatal(err)
	}
}
