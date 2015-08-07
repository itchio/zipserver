package zipserver

import (
	"strings"
	"testing"
)

func TestGetFile(t *testing.T) {
	config := &Config{
		PrivateKeyPath: "/home/leafo/code/go/cf45ea3f8a5f730a4b9702d11236439d9b014b20-privatekey.pem",
		ClientEmail:    "507810471102@developer.gserviceaccount.com",
		Bucket:         "leafo",
	}

	client, err := NewStorageClient(config)

	if err != nil {
		t.Fatal(err)
	}

	str, err := client.GetFileToString(config.Bucket, "text.txt")

	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(str, "Gravity") {
		t.Fatal("Expected to get string from text.txt")
	}
}
