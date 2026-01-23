package zipserver

import (
	"github.com/klauspost/compress/zip"
	"bytes"
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

func TestGetReaderAtRangeEfficiency(t *testing.T) {
	ctx := context.Background()

	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		// Create a zip with uncompressed data to make it large enough to see efficiency gains
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)

		// Add files with Store (no compression) to ensure predictable size
		for i := 0; i < 10; i++ {
			header := &zip.FileHeader{
				Name:   fmt.Sprintf("file%d.bin", i),
				Method: zip.Store, // No compression
			}
			f, err := zw.CreateHeader(header)
			if err != nil {
				t.Fatalf("create file: %v", err)
			}
			// Write 100KB of pseudo-random data per file (1MB total)
			padding := make([]byte, 100*1024)
			for j := range padding {
				padding[j] = byte((j * 17) % 256) // Pseudo-random pattern
			}
			if _, err := f.Write(padding); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("close zip: %v", err)
		}

		zipData := buf.Bytes()
		zipSize := int64(len(zipData))
		t.Logf("Test zip size: %d bytes", zipSize)

		// Upload the test zip
		testKey := "zipserver_range_test.zip"
		_, err := storage.PutFile(ctx, config.Bucket, testKey, bytes.NewReader(zipData), PutOptions{
			ContentType: "application/zip",
		})
		if err != nil {
			t.Fatalf("upload test zip: %v", err)
		}
		defer storage.DeleteFile(ctx, config.Bucket, testKey)

		// Get a ReaderAt and list the zip contents
		readerAt, size, err := storage.GetReaderAt(ctx, config.Bucket, testKey, 0)
		if err != nil {
			t.Fatalf("GetReaderAt: %v", err)
		}
		defer readerAt.Close()

		if size != zipSize {
			t.Fatalf("size mismatch: got %d, expected %d", size, zipSize)
		}

		// Use zip.NewReader which should only read the central directory
		zipReader, err := zip.NewReader(readerAt, size)
		if err != nil {
			t.Fatalf("zip.NewReader: %v", err)
		}

		// Verify we got the right files
		if len(zipReader.File) != 10 {
			t.Fatalf("expected 10 files, got %d", len(zipReader.File))
		}

		bytesRead := readerAt.BytesRead()
		t.Logf("Bytes read: %d / %d (%.2f%%)", bytesRead, zipSize, float64(bytesRead)/float64(zipSize)*100)

		// The central directory + EOCD should be much smaller than the full zip
		// For a 1MB zip with 10 files, we expect to read only a few KB
		// Use 5% as threshold - actual should be < 1%
		maxExpectedBytes := uint64(zipSize / 20)
		if bytesRead > maxExpectedBytes {
			t.Errorf("Read too many bytes: %d > %d (expected < 5%% of file)", bytesRead, maxExpectedBytes)
		}
	})
}
