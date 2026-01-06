package zipserver

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestInfoOperation(t *testing.T) {
	// Create test config with mem storage target
	config := &Config{
		Bucket:        "test-bucket",
		ExtractPrefix: "extract/",
		StorageTargets: []StorageConfig{
			{
				Name:   "test-mem",
				Type:   Mem,
				Bucket: "target-bucket",
			},
		},
		FileGetTimeout: Duration(30 * time.Second),
	}

	// Get the named mem storage and put a test file
	memStorage := GetNamedMemStorage("test-mem")
	defer ClearNamedMemStorage("test-mem")

	testContent := strings.NewReader("hello world")
	_, err := memStorage.PutFile(context.Background(), "target-bucket", "test/file.txt", testContent, PutOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("failed to put test file: %v", err)
	}

	ops := NewOperations(config)

	// Test getting info with target
	params := InfoParams{
		Key:        "test/file.txt",
		TargetName: "test-mem",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := ops.Info(ctx, params)
	if result.Err != nil {
		t.Fatalf("Info operation failed: %v", result.Err)
	}

	if result.Key != "test/file.txt" {
		t.Errorf("expected Key 'test/file.txt', got '%s'", result.Key)
	}

	if result.Bucket != "target-bucket" {
		t.Errorf("expected Bucket 'target-bucket', got '%s'", result.Bucket)
	}

	if len(result.Headers["Content-Type"]) == 0 || result.Headers["Content-Type"][0] != "text/plain" {
		t.Errorf("expected Content-Type 'text/plain', got '%v'", result.Headers["Content-Type"])
	}

	if len(result.Headers["Content-Length"]) == 0 || result.Headers["Content-Length"][0] != "11" {
		t.Errorf("expected Content-Length '11', got '%v'", result.Headers["Content-Length"])
	}
}

func TestInfoOperationMissingFile(t *testing.T) {
	config := &Config{
		Bucket:        "test-bucket",
		ExtractPrefix: "extract/",
		StorageTargets: []StorageConfig{
			{
				Name:   "test-mem-missing",
				Type:   Mem,
				Bucket: "target-bucket",
			},
		},
		FileGetTimeout: Duration(30 * time.Second),
	}

	// Clear any existing storage
	ClearNamedMemStorage("test-mem-missing")
	defer ClearNamedMemStorage("test-mem-missing")

	ops := NewOperations(config)

	params := InfoParams{
		Key:        "nonexistent/file.txt",
		TargetName: "test-mem-missing",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := ops.Info(ctx, params)
	if result.Err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestInfoOperationMissingKey(t *testing.T) {
	config := &Config{
		Bucket:        "test-bucket",
		ExtractPrefix: "extract/",
	}

	ops := NewOperations(config)

	params := InfoParams{
		Key: "", // empty key
	}

	result := ops.Info(context.Background(), params)
	if result.Err == nil {
		t.Fatal("expected error for missing key, got nil")
	}

	if !strings.Contains(result.Err.Error(), "key is required") {
		t.Errorf("expected error to contain 'key is required', got '%v'", result.Err)
	}
}

func TestInfoOperationInvalidTarget(t *testing.T) {
	config := &Config{
		Bucket:        "test-bucket",
		ExtractPrefix: "extract/",
	}

	ops := NewOperations(config)

	params := InfoParams{
		Key:        "some/file.txt",
		TargetName: "nonexistent-target",
	}

	result := ops.Info(context.Background(), params)
	if result.Err == nil {
		t.Fatal("expected error for invalid target, got nil")
	}

	if !strings.Contains(result.Err.Error(), "invalid target") {
		t.Errorf("expected error to contain 'invalid target', got '%v'", result.Err)
	}
}
