package zipserver

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func deleteTestConfig() *Config {
	return &Config{
		Bucket:            "test-bucket",
		ExtractPrefix:     "extracted",
		JobTimeout:        Duration(10 * time.Second),
		ExtractionThreads: 2,
	}
}

// Unit tests for validateKeysInExtractPrefix

func TestValidateKeysInExtractPrefix_ValidKeys(t *testing.T) {
	err := validateKeysInExtractPrefix([]string{
		"extracted/file.txt",
		"extracted/subdir/file.txt",
		"extracted/a/b/c/deep.txt",
	}, "extracted")

	assert.NoError(t, err)
}

func TestValidateKeysInExtractPrefix_InvalidKey(t *testing.T) {
	err := validateKeysInExtractPrefix([]string{
		"other/file.txt",
	}, "extracted")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "keys must be within extract prefix")
	assert.Contains(t, err.Error(), "other/file.txt")
}

func TestValidateKeysInExtractPrefix_MixedKeys(t *testing.T) {
	err := validateKeysInExtractPrefix([]string{
		"extracted/good.txt",
		"other/bad.txt",
		"extracted/also-good.txt",
		"wrong/path.txt",
	}, "extracted")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "other/bad.txt")
	assert.Contains(t, err.Error(), "wrong/path.txt")
	// Valid keys should not be in error message
	assert.NotContains(t, err.Error(), "good.txt")
}

func TestValidateKeysInExtractPrefix_PrefixWithoutTrailingSlash(t *testing.T) {
	// Prefix "extracted" should work the same as "extracted/"
	err := validateKeysInExtractPrefix([]string{
		"extracted/file.txt",
	}, "extracted")

	assert.NoError(t, err)
}

func TestValidateKeysInExtractPrefix_PrefixWithTrailingSlash(t *testing.T) {
	err := validateKeysInExtractPrefix([]string{
		"extracted/file.txt",
	}, "extracted/")

	assert.NoError(t, err)
}

func TestValidateKeysInExtractPrefix_EmptyKeys(t *testing.T) {
	err := validateKeysInExtractPrefix([]string{}, "extracted")
	assert.NoError(t, err)
}

func TestValidateKeysInExtractPrefix_PrefixSubstring(t *testing.T) {
	// "extracted-other/file.txt" should NOT match prefix "extracted"
	// because it's a substring match, not a path prefix match
	err := validateKeysInExtractPrefix([]string{
		"extracted-other/file.txt",
	}, "extracted")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "extracted-other/file.txt")
}

func TestValidateKeysInExtractPrefix_DotSegments(t *testing.T) {
	err := validateKeysInExtractPrefix([]string{
		"extracted/../secret.txt",
	}, "extracted")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "extracted/../secret.txt")
}

func TestValidateKeysInExtractPrefix_RootOfPrefix(t *testing.T) {
	// Key that is exactly the prefix without trailing content should fail
	// because "extracted" doesn't start with "extracted/"
	err := validateKeysInExtractPrefix([]string{
		"extracted",
	}, "extracted")

	assert.Error(t, err)
}

// Integration tests for Operations.Delete with named targets

func TestDelete_WithMemTarget(t *testing.T) {
	targetName := "mem-target-delete-basic"
	defer ClearNamedMemStorage(targetName)

	config := deleteTestConfig()
	config.StorageTargets = []StorageConfig{
		{Name: targetName, Type: Mem, Bucket: "test-bucket"},
	}
	ops := NewOperations(config)

	// Pre-populate storage with files
	ctx := context.Background()
	storage := GetNamedMemStorage(targetName)
	storage.PutFile(ctx, "test-bucket", "file1.txt", bytes.NewReader([]byte("content1")), PutOptions{})
	storage.PutFile(ctx, "test-bucket", "file2.txt", bytes.NewReader([]byte("content2")), PutOptions{})

	// Verify files exist
	_, _, err := storage.GetFile(ctx, "test-bucket", "file1.txt")
	require.NoError(t, err)

	// Delete files
	result := ops.Delete(ctx, DeleteParams{
		Keys:       []string{"file1.txt", "file2.txt"},
		TargetName: targetName,
	})

	require.NoError(t, result.Err)
	assert.Equal(t, 2, result.TotalKeys)
	assert.Equal(t, 2, result.DeletedKeys)
	assert.Empty(t, result.Errors)

	// Verify files are gone
	_, _, err = storage.GetFile(ctx, "test-bucket", "file1.txt")
	assert.Error(t, err)
	_, _, err = storage.GetFile(ctx, "test-bucket", "file2.txt")
	assert.Error(t, err)
}

func TestDelete_InvalidTarget(t *testing.T) {
	config := deleteTestConfig()
	ops := NewOperations(config)

	ctx := context.Background()
	result := ops.Delete(ctx, DeleteParams{
		Keys:       []string{"file.txt"},
		TargetName: "nonexistent-target",
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "invalid target: nonexistent-target")
}

func TestDelete_ReadonlyTarget(t *testing.T) {
	targetName := "readonly-target-delete"
	defer ClearNamedMemStorage(targetName)

	config := deleteTestConfig()
	config.StorageTargets = []StorageConfig{
		{Name: targetName, Type: Mem, Bucket: "test-bucket", Readonly: true},
	}
	ops := NewOperations(config)

	ctx := context.Background()
	result := ops.Delete(ctx, DeleteParams{
		Keys:       []string{"file.txt"},
		TargetName: targetName,
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "readonly")
}

func TestDelete_MultipleKeys(t *testing.T) {
	targetName := "mem-target-delete-multi"
	defer ClearNamedMemStorage(targetName)

	config := deleteTestConfig()
	config.StorageTargets = []StorageConfig{
		{Name: targetName, Type: Mem, Bucket: "test-bucket"},
	}
	ops := NewOperations(config)

	ctx := context.Background()
	storage := GetNamedMemStorage(targetName)

	// Create 10 files
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file%d.txt", i)
		storage.PutFile(ctx, "test-bucket", key, bytes.NewReader([]byte("content")), PutOptions{})
	}

	// Delete all 10
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("file%d.txt", i)
	}

	result := ops.Delete(ctx, DeleteParams{
		Keys:       keys,
		TargetName: targetName,
	})

	require.NoError(t, result.Err)
	assert.Equal(t, 10, result.TotalKeys)
	assert.Equal(t, 10, result.DeletedKeys)
	assert.Empty(t, result.Errors)
}

func TestDelete_NonexistentKey(t *testing.T) {
	targetName := "mem-target-delete-nonexistent"
	defer ClearNamedMemStorage(targetName)

	config := deleteTestConfig()
	config.StorageTargets = []StorageConfig{
		{Name: targetName, Type: Mem, Bucket: "test-bucket"},
	}
	ops := NewOperations(config)

	ctx := context.Background()

	// Try to delete a file that doesn't exist
	// MemStorage.DeleteFile doesn't return error for nonexistent keys
	result := ops.Delete(ctx, DeleteParams{
		Keys:       []string{"nonexistent.txt"},
		TargetName: targetName,
	})

	require.NoError(t, result.Err)
	assert.Equal(t, 1, result.TotalKeys)
	assert.Equal(t, 1, result.DeletedKeys) // MemStorage doesn't error on missing keys
}

// Tests for primary storage delete with ExtractPrefix validation

func TestDelete_PrimaryStorage_KeyOutsidePrefix(t *testing.T) {
	config := deleteTestConfig()
	config.ExtractPrefix = "extracted"
	ops := NewOperations(config)

	ctx := context.Background()
	result := ops.Delete(ctx, DeleteParams{
		Keys:       []string{"other/file.txt"},
		TargetName: "", // Empty means primary storage
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "keys must be within extract prefix")
	assert.Contains(t, result.Err.Error(), "other/file.txt")
}

func TestDelete_PrimaryStorage_MixedValidInvalid(t *testing.T) {
	config := deleteTestConfig()
	config.ExtractPrefix = "data"
	ops := NewOperations(config)

	ctx := context.Background()
	result := ops.Delete(ctx, DeleteParams{
		Keys: []string{
			"data/valid.txt",
			"outside/invalid.txt",
		},
		TargetName: "",
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "outside/invalid.txt")
}

func TestDelete_PrimaryStorage_AllKeysValid(t *testing.T) {
	// This test verifies that validation passes for valid keys
	// Note: The actual deletion would require GCS credentials,
	// so we're only testing the validation logic here
	config := deleteTestConfig()
	config.ExtractPrefix = "extracted"
	// Without valid GCS credentials, this will fail at storage creation
	// but after the prefix validation passes
	ops := NewOperations(config)

	ctx := context.Background()
	result := ops.Delete(ctx, DeleteParams{
		Keys: []string{
			"extracted/file1.txt",
			"extracted/subdir/file2.txt",
		},
		TargetName: "",
	})

	// The error should be about GCS credentials, not prefix validation
	if result.Err != nil {
		assert.NotContains(t, result.Err.Error(), "keys must be within extract prefix")
	}
}
