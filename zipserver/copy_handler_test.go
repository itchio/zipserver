package zipserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopy_Basic(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		targetName := "mem-target-copy-basic"
		defer ClearNamedMemStorage(targetName)

		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "target-bucket",
		}}

		testContent := "test content for copy"
		testKey := "zipserver_test/copy_basic_test.txt"

		_, err := storage.PutFile(ctx, config.Bucket, testKey,
			strings.NewReader(testContent), PutOptions{ContentType: "text/plain", ACL: ACLPublicRead})
		require.NoError(t, err)

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:        testKey,
			TargetName: targetName,
		})

		require.NoError(t, result.Err)
		assert.Equal(t, testKey, result.Key)
		assert.Equal(t, int64(len(testContent)), result.Size)
		assert.NotEmpty(t, result.Md5)
		assert.NotEmpty(t, result.Duration)
		assert.False(t, result.Injected)

		// Verify in target storage
		targetStorage := GetNamedMemStorage(targetName)
		reader, headers, err := targetStorage.GetFile(ctx, "target-bucket", testKey)
		require.NoError(t, err)
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		assert.Equal(t, testContent, string(data))
		assert.Equal(t, "text/plain", headers.Get("Content-Type"))
	})
}

func TestCopy_InvalidTarget(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:        "some/file.txt",
			TargetName: "nonexistent-target",
		})

		require.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "invalid target")
	})
}

func TestCopy_ReadonlyTarget(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		targetName := "mem-target-copy-readonly"
		defer ClearNamedMemStorage(targetName)

		config.StorageTargets = []StorageConfig{{
			Name:     targetName,
			Type:     Mem,
			Bucket:   "target-bucket",
			Readonly: true,
		}}

		testKey := "zipserver_test/copy_readonly_test.txt"
		_, err := storage.PutFile(ctx, config.Bucket, testKey,
			strings.NewReader("test"), PutOptions{ContentType: "text/plain", ACL: ACLPublicRead})
		require.NoError(t, err)

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:        testKey,
			TargetName: targetName,
		})

		require.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "readonly")
	})
}

func TestCopy_ExpectedBucketMismatch(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		targetName := "mem-target-copy-bucket-mismatch"
		defer ClearNamedMemStorage(targetName)

		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "actual-bucket",
		}}

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:            "some/file.txt",
			TargetName:     targetName,
			ExpectedBucket: "wrong-bucket",
		})

		require.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "expected bucket does not match")
	})
}

func TestCopy_ExpectedBucketMatches(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		targetName := "mem-target-copy-bucket-match"
		defer ClearNamedMemStorage(targetName)

		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "target-bucket",
		}}

		testKey := "zipserver_test/copy_bucket_match_test.txt"
		_, err := storage.PutFile(ctx, config.Bucket, testKey,
			strings.NewReader("test content"), PutOptions{ContentType: "text/plain", ACL: ACLPublicRead})
		require.NoError(t, err)

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:            testKey,
			TargetName:     targetName,
			ExpectedBucket: "target-bucket",
		})

		require.NoError(t, result.Err)
		assert.Equal(t, testKey, result.Key)
	})
}

func TestCopy_PreservesHeaders(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		targetName := "mem-target-copy-headers"
		defer ClearNamedMemStorage(targetName)

		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "target-bucket",
		}}

		testKey := "zipserver_test/copy_headers_test.json"
		_, err := storage.PutFile(ctx, config.Bucket, testKey,
			strings.NewReader(`{"test": true}`), PutOptions{
				ContentType:        "application/json",
				ContentDisposition: "attachment; filename=\"test.json\"",
				ACL:                ACLPublicRead,
			})
		require.NoError(t, err)

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:        testKey,
			TargetName: targetName,
		})

		require.NoError(t, result.Err)

		// Verify headers preserved
		targetStorage := GetNamedMemStorage(targetName)
		reader, headers, err := targetStorage.GetFile(ctx, "target-bucket", testKey)
		require.NoError(t, err)
		defer reader.Close()

		assert.Equal(t, "application/json", headers.Get("Content-Type"))
		assert.Equal(t, "attachment; filename=\"test.json\"", headers.Get("Content-Disposition"))
	})
}

func TestCopy_WithHtmlFooter(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		targetName := "mem-target-copy-footer"
		defer ClearNamedMemStorage(targetName)

		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "target-bucket",
		}}

		testKey := "zipserver_test/copy_footer_test.html"
		originalContent := "<html><body>Hello</body></html>"
		footer := "<script>console.log('injected')</script>"

		_, err := storage.PutFile(ctx, config.Bucket, testKey,
			strings.NewReader(originalContent), PutOptions{ContentType: "text/html", ACL: ACLPublicRead})
		require.NoError(t, err)

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:        testKey,
			TargetName: targetName,
			HtmlFooter: footer,
		})

		require.NoError(t, result.Err)
		assert.True(t, result.Injected)
		assert.Equal(t, int64(len(originalContent)+len(footer)), result.Size)

		// Verify content has footer appended
		targetStorage := GetNamedMemStorage(targetName)
		reader, _, err := targetStorage.GetFile(ctx, "target-bucket", testKey)
		require.NoError(t, err)
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		assert.Equal(t, originalContent+footer, string(data))
	})
}

// Note: TestCopy_HtmlFooterSkippedWhenEncoded is not included because GCS
// transparently decompresses gzip content, stripping the Content-Encoding header.
// The logic is tested via the extract tests which use local zip files.

func TestCopy_SourceNotFound(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		targetName := "mem-target-copy-notfound"
		defer ClearNamedMemStorage(targetName)

		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "target-bucket",
		}}

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:        "zipserver_test/nonexistent_file_12345.txt",
			TargetName: targetName,
		})

		require.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "failed to get file")
	})
}

func TestCopy_SameStorage(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()

		testContent := "test content for same storage copy"
		sourceKey := "zipserver_test/same_storage_source.txt"
		destKey := "zipserver_test/same_storage_dest.txt"

		// Upload source file
		_, err := storage.PutFile(ctx, config.Bucket, sourceKey,
			strings.NewReader(testContent), PutOptions{ContentType: "text/plain", ACL: ACLPublicRead})
		require.NoError(t, err)

		// Clean up dest key if it exists from previous test run
		defer storage.DeleteFile(ctx, config.Bucket, destKey)
		defer storage.DeleteFile(ctx, config.Bucket, sourceKey)

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:     sourceKey,
			DestKey: destKey,
			// No TargetName - same-storage copy
		})

		require.NoError(t, result.Err)
		assert.Equal(t, destKey, result.Key)
		assert.Equal(t, int64(len(testContent)), result.Size)
		assert.NotEmpty(t, result.Md5)

		// Verify the file was copied to dest key in primary storage
		reader, headers, err := storage.GetFile(ctx, config.Bucket, destKey)
		require.NoError(t, err)
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		assert.Equal(t, testContent, string(data))
		assert.Equal(t, "text/plain", headers.Get("Content-Type"))
	})
}

func TestCopy_CrossStorageWithDestKey(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		targetName := "mem-target-copy-destkey"
		defer ClearNamedMemStorage(targetName)

		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "target-bucket",
		}}

		testContent := "test content for cross storage with dest key"
		sourceKey := "zipserver_test/cross_storage_source.txt"
		destKey := "renamed/destination.txt"

		_, err := storage.PutFile(ctx, config.Bucket, sourceKey,
			strings.NewReader(testContent), PutOptions{ContentType: "text/plain", ACL: ACLPublicRead})
		require.NoError(t, err)

		ops := NewOperations(config)
		result := ops.Copy(ctx, CopyParams{
			Key:        sourceKey,
			DestKey:    destKey,
			TargetName: targetName,
		})

		require.NoError(t, result.Err)
		assert.Equal(t, destKey, result.Key) // Result key should be dest key
		assert.Equal(t, int64(len(testContent)), result.Size)

		// Verify file was written to dest key (not source key) in target storage
		targetStorage := GetNamedMemStorage(targetName)
		reader, _, err := targetStorage.GetFile(ctx, "target-bucket", destKey)
		require.NoError(t, err)
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		assert.Equal(t, testContent, string(data))

		// Verify source key does NOT exist in target storage
		_, _, err = targetStorage.GetFile(ctx, "target-bucket", sourceKey)
		assert.Error(t, err)
	})
}

func TestCopy_SameStorageExpectedBucket(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()

		testContent := "test content"
		sourceKey := "zipserver_test/same_storage_bucket_test.txt"
		destKey := "zipserver_test/same_storage_bucket_dest.txt"

		_, err := storage.PutFile(ctx, config.Bucket, sourceKey,
			strings.NewReader(testContent), PutOptions{ContentType: "text/plain", ACL: ACLPublicRead})
		require.NoError(t, err)
		defer storage.DeleteFile(ctx, config.Bucket, sourceKey)

		ops := NewOperations(config)

		// Test with wrong expected bucket
		result := ops.Copy(ctx, CopyParams{
			Key:            sourceKey,
			DestKey:        destKey,
			ExpectedBucket: "wrong-bucket",
		})

		require.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "expected bucket does not match primary bucket")

		// Test with correct expected bucket
		result = ops.Copy(ctx, CopyParams{
			Key:            sourceKey,
			DestKey:        destKey,
			ExpectedBucket: config.Bucket,
		})

		require.NoError(t, result.Err)
		defer storage.DeleteFile(ctx, config.Bucket, destKey)
		assert.Equal(t, destKey, result.Key)
	})
}

// Handler validation tests - these don't need GCS credentials

func copyHandlerTestConfig() *Config {
	return &Config{
		Bucket:     "test-bucket",
		JobTimeout: Duration(10 * time.Second),
		StorageTargets: []StorageConfig{{
			Name:   "test-target",
			Type:   Mem,
			Bucket: "target-bucket",
		}},
	}
}

func TestCopyHandler_MissingTargetAndDestKey(t *testing.T) {
	oldConfig := globalConfig
	globalConfig = copyHandlerTestConfig()
	defer func() { globalConfig = oldConfig }()

	form := url.Values{}
	form.Set("key", "some/file.txt")
	// Neither target nor dest_key provided

	req := httptest.NewRequest(http.MethodPost, "/copy", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	err := copyHandler(w, req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required parameter: target or dest_key")
}

func TestCopyHandler_SameKeyNoTarget(t *testing.T) {
	oldConfig := globalConfig
	globalConfig = copyHandlerTestConfig()
	defer func() { globalConfig = oldConfig }()

	form := url.Values{}
	form.Set("key", "some/file.txt")
	form.Set("dest_key", "some/file.txt") // Same as key, no target

	req := httptest.NewRequest(http.MethodPost, "/copy", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	err := copyHandler(w, req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "dest_key must differ from key")
}

func TestCopyHandler_SameKeyWithTarget(t *testing.T) {
	oldConfig := globalConfig
	globalConfig = copyHandlerTestConfig()
	defer func() { globalConfig = oldConfig }()
	defer ClearNamedMemStorage("test-target")

	// Pre-populate the mem storage with a file
	targetStorage := GetNamedMemStorage("test-target")
	ctx := context.Background()
	_, err := targetStorage.PutFile(ctx, "target-bucket", "some/file.txt",
		strings.NewReader("test"), PutOptions{ContentType: "text/plain"})
	require.NoError(t, err)

	form := url.Values{}
	form.Set("key", "some/file.txt")
	form.Set("dest_key", "some/file.txt") // Same as key, but with target - this is OK
	form.Set("target", "test-target")

	req := httptest.NewRequest(http.MethodPost, "/copy", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	// This will fail at the storage level (can't read from GCS without credentials)
	// but should NOT fail at the validation level
	err = copyHandler(w, req)

	// The error should be about storage, not validation
	if err != nil {
		assert.NotContains(t, err.Error(), "dest_key must differ")
		assert.NotContains(t, err.Error(), "missing required parameter")
	}
}

func TestCopyHandler_InvalidTarget(t *testing.T) {
	oldConfig := globalConfig
	globalConfig = copyHandlerTestConfig()
	defer func() { globalConfig = oldConfig }()

	form := url.Values{}
	form.Set("key", "some/file.txt")
	form.Set("target", "nonexistent-target")

	req := httptest.NewRequest(http.MethodPost, "/copy", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	err := copyHandler(w, req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid target")
}
