package zipserver

import (
	"context"
	"io"
	"strings"
	"testing"

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
