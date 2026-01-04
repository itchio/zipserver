package zipserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func slurpTestConfig() *Config {
	return &Config{
		Bucket:         "default-bucket",
		JobTimeout:     Duration(10 * time.Second),
		FileGetTimeout: Duration(10 * time.Second),
		FilePutTimeout: Duration(10 * time.Second),
	}
}

func TestSlurp_InvalidTarget(t *testing.T) {
	config := slurpTestConfig()
	ops := NewOperations(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("test content"))
	}))
	defer server.Close()

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key:        "test-key",
		URL:        server.URL,
		TargetName: "nonexistent-target",
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "invalid target: nonexistent-target")
}

func TestSlurp_ReadonlyTarget(t *testing.T) {
	targetName := "readonly-target-slurp"
	defer ClearNamedMemStorage(targetName)

	config := slurpTestConfig()
	config.StorageTargets = []StorageConfig{
		{
			Name:     targetName,
			Type:     Mem,
			Readonly: true,
			Bucket:   "readonly-bucket",
		},
	}
	ops := NewOperations(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("test content"))
	}))
	defer server.Close()

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key:        "test-key",
		URL:        server.URL,
		TargetName: targetName,
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "readonly")
}

func TestSlurp_WithMemTarget(t *testing.T) {
	targetName := "mem-target-slurp-basic"
	defer ClearNamedMemStorage(targetName)

	config := slurpTestConfig()
	config.StorageTargets = []StorageConfig{
		{
			Name:   targetName,
			Type:   Mem,
			Bucket: "test-bucket",
		},
	}
	ops := NewOperations(config)

	testContent := "hello world from slurp test"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(testContent))
	}))
	defer server.Close()

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key:        "slurped-file.txt",
		URL:        server.URL,
		TargetName: targetName,
	})

	require.NoError(t, result.Err)

	// Verify content was stored correctly
	storage := GetNamedMemStorage(targetName)
	reader, headers, err := storage.GetFile(ctx, "test-bucket", "slurped-file.txt")
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testContent, string(data))
	assert.Equal(t, "text/plain", headers.Get("Content-Type"))
}

func TestSlurp_WithContentTypeOverride(t *testing.T) {
	targetName := "mem-target-slurp-content-type"
	defer ClearNamedMemStorage(targetName)

	config := slurpTestConfig()
	config.StorageTargets = []StorageConfig{
		{
			Name:   targetName,
			Type:   Mem,
			Bucket: "test-bucket",
		},
	}
	ops := NewOperations(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write([]byte(`{"key": "value"}`))
	}))
	defer server.Close()

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key:         "file.json",
		URL:         server.URL,
		ContentType: "application/json",
		TargetName:  targetName,
	})

	require.NoError(t, result.Err)

	storage := GetNamedMemStorage(targetName)
	_, headers, err := storage.GetFile(ctx, "test-bucket", "file.json")
	require.NoError(t, err)
	assert.Equal(t, "application/json", headers.Get("Content-Type"))
}

func TestSlurp_MaxBytesExceeded(t *testing.T) {
	targetName := "mem-target-slurp-maxbytes"
	defer ClearNamedMemStorage(targetName)

	config := slurpTestConfig()
	config.StorageTargets = []StorageConfig{
		{
			Name:   targetName,
			Type:   Mem,
			Bucket: "test-bucket",
		},
	}
	ops := NewOperations(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "1000")
		w.Write([]byte("test content"))
	}))
	defer server.Close()

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key:        "file.txt",
		URL:        server.URL,
		MaxBytes:   10,
		TargetName: targetName,
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "Content-Length is greater than max bytes")
}

func TestSlurp_HTTPError(t *testing.T) {
	targetName := "mem-target-slurp-httperror"
	defer ClearNamedMemStorage(targetName)

	config := slurpTestConfig()
	config.StorageTargets = []StorageConfig{
		{
			Name:   targetName,
			Type:   Mem,
			Bucket: "test-bucket",
		},
	}
	ops := NewOperations(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key:        "file.txt",
		URL:        server.URL,
		TargetName: targetName,
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "failed to fetch file: 404")
}

func TestSlurp_WithACLAndContentDisposition(t *testing.T) {
	targetName := "mem-target-slurp-acl"
	defer ClearNamedMemStorage(targetName)

	config := slurpTestConfig()
	config.StorageTargets = []StorageConfig{
		{
			Name:   targetName,
			Type:   Mem,
			Bucket: "test-bucket",
		},
	}
	ops := NewOperations(config)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("test content"))
	}))
	defer server.Close()

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key:                "file.txt",
		URL:                server.URL,
		ACL:                "public-read",
		ContentDisposition: "attachment; filename=\"download.txt\"",
		TargetName:         targetName,
	})

	require.NoError(t, result.Err)

	storage := GetNamedMemStorage(targetName)
	_, headers, err := storage.GetFile(ctx, "test-bucket", "file.txt")
	require.NoError(t, err)
	assert.Equal(t, "public-read", headers.Get("x-acl"))
	assert.Equal(t, "attachment; filename=\"download.txt\"", headers.Get("Content-Disposition"))
}

func TestSlurp_InvalidURL(t *testing.T) {
	config := slurpTestConfig()
	ops := NewOperations(config)

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key: "file.txt",
		URL: "not-a-valid-url",
	})

	assert.Error(t, result.Err)
}

func TestSlurp_UsesServerContentType(t *testing.T) {
	targetName := "mem-target-slurp-server-ct"
	defer ClearNamedMemStorage(targetName)

	config := slurpTestConfig()
	config.StorageTargets = []StorageConfig{
		{
			Name:   targetName,
			Type:   Mem,
			Bucket: "test-bucket",
		},
	}
	ops := NewOperations(config)

	// Server returns explicit content-type
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("fake png data"))
	}))
	defer server.Close()

	ctx := context.Background()
	result := ops.Slurp(ctx, SlurpParams{
		Key:        "image.png",
		URL:        server.URL,
		TargetName: targetName,
	})

	require.NoError(t, result.Err)

	storage := GetNamedMemStorage(targetName)
	_, headers, err := storage.GetFile(ctx, "test-bucket", "image.png")
	require.NoError(t, err)
	// Should use content-type from server response
	assert.Equal(t, "image/png", headers.Get("Content-Type"))
}
