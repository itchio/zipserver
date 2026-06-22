package zipserver

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func peekTestConfig(targetName string) *Config {
	return &Config{
		Bucket:         "test-bucket",
		ExtractPrefix:  "extract/",
		MaxPeekBytes:   1024,
		FileGetTimeout: Duration(30 * time.Second),
		StorageTargets: []StorageConfig{
			{
				Name:   targetName,
				Type:   Mem,
				Bucket: "target-bucket",
			},
		},
	}
}

func gzipBytes(t *testing.T, input []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(input)
	if err != nil {
		t.Fatalf("gzip write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip close failed: %v", err)
	}

	return buf.Bytes()
}

func TestPeekOperationPlainFile(t *testing.T) {
	targetName := "mem-target-peek-plain"
	config := peekTestConfig(targetName)
	storage := GetNamedMemStorage(targetName)
	defer ClearNamedMemStorage(targetName)

	_, err := storage.PutFile(context.Background(), "target-bucket", "test/file.txt", strings.NewReader("hello world"), PutOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("put file: %v", err)
	}

	result := NewOperations(config).Peek(context.Background(), PeekParams{
		Key:        "test/file.txt",
		TargetName: targetName,
		MaxBytes:   5,
	})
	if result.Err != nil {
		t.Fatalf("peek failed: %v", result.Err)
	}

	if string(result.Data) != "hello" {
		t.Fatalf("peek data = %q, want %q", result.Data, "hello")
	}
	if result.ContentType != "text/plain" {
		t.Fatalf("content type = %q, want text/plain", result.ContentType)
	}
	if result.Decoded {
		t.Fatalf("plain file should not be marked decoded")
	}
}

func TestPeekOperationGzipFile(t *testing.T) {
	targetName := "mem-target-peek-gzip"
	config := peekTestConfig(targetName)
	storage := GetNamedMemStorage(targetName)
	defer ClearNamedMemStorage(targetName)

	plain := []byte("hello compressed world")
	compressed := gzipBytes(t, plain)
	_, err := storage.PutFile(context.Background(), "target-bucket", "test/file.txt", bytes.NewReader(compressed), PutOptions{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
	})
	if err != nil {
		t.Fatalf("put file: %v", err)
	}

	result := NewOperations(config).Peek(context.Background(), PeekParams{
		Key:        "test/file.txt",
		TargetName: targetName,
		MaxBytes:   11,
	})
	if result.Err != nil {
		t.Fatalf("peek failed: %v", result.Err)
	}

	if string(result.Data) != "hello compr" {
		t.Fatalf("peek data = %q, want %q", result.Data, "hello compr")
	}
	if result.ContentEncoding != "gzip" {
		t.Fatalf("content encoding = %q, want gzip", result.ContentEncoding)
	}
	if !result.Decoded {
		t.Fatalf("gzip file should be marked decoded")
	}

}

func TestPeekOperationValidation(t *testing.T) {
	targetName := "mem-target-peek-validation"
	config := peekTestConfig(targetName)
	defer ClearNamedMemStorage(targetName)

	ops := NewOperations(config)
	if result := ops.Peek(context.Background(), PeekParams{TargetName: targetName, MaxBytes: 1}); result.Err == nil {
		t.Fatalf("expected missing key error")
	}
	if result := ops.Peek(context.Background(), PeekParams{Key: "x", TargetName: targetName}); result.Err == nil {
		t.Fatalf("expected missing bytes error")
	}
	if result := ops.Peek(context.Background(), PeekParams{Key: "x", TargetName: targetName, MaxBytes: 2048}); result.Err == nil {
		t.Fatalf("expected over max bytes error")
	}
}

func TestPeekOperationUnsupportedEncoding(t *testing.T) {
	targetName := "mem-target-peek-br"
	config := peekTestConfig(targetName)
	storage := GetNamedMemStorage(targetName)
	defer ClearNamedMemStorage(targetName)

	_, err := storage.PutFile(context.Background(), "target-bucket", "test/file.br", strings.NewReader("not actually brotli"), PutOptions{
		ContentEncoding: "br",
	})
	if err != nil {
		t.Fatalf("put file: %v", err)
	}

	result := NewOperations(config).Peek(context.Background(), PeekParams{
		Key:        "test/file.br",
		TargetName: targetName,
		MaxBytes:   4,
	})
	if result.Err == nil {
		t.Fatalf("expected unsupported encoding error")
	}
}

func TestPeekHandler(t *testing.T) {
	targetName := "mem-target-peek-handler"
	globalConfig = peekTestConfig(targetName)
	storage := GetNamedMemStorage(targetName)
	defer ClearNamedMemStorage(targetName)

	_, err := storage.PutFile(context.Background(), "target-bucket", "test/file.txt", strings.NewReader("hello world"), PutOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("put file: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/peek?target="+targetName+"&key=test/file.txt&bytes=5", nil)
	w := httptest.NewRecorder()
	if err := peekHandler(w, req); err != nil {
		t.Fatalf("peekHandler failed: %v", err)
	}

	res := w.Result()
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if string(data) != "hello" {
		t.Fatalf("response data = %q, want hello", data)
	}
	if got := res.Header.Get("X-Zipserver-Source-Content-Type"); got != "text/plain" {
		t.Fatalf("source content type header = %q, want text/plain", got)
	}
	if got := res.Header.Get("X-Zipserver-Decoded"); got != "false" {
		t.Fatalf("decoded header = %q, want false", got)
	}
	if got := res.Header.Get("X-Zipserver-Peek-Bytes"); got != "5" {
		t.Fatalf("peek bytes header = %q, want 5", got)
	}
}

// A bare request (no bytes param) must succeed even when MaxPeekBytes is
// configured below defaultPeekBytes; the default is clamped to the cap.
func TestPeekHandlerDefaultBytesUnderCap(t *testing.T) {
	targetName := "mem-target-peek-default"
	globalConfig = peekTestConfig(targetName) // MaxPeekBytes: 1024 < defaultPeekBytes
	storage := GetNamedMemStorage(targetName)
	defer ClearNamedMemStorage(targetName)

	_, err := storage.PutFile(context.Background(), "target-bucket", "test/file.txt", strings.NewReader("hello world"), PutOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("put file: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/peek?target="+targetName+"&key=test/file.txt", nil)
	w := httptest.NewRecorder()
	if err := peekHandler(w, req); err != nil {
		t.Fatalf("peekHandler failed: %v", err)
	}

	res := w.Result()
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if string(data) != "hello world" {
		t.Fatalf("response data = %q, want %q", data, "hello world")
	}
}
