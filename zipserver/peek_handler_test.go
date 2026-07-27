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

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zip"
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

func TestPeekOperationBrotliFile(t *testing.T) {
	targetName := "mem-target-peek-br"
	config := peekTestConfig(targetName)
	storage := GetNamedMemStorage(targetName)
	defer ClearNamedMemStorage(targetName)

	plain := []byte("hello brotli world")
	var buf bytes.Buffer
	writer := brotli.NewWriter(&buf)
	if _, err := writer.Write(plain); err != nil {
		t.Fatalf("brotli write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("brotli close failed: %v", err)
	}

	_, err := storage.PutFile(context.Background(), "target-bucket", "test/file.txt", bytes.NewReader(buf.Bytes()), PutOptions{
		ContentType:     "text/plain",
		ContentEncoding: "br",
	})
	if err != nil {
		t.Fatalf("put file: %v", err)
	}

	result := NewOperations(config).Peek(context.Background(), PeekParams{
		Key:        "test/file.txt",
		TargetName: targetName,
		MaxBytes:   12,
	})
	if result.Err != nil {
		t.Fatalf("peek failed: %v", result.Err)
	}

	if string(result.Data) != "hello brotli" {
		t.Fatalf("peek data = %q, want %q", result.Data, "hello brotli")
	}
	if result.ContentEncoding != "br" {
		t.Fatalf("content encoding = %q, want br", result.ContentEncoding)
	}
	if !result.Decoded {
		t.Fatalf("brotli file should be marked decoded")
	}
}

func TestPeekOperationUnsupportedEncoding(t *testing.T) {
	targetName := "mem-target-peek-unsupported"
	config := peekTestConfig(targetName)
	storage := GetNamedMemStorage(targetName)
	defer ClearNamedMemStorage(targetName)

	_, err := storage.PutFile(context.Background(), "target-bucket", "test/file.zst", strings.NewReader("not actually zstd"), PutOptions{
		ContentEncoding: "zstd",
	})
	if err != nil {
		t.Fatalf("put file: %v", err)
	}

	result := NewOperations(config).Peek(context.Background(), PeekParams{
		Key:        "test/file.zst",
		TargetName: targetName,
		MaxBytes:   4,
	})
	if result.Err == nil {
		t.Fatalf("expected unsupported encoding error")
	}
}

// Extracting into a brotli-enabled target must produce objects Peek can decode.
func TestPeekAfterBrotliExtract(t *testing.T) {
	targetName := "mem-target-peek-br-extract"
	config := peekTestConfig(targetName)
	config.MaxPeekBytes = 4096
	config.ExtractionThreads = 4
	config.JobTimeout = Duration(10 * time.Second)
	config.FilePutTimeout = Duration(10 * time.Second)
	storage := GetNamedMemStorage(targetName)
	defer ClearNamedMemStorage(targetName)

	htmlContent := bytes.Repeat([]byte("<html><body>Hello Peek!</body></html>"), 50)

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	(&zipLayout{entries: []zipEntry{{name: "index.html", data: htmlContent}}}).Write(t, zw)
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close failed: %v", err)
	}

	ctx := context.Background()
	source, _ := NewMemStorage()
	zipPath := "peek_extract_test.zip"
	if _, err := source.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), PutOptions{}); err != nil {
		t.Fatalf("put zip: %v", err)
	}

	archiver := &ArchiveExtractor{
		Storage:       source,
		Config:        config,
		TargetStorage: storage,
		TargetBucket:  "target-bucket",
		Compression: &CompressionConfig{
			Enabled:    true,
			MinSize:    100,
			Extensions: []string{".html"},
			Format:     CompressFormatBrotli,
		},
	}

	files, err := archiver.ExtractZip(ctx, zipPath, "peek_extract", testLimits())
	if err != nil {
		t.Fatalf("extract failed: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 extracted file, got %d", len(files))
	}

	result := NewOperations(config).Peek(context.Background(), PeekParams{
		Key:        files[0].Key,
		TargetName: targetName,
		MaxBytes:   uint64(len(htmlContent)),
	})
	if result.Err != nil {
		t.Fatalf("peek failed: %v", result.Err)
	}

	if result.ContentEncoding != "br" {
		t.Fatalf("content encoding = %q, want br", result.ContentEncoding)
	}
	if !result.Decoded {
		t.Fatalf("extracted brotli file should be marked decoded")
	}
	if !bytes.Equal(result.Data, htmlContent) {
		t.Fatalf("peek data does not match original content")
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
