package zipserver

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"sync"
)

// alreadyCompressedExtensions contains file extensions that are already compressed
// and should not be pre-compressed
var alreadyCompressedExtensions = map[string]bool{
	".gz":   true,
	".br":   true,
	".zip":  true,
	".png":  true,
	".jpg":  true,
	".jpeg": true,
	".gif":  true,
	".webp": true,
	".mp3":  true,
	".mp4":  true,
	".webm": true,
	".ogg":  true,
	".flac": true,
	".rar":  true,
	".7z":   true,
	".bz2":  true,
	".xz":   true,
	".zst":  true,
}

// io.LimitReader takes int64, so this is the largest safe size we can pass
// without overflow in the pre-compression read path.
const maxPreCompressBufferSize = uint64(math.MaxInt64 - 1)

const (
	defaultPreCompressMaxConcurrent = 1
	preCompressCopyBufferSize       = 64 * 1024
)

type preCompressedFile struct {
	Reader  *os.File
	Size    uint64
	cleanup func()
}

func (f *preCompressedFile) Cleanup() {
	if f == nil || f.cleanup == nil {
		return
	}
	f.cleanup()
	f.cleanup = nil
}

type preCompressLimiter struct {
	slots chan struct{}
}

var (
	preCompressLimitersMu sync.Mutex
	preCompressLimiters   = map[int]*preCompressLimiter{}
)

func effectivePreCompressMaxConcurrent(config *Config) int {
	if config == nil || config.PreCompressMaxConcurrent <= 0 {
		return defaultPreCompressMaxConcurrent
	}
	return config.PreCompressMaxConcurrent
}

func getPreCompressLimiter(config *Config) *preCompressLimiter {
	maxConcurrent := effectivePreCompressMaxConcurrent(config)

	preCompressLimitersMu.Lock()
	defer preCompressLimitersMu.Unlock()

	if limiter, ok := preCompressLimiters[maxConcurrent]; ok {
		return limiter
	}

	limiter := &preCompressLimiter{
		slots: make(chan struct{}, maxConcurrent),
	}
	preCompressLimiters[maxConcurrent] = limiter
	return limiter
}

func (l *preCompressLimiter) acquire(ctx context.Context) error {
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *preCompressLimiter) release() {
	select {
	case <-l.slots:
	default:
	}
}

// shouldPreCompress checks if a file should be pre-compressed based on
// filename, size, and configuration
func shouldPreCompress(filename string, size uint64, config *Config) bool {
	if config == nil {
		return false
	}

	if !config.PreCompressEnabled {
		return false
	}

	if size > maxPreCompressBufferSize {
		return false
	}

	if config.PreCompressMinSize > 0 && size < uint64(config.PreCompressMinSize) {
		return false
	}

	ext := strings.ToLower(path.Ext(filename))
	if ext == "" {
		return false
	}

	// Skip already compressed files
	if alreadyCompressedExtensions[ext] {
		return false
	}

	// Check if extension matches configured extensions
	for _, allowedExt := range config.PreCompressExtensions {
		if ext == normalizeExtension(allowedExt) {
			return true
		}
	}

	return false
}

func normalizeExtension(ext string) string {
	ext = strings.TrimSpace(strings.ToLower(ext))
	if ext == "" {
		return ""
	}
	if !strings.HasPrefix(ext, ".") {
		return "." + ext
	}
	return ext
}

// preCompressStreamToTemp streams data to a temporary gzip file and returns it
// only if the compressed payload is smaller than the input payload.
func preCompressStreamToTemp(
	ctx context.Context,
	reader io.Reader,
	expectedSize uint64,
	config *Config,
) (*preCompressedFile, bool, error) {
	limiter := getPreCompressLimiter(config)
	if err := limiter.acquire(ctx); err != nil {
		return nil, false, err
	}
	defer limiter.release()

	if err := os.MkdirAll(tmpDir, os.ModeDir|0o777); err != nil {
		return nil, false, err
	}

	tempFile, err := os.CreateTemp(tmpDir, "zipserver-precompress-*.gz")
	if err != nil {
		return nil, false, err
	}

	cleanup := func() {
		tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}
	cleanupOnError := true
	defer func() {
		if cleanupOnError {
			cleanup()
		}
	}()

	var sourceSize uint64
	limited := limitedReader(reader, expectedSize, &sourceSize)
	writer, err := gzip.NewWriterLevel(tempFile, gzip.BestCompression)
	if err != nil {
		return nil, false, err
	}

	buffer := make([]byte, preCompressCopyBufferSize)
	_, err = io.CopyBuffer(writer, limited, buffer)
	if err != nil {
		_ = writer.Close()
		return nil, false, err
	}

	if err := writer.Close(); err != nil {
		return nil, false, err
	}

	info, err := tempFile.Stat()
	if err != nil {
		return nil, false, err
	}
	compressedSize := uint64(info.Size())

	if compressedSize >= sourceSize {
		return nil, false, nil
	}

	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return nil, false, err
	}

	cleanupOnError = false
	return &preCompressedFile{
		Reader:  tempFile,
		Size:    compressedSize,
		cleanup: cleanup,
	}, true, nil
}

// gzipCompress compresses data using gzip with best compression
func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(data)
	if err != nil {
		writer.Close()
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
