package zipserver

import (
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
// and should not be compressed
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
// without overflow in the compression read path.
const maxCompressBufferSize = uint64(math.MaxInt64 - 1)

const (
	defaultCompressMaxConcurrent = 1
	defaultCompressLevel         = 7
	compressCopyBufferSize       = 64 * 1024
)

type compressedFile struct {
	Reader  *os.File
	Size    uint64
	cleanup func()
}

func (f *compressedFile) Cleanup() {
	if f == nil || f.cleanup == nil {
		return
	}
	f.cleanup()
	f.cleanup = nil
}

type compressLimiter struct {
	slots chan struct{}
}

var (
	compressLimitersMu sync.Mutex
	compressLimiters   = map[int]*compressLimiter{}
)

func effectiveCompressMaxConcurrent(config *CompressionConfig) int {
	if config == nil || config.MaxConcurrent <= 0 {
		return defaultCompressMaxConcurrent
	}
	return config.MaxConcurrent
}

// effectiveCompressLevel returns the configured gzip level, falling back to
// the default when unset (zero) or outside gzip's valid range.
func effectiveCompressLevel(config *CompressionConfig) int {
	if config == nil || config.Level == 0 {
		return defaultCompressLevel
	}
	if config.Level < gzip.HuffmanOnly || config.Level > gzip.BestCompression {
		return defaultCompressLevel
	}
	return config.Level
}

func getCompressLimiter(config *CompressionConfig) *compressLimiter {
	maxConcurrent := effectiveCompressMaxConcurrent(config)

	compressLimitersMu.Lock()
	defer compressLimitersMu.Unlock()

	if limiter, ok := compressLimiters[maxConcurrent]; ok {
		return limiter
	}

	limiter := &compressLimiter{
		slots: make(chan struct{}, maxConcurrent),
	}
	compressLimiters[maxConcurrent] = limiter
	return limiter
}

func (l *compressLimiter) acquire(ctx context.Context) error {
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *compressLimiter) release() {
	select {
	case <-l.slots:
	default:
	}
}

// shouldCompress checks if a file should be compressed based on
// filename, size, and configuration
func shouldCompress(filename string, size uint64, config *CompressionConfig) bool {
	if config == nil {
		return false
	}

	if !config.Enabled {
		return false
	}

	if size > maxCompressBufferSize {
		return false
	}

	if config.MinSize > 0 && size < uint64(config.MinSize) {
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
	for _, allowedExt := range config.Extensions {
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

// compressStreamToTemp streams data to a temporary gzip file and returns it
// only if the compressed payload is smaller than the input payload.
func compressStreamToTemp(
	ctx context.Context,
	reader io.Reader,
	expectedSize uint64,
	config *CompressionConfig,
) (*compressedFile, bool, error) {
	limiter := getCompressLimiter(config)
	if err := limiter.acquire(ctx); err != nil {
		return nil, false, err
	}
	defer limiter.release()

	if err := os.MkdirAll(tmpDir, os.ModeDir|0o777); err != nil {
		return nil, false, err
	}

	tempFile, err := os.CreateTemp(tmpDir, "zipserver-compress-*.gz")
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
	writer, err := gzip.NewWriterLevel(tempFile, effectiveCompressLevel(config))
	if err != nil {
		return nil, false, err
	}

	buffer := make([]byte, compressCopyBufferSize)
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
	return &compressedFile{
		Reader:  tempFile,
		Size:    compressedSize,
		cleanup: cleanup,
	}, true, nil
}
