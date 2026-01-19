package zipserver

import (
	"bytes"
	"compress/gzip"
	"path"
	"strings"
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

// shouldPreCompress checks if a file should be pre-compressed based on
// filename, size, and configuration
func shouldPreCompress(filename string, size uint64, config *Config) bool {
	if !config.PreCompressEnabled {
		return false
	}

	if int64(size) < config.PreCompressMinSize {
		return false
	}

	ext := strings.ToLower(path.Ext(filename))

	// Skip already compressed files
	if alreadyCompressedExtensions[ext] {
		return false
	}

	// Check if extension matches configured extensions
	for _, allowedExt := range config.PreCompressExtensions {
		if strings.EqualFold(ext, allowedExt) {
			return true
		}
	}

	return false
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
