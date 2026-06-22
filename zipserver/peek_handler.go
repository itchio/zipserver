package zipserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/gzip"
)

const defaultPeekBytes uint64 = 4096

func effectiveMaxPeekBytes(config *Config) uint64 {
	if config == nil || config.MaxPeekBytes == 0 {
		return defaultConfig.MaxPeekBytes
	}
	return config.MaxPeekBytes
}

// DefaultPeekBytes returns the default number of bytes a peek reads, clamped to
// the configured MaxPeekBytes so a bare request still works when MaxPeekBytes is
// configured below defaultPeekBytes.
func DefaultPeekBytes(config *Config) uint64 {
	maxBytes := defaultPeekBytes
	if max := effectiveMaxPeekBytes(config); maxBytes > max {
		maxBytes = max
	}
	return maxBytes
}

func resolveReadStorage(config *Config, targetName string) (Storage, string, error) {
	if targetName == "" {
		storage, err := NewGcsStorage(config)
		if err != nil {
			return nil, "", fmt.Errorf("failed to create primary storage: %v", err)
		}
		return storage, config.Bucket, nil
	}

	storageTargetConfig := config.GetStorageTargetByName(targetName)
	if storageTargetConfig == nil {
		return nil, "", fmt.Errorf("invalid target: %s", targetName)
	}

	storage, err := storageTargetConfig.NewStorageClient()
	if err != nil {
		return nil, "", fmt.Errorf("failed to create target storage: %v", err)
	}

	return storage, storageTargetConfig.Bucket, nil
}

// Peek reads up to MaxBytes from a stored object, decoding supported Content-Encoding values.
func (o *Operations) Peek(ctx context.Context, params PeekParams) PeekResult {
	if params.Key == "" {
		return PeekResult{Err: fmt.Errorf("key is required")}
	}
	if params.MaxBytes == 0 {
		return PeekResult{Err: fmt.Errorf("bytes must be positive")}
	}
	if maxPeekBytes := effectiveMaxPeekBytes(o.config); maxPeekBytes > 0 && params.MaxBytes > maxPeekBytes {
		return PeekResult{Err: fmt.Errorf("bytes exceeds maximum (%d > %d)", params.MaxBytes, maxPeekBytes)}
	}

	storage, bucket, err := resolveReadStorage(o.config, params.TargetName)
	if err != nil {
		return PeekResult{Err: err}
	}

	getCtx, cancel := context.WithTimeout(ctx, time.Duration(o.config.FileGetTimeout))
	defer cancel()

	reader, headers, err := storage.GetFile(getCtx, bucket, params.Key)
	if err != nil {
		return PeekResult{Err: fmt.Errorf("failed to get file: %v", err)}
	}
	defer reader.Close()

	contentType := headers.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentEncoding := headers.Get("Content-Encoding")

	var body io.Reader = reader
	decoded := false
	if contentEncoding != "" {
		switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
		case "gzip":
			gzReader, err := gzip.NewReader(reader)
			if err != nil {
				return PeekResult{Err: fmt.Errorf("failed to decompress gzip stream: %v", err)}
			}
			defer gzReader.Close()
			body = gzReader
			decoded = true
		default:
			return PeekResult{Err: fmt.Errorf("unsupported content encoding for peek decompression: %s", contentEncoding)}
		}
	}

	data, err := io.ReadAll(io.LimitReader(body, int64(params.MaxBytes)))
	if err != nil {
		return PeekResult{Err: fmt.Errorf("failed to read file: %v", err)}
	}

	return PeekResult{
		Key:             params.Key,
		Bucket:          bucket,
		Data:            data,
		ContentType:     contentType,
		ContentEncoding: contentEncoding,
		Decoded:         decoded,
	}
}

func peekHandler(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return fmt.Errorf("failed to parse form: %w", err)
	}

	params := r.Form
	key, err := getParam(params, "key")
	if err != nil {
		return err
	}

	// An explicit oversized bytes value is still rejected by Peek.
	maxBytes := DefaultPeekBytes(globalConfig)
	if bytesStr := params.Get("bytes"); bytesStr != "" {
		maxBytes, err = strconv.ParseUint(bytesStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid bytes value: %s", bytesStr)
		}
	}

	ops := NewOperations(globalConfig)
	result := ops.Peek(r.Context(), PeekParams{
		Key:        key,
		TargetName: params.Get("target"),
		MaxBytes:   maxBytes,
	})
	if result.Err != nil {
		return result.Err
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Zipserver-Source-Content-Type", result.ContentType)
	if result.ContentEncoding != "" {
		w.Header().Set("X-Zipserver-Source-Content-Encoding", result.ContentEncoding)
	}
	w.Header().Set("X-Zipserver-Decoded", strconv.FormatBool(result.Decoded))
	w.Header().Set("X-Zipserver-Peek-Bytes", strconv.Itoa(len(result.Data)))
	_, err = w.Write(result.Data)
	return err
}
