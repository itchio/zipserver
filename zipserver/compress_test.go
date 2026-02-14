package zipserver

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"math"
	"testing"
)

func TestShouldPreCompress(t *testing.T) {
	baseConfig := &Config{
		PreCompressEnabled:    true,
		PreCompressMinSize:    1024,
		PreCompressExtensions: []string{".html", ".js", ".css", ".svg"},
	}

	tests := []struct {
		name     string
		filename string
		size     uint64
		config   *Config
		want     bool
	}{
		{
			name:     "nil config",
			filename: "test.html",
			size:     2000,
			config:   nil,
			want:     false,
		},
		{
			name:     "feature disabled",
			filename: "test.html",
			size:     2000,
			config: &Config{
				PreCompressEnabled:    false,
				PreCompressMinSize:    1024,
				PreCompressExtensions: []string{".html"},
			},
			want: false,
		},
		{
			name:     "file too small",
			filename: "test.html",
			size:     500,
			config:   baseConfig,
			want:     false,
		},
		{
			name:     "already compressed gz",
			filename: "test.js.gz",
			size:     2000,
			config:   baseConfig,
			want:     false,
		},
		{
			name:     "already compressed png",
			filename: "image.png",
			size:     2000,
			config:   baseConfig,
			want:     false,
		},
		{
			name:     "already compressed jpg",
			filename: "photo.jpg",
			size:     2000,
			config:   baseConfig,
			want:     false,
		},
		{
			name:     "already compressed zip",
			filename: "archive.zip",
			size:     2000,
			config:   baseConfig,
			want:     false,
		},
		{
			name:     "matching extension html",
			filename: "index.html",
			size:     2000,
			config:   baseConfig,
			want:     true,
		},
		{
			name:     "matching extension js",
			filename: "app.js",
			size:     2000,
			config:   baseConfig,
			want:     true,
		},
		{
			name:     "matching extension css",
			filename: "style.css",
			size:     2000,
			config:   baseConfig,
			want:     true,
		},
		{
			name:     "matching extension svg",
			filename: "icon.svg",
			size:     2000,
			config:   baseConfig,
			want:     true,
		},
		{
			name:     "non-matching extension",
			filename: "data.json",
			size:     2000,
			config:   baseConfig,
			want:     false,
		},
		{
			name:     "case insensitive extension matching",
			filename: "TEST.HTML",
			size:     2000,
			config:   baseConfig,
			want:     true,
		},
		{
			name:     "case insensitive extension JS",
			filename: "bundle.JS",
			size:     2000,
			config:   baseConfig,
			want:     true,
		},
		{
			name:     "configured extension without dot",
			filename: "index.html",
			size:     2000,
			config: &Config{
				PreCompressEnabled:    true,
				PreCompressMinSize:    1024,
				PreCompressExtensions: []string{"html"},
			},
			want: true,
		},
		{
			name:     "nested path with matching extension",
			filename: "assets/scripts/main.js",
			size:     2000,
			config:   baseConfig,
			want:     true,
		},
		{
			name:     "exactly at minimum size",
			filename: "test.html",
			size:     1024,
			config:   baseConfig,
			want:     true,
		},
		{
			name:     "one byte below minimum size",
			filename: "test.html",
			size:     1023,
			config:   baseConfig,
			want:     false,
		},
		{
			name:     "too large to safely pre-compress",
			filename: "test.html",
			size:     uint64(math.MaxInt64),
			config:   baseConfig,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldPreCompress(tt.filename, tt.size, tt.config)
			if got != tt.want {
				t.Errorf("shouldPreCompress(%q, %d) = %v, want %v", tt.filename, tt.size, got, tt.want)
			}
		})
	}
}

func TestGzipCompress(t *testing.T) {
	t.Run("valid compression", func(t *testing.T) {
		// Use compressible data (repetitive text compresses well)
		input := bytes.Repeat([]byte("Hello, World! This is a test of compression. "), 100)

		compressed, err := gzipCompress(input)
		if err != nil {
			t.Fatalf("gzipCompress failed: %v", err)
		}

		if len(compressed) >= len(input) {
			t.Errorf("compressed data (%d bytes) should be smaller than input (%d bytes)", len(compressed), len(input))
		}
	})

	t.Run("decompression verification", func(t *testing.T) {
		input := []byte("Test data for compression and decompression verification")

		compressed, err := gzipCompress(input)
		if err != nil {
			t.Fatalf("gzipCompress failed: %v", err)
		}

		// Decompress and verify
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			t.Fatalf("gzip.NewReader failed: %v", err)
		}
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("io.ReadAll failed: %v", err)
		}

		if !bytes.Equal(decompressed, input) {
			t.Errorf("decompressed data does not match input")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		input := []byte{}

		compressed, err := gzipCompress(input)
		if err != nil {
			t.Fatalf("gzipCompress failed on empty input: %v", err)
		}

		// Decompress and verify it's empty
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			t.Fatalf("gzip.NewReader failed: %v", err)
		}
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("io.ReadAll failed: %v", err)
		}

		if len(decompressed) != 0 {
			t.Errorf("decompressed empty input should be empty, got %d bytes", len(decompressed))
		}
	})
}

func TestPreCompressStreamToTemp(t *testing.T) {
	t.Run("returns compressed temp file when smaller", func(t *testing.T) {
		input := bytes.Repeat([]byte("Hello, World! This is compressible data. "), 200)

		compressed, used, err := preCompressStreamToTemp(
			context.Background(),
			bytes.NewReader(input),
			uint64(len(input)),
			&Config{PreCompressMaxConcurrent: 1},
		)
		if err != nil {
			t.Fatalf("preCompressStreamToTemp failed: %v", err)
		}
		if !used {
			t.Fatalf("expected compression to be used")
		}
		defer compressed.Cleanup()

		reader, err := gzip.NewReader(compressed.Reader)
		if err != nil {
			t.Fatalf("gzip.NewReader failed: %v", err)
		}
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("io.ReadAll failed: %v", err)
		}
		if !bytes.Equal(decompressed, input) {
			t.Fatalf("decompressed data mismatch")
		}
	})

	t.Run("skips when compressed output is larger", func(t *testing.T) {
		input := []byte{}

		compressed, used, err := preCompressStreamToTemp(
			context.Background(),
			bytes.NewReader(input),
			uint64(len(input)),
			&Config{PreCompressMaxConcurrent: 1},
		)
		if err != nil {
			t.Fatalf("preCompressStreamToTemp failed: %v", err)
		}
		if used {
			t.Fatalf("expected compression to be skipped")
		}
		if compressed != nil {
			t.Fatalf("expected nil compressed file when skipped")
		}
	})

	t.Run("returns limit exceeded error when source exceeds expected size", func(t *testing.T) {
		input := bytes.Repeat([]byte("x"), 2048)

		_, _, err := preCompressStreamToTemp(
			context.Background(),
			bytes.NewReader(input),
			128,
			&Config{PreCompressMaxConcurrent: 1},
		)
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("expected ErrLimitExceeded, got %v", err)
		}
	})
}
