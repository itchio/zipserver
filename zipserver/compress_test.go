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

func TestShouldCompress(t *testing.T) {
	baseConfig := &CompressionConfig{
		Enabled:    true,
		MinSize:    1024,
		Extensions: []string{".html", ".js", ".css", ".svg"},
	}

	tests := []struct {
		name     string
		filename string
		size     uint64
		config   *CompressionConfig
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
			config: &CompressionConfig{
				Enabled:    false,
				MinSize:    1024,
				Extensions: []string{".html"},
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
			config: &CompressionConfig{
				Enabled:    true,
				MinSize:    1024,
				Extensions: []string{"html"},
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
			name:     "too large to safely compress",
			filename: "test.html",
			size:     uint64(math.MaxInt64),
			config:   baseConfig,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldCompress(tt.filename, tt.size, tt.config)
			if got != tt.want {
				t.Errorf("shouldCompress(%q, %d) = %v, want %v", tt.filename, tt.size, got, tt.want)
			}
		})
	}
}

func TestEffectiveCompressLevel(t *testing.T) {
	cases := []struct {
		name   string
		config *CompressionConfig
		want   int
	}{
		{"nil config", nil, defaultCompressLevel},
		{"unset (zero) falls back to default", &CompressionConfig{}, defaultCompressLevel},
		{"configured valid level", &CompressionConfig{Level: 3}, 3},
		{"best compression", &CompressionConfig{Level: gzip.BestCompression}, gzip.BestCompression},
		{"huffman only", &CompressionConfig{Level: gzip.HuffmanOnly}, gzip.HuffmanOnly},
		{"above range falls back to default", &CompressionConfig{Level: 10}, defaultCompressLevel},
		{"below range falls back to default", &CompressionConfig{Level: -3}, defaultCompressLevel},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveCompressLevel(tc.config); got != tc.want {
				t.Errorf("effectiveCompressLevel = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCompressStreamToTemp(t *testing.T) {
	t.Run("returns compressed temp file when smaller", func(t *testing.T) {
		input := bytes.Repeat([]byte("Hello, World! This is compressible data. "), 200)

		compressed, used, err := compressStreamToTemp(
			context.Background(),
			bytes.NewReader(input),
			uint64(len(input)),
			&CompressionConfig{},
		)
		if err != nil {
			t.Fatalf("compressStreamToTemp failed: %v", err)
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

		compressed, used, err := compressStreamToTemp(
			context.Background(),
			bytes.NewReader(input),
			uint64(len(input)),
			&CompressionConfig{},
		)
		if err != nil {
			t.Fatalf("compressStreamToTemp failed: %v", err)
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

		_, _, err := compressStreamToTemp(
			context.Background(),
			bytes.NewReader(input),
			128,
			&CompressionConfig{},
		)
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("expected ErrLimitExceeded, got %v", err)
		}
	})
}
