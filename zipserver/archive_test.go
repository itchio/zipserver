package zipserver

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLimits() *ExtractLimits {
	return &ExtractLimits{
		MaxFileSize:       1024 * 1024 * 200,
		MaxTotalSize:      1024 * 1024 * 500,
		MaxNumFiles:       100,
		MaxFileNameLength: 80,
		ExtractionThreads: 4,
	}
}

func emptyConfig() *Config {
	return &Config{
		Bucket:            "testbucket",
		ExtractionThreads: 8,
		JobTimeout:        Duration(10 * time.Second),
		FileGetTimeout:    Duration(10 * time.Second),
		FilePutTimeout:    Duration(10 * time.Second),
	}
}

func Test_ExtractOnGCS(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		archiver := &ArchiveExtractor{Storage: storage, Config: config}

		r, err := os.Open("/home/leafo/code/go/etlua.zip")
		assert.NoError(t, err)
		defer r.Close()

		err = storage.PutFile(ctx, config.Bucket, "zipserver_test/test.zip", r, PutOptions{
			ContentType: "application/zip",
			ACL:         ACLPublicRead,
		})
		assert.NoError(t, err)

		_, err = archiver.ExtractZip(ctx, "zipserver_test/test.zip", "zipserver_test/extract", testLimits())
		assert.NoError(t, err)
	})
}

type zipEntry struct {
	name                    string
	outName                 string
	data                    []byte
	expectedMimeType        string
	expectedContentEncoding string
	ignored                 bool
}

type zipLayout struct {
	entries []zipEntry
}

func (zl *zipLayout) Write(t *testing.T, zw *zip.Writer) {
	for _, entry := range zl.entries {
		writer, err := zw.CreateHeader(&zip.FileHeader{
			Name:               entry.name,
			UncompressedSize64: uint64(len(entry.data)),
		})
		assert.NoError(t, err)

		_, err = io.Copy(writer, bytes.NewReader(entry.data))
		assert.NoError(t, err)
	}
}

func (zl *zipLayout) Check(t *testing.T, storage *MemStorage, bucket, prefix string) {
	ctx := context.Background()

	for _, entry := range zl.entries {
		func() {
			name := entry.name
			if entry.outName != "" {
				name = entry.outName
			}

			path := fmt.Sprintf("%s/%s", prefix, name)
			reader, _, err := storage.GetFile(ctx, bucket, path)
			if entry.ignored {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), "object not found"))
				return
			}

			assert.NoError(t, err)

			defer reader.Close()

			data, err := io.ReadAll(reader)
			assert.NoError(t, err)
			assert.EqualValues(t, data, entry.data)

			h, err := storage.getHeaders(bucket, path)
			assert.NoError(t, err)
			assert.EqualValues(t, entry.expectedMimeType, h.Get("content-type"))
			assert.EqualValues(t, "public-read", h.Get("x-acl"))

			if entry.expectedContentEncoding != "" {
				assert.EqualValues(t, entry.expectedContentEncoding, h.Get("content-encoding"))
			}
		}()
	}
}

func Test_ExtractInMemory(t *testing.T) {
	config := emptyConfig()

	ctx := context.Background()

	storage, err := NewMemStorage()
	assert.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	prefix := "zipserver_test/mem_test_extracted"
	zipPath := "mem_test.zip"

	_, err = archiver.ExtractZip(ctx, zipPath, prefix, testLimits())
	assert.Error(t, err)

	withZip := func(zl *zipLayout, cb func(zl *zipLayout)) {
		var buf bytes.Buffer

		zw := zip.NewWriter(&buf)

		zl.Write(t, zw)

		err = zw.Close()
		assert.NoError(t, err)

		err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), PutOptions{
			ContentType: "application/octet-stream",
			ACL:         ACLPublicRead,
		})
		assert.NoError(t, err)

		cb(zl)
	}

	withZip(&zipLayout{
		entries: []zipEntry{
			zipEntry{
				name:             "file.txt",
				data:             []byte("Hello there"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "garbage.bin",
				data:             bytes.Repeat([]byte{3, 1, 5, 3, 2, 6, 1, 2, 5, 3, 4, 6, 2}, 20),
				expectedMimeType: "application/octet-stream",
			},
			zipEntry{
				name:             "something.gz",
				data:             []byte{0x1F, 0x8B, 0x08, 1, 5, 2, 4, 9, 3, 1, 2, 5},
				expectedMimeType: "application/gzip",
			},
			zipEntry{
				name:                    "something.unityweb",
				data:                    []byte{0x1F, 0x8B, 0x08, 9, 1, 5, 2, 3, 5, 2, 6, 4, 4},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gamedata.memgz",
				outName:                 "gamedata.mem",
				data:                    []byte{0x1F, 0x8B, 0x08, 1, 5, 2, 3, 1, 2, 1, 2},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gamedata.jsgz",
				outName:                 "gamedata.js",
				data:                    []byte{0x1F, 0x8B, 0x08, 3, 7, 3, 4, 12, 53, 26, 34},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gamedata.asm.jsgz",
				outName:                 "gamedata.asm.js",
				data:                    []byte{0x1F, 0x8B, 0x08, 62, 34, 128, 37, 10, 39, 82},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gamedata.datagz",
				outName:                 "gamedata.data",
				data:                    []byte{0x1F, 0x8B, 0x08, 8, 5, 23, 1, 25, 38},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "bundle.wasm.br",
				outName:                 "bundle.wasm.br",
				data:                    []byte("not really brotli"),
				expectedMimeType:        "application/wasm",
				expectedContentEncoding: "br",
			},
			zipEntry{
				name:                    "readme.txt.br",
				outName:                 "readme.txt.br",
				data:                    []byte("brotli compressed text"),
				expectedMimeType:        "text/plain; charset=utf-8",
				expectedContentEncoding: "br",
			},
			zipEntry{
				name:                    "mystery.bin.br",
				outName:                 "mystery.bin.br",
				data:                    []byte("not really brotli either"),
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "br",
			},
			zipEntry{
				name:    "__MACOSX/hello",
				data:    []byte{},
				ignored: true,
			},
			zipEntry{
				name:    "/woops/hi/im/absolute",
				data:    []byte{},
				ignored: true,
			},
			zipEntry{
				name:    "oh/hey/im/a/dir/",
				data:    []byte{},
				ignored: true,
			},
			zipEntry{
				name:    "im/trying/to/escape/../../../../../../etc/hosts",
				data:    []byte{},
				ignored: true,
			},
		},
	}, func(zl *zipLayout) {
		_, err := archiver.ExtractZip(ctx, zipPath, prefix, testLimits())
		assert.NoError(t, err)

		zl.Check(t, storage, config.Bucket, prefix)
	})

	withZip(&zipLayout{
		entries: []zipEntry{
			zipEntry{
				name:             strings.Repeat("x", 101),
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
		},
	}, func(zl *zipLayout) {
		limits := testLimits()
		limits.MaxFileNameLength = 100

		_, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "paths that are too long"))
	})

	withZip(&zipLayout{
		entries: []zipEntry{
			zipEntry{
				name:             "x",
				data:             bytes.Repeat([]byte("oh no"), 100),
				expectedMimeType: "text/plain; charset=utf-8",
			},
		},
	}, func(zl *zipLayout) {
		limits := testLimits()
		limits.MaxFileSize = 499

		_, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "file that is too large"))
	})

	withZip(&zipLayout{
		entries: []zipEntry{
			zipEntry{
				name:             "1",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "2",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "3",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "4",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
		},
	}, func(zl *zipLayout) {
		limits := testLimits()
		limits.MaxNumFiles = 3

		_, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "Too many files"))
	})

	withZip(&zipLayout{
		entries: []zipEntry{
			zipEntry{
				name:             "1",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "2",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "3",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "4",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
		},
	}, func(zl *zipLayout) {
		limits := testLimits()
		limits.MaxTotalSize = 6

		_, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "zip too large"))
	})

	// reset storage for this next test
	storage, err = NewMemStorage()
	assert.NoError(t, err)
	storage.planForFailure(config.Bucket, fmt.Sprintf("%s/%s", prefix, "3"))
	storage.putDelay = 200 * time.Millisecond
	archiver = &ArchiveExtractor{Storage: storage, Config: config}

	withZip(&zipLayout{
		entries: []zipEntry{
			zipEntry{
				name:             "1",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "2",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "3",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
			zipEntry{
				name:             "4",
				data:             []byte("uh oh"),
				expectedMimeType: "text/plain; charset=utf-8",
			},
		},
	}, func(zl *zipLayout) {
		limits := testLimits()

		_, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "intentional failure"))

		assert.EqualValues(t, 1, len(storage.objects), "make sure all objects have been cleaned up")
		for k := range storage.objects {
			assert.EqualValues(t, k, storage.objectPath(config.Bucket, zipPath), "make sure the only remaining object is the zip")
		}
	})
}

// TestFetchZipFailing simulates a download failing after the ouptut file has been created,
// and makes sure the incomplete file is removed.
func TestFetchZipFailing(t *testing.T) {
	rand.Seed(time.Now().Unix())
	bucket := "bucket" + strconv.Itoa(rand.Int())
	key := "key" + strconv.Itoa(rand.Int())
	path := fetchZipFilename(bucket, key)
	path = filepath.Join(tmpDir, path)
	require.False(t, fileExists(path), "test output file existed ahead of time")
	t.Logf("temp file: %s", path)

	a := &ArchiveExtractor{
		Storage: &mockFailingStorage{t, path},
		Config: &Config{
			Bucket: bucket,
		},
	}

	ctx := context.Background()
	_, err := a.fetchZip(ctx, key)
	assert.EqualError(t, err, "intentional failure")
	assert.False(t, fileExists(path), "file should have been removed")
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	panic("unexpected error from stat: " + err.Error())
}

type mockFailingStorage struct {
	t    *testing.T
	path string
}

func (m *mockFailingStorage) GetFile(_ context.Context, _, _ string) (io.ReadCloser, http.Header, error) {
	return &mockFailingReadCloser{m.t, m.path}, nil, nil
}

func (m *mockFailingStorage) PutFile(_ context.Context, _, _ string, contents io.Reader, _ PutOptions) error {
	return nil
}

func (m *mockFailingStorage) DeleteFile(_ context.Context, _, _ string) error {
	return nil
}

type mockFailingReadCloser struct {
	t    *testing.T
	path string
}

func (m *mockFailingReadCloser) Read(p []byte) (int, error) {
	assert.True(m.t, fileExists(m.path), "file should have been created")
	return 0, errors.New("intentional failure")
}

func (m *mockFailingReadCloser) Close() error {
	return nil
}
