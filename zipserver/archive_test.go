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

func readTestZip(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "test_files", name)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func Test_ExtractOnGCS(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		ctx := context.Background()
		archiver := &ArchiveExtractor{Storage: storage, Config: config}

		r, err := os.Open("/home/leafo/code/go/etlua.zip")
		assert.NoError(t, err)
		defer r.Close()

		_, err = storage.PutFile(ctx, config.Bucket, "zipserver_test/test.zip", r, PutOptions{
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

		_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), PutOptions{
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

func Test_ExtractEmptyZip(t *testing.T) {
	config := emptyConfig()
	ctx := context.Background()

	storage, err := NewMemStorage()
	assert.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	prefix := "zipserver_test/empty_zip_test"
	zipPath := "empty_test.zip"

	// Create an empty zip file (no entries)
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	err = zw.Close()
	assert.NoError(t, err)

	_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), PutOptions{})
	assert.NoError(t, err)

	files, err := archiver.ExtractZip(ctx, zipPath, prefix, testLimits())
	assert.NoError(t, err)
	assert.Len(t, files, 0)
}

func Test_ExtractRejectsUnderreportedSize(t *testing.T) {
	config := emptyConfig()
	ctx := context.Background()

	storage, err := NewMemStorage()
	require.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	zipPath := "zipserver_test/lie_underreported.zip"

	zipBytes := readTestZip(t, "zip_lie_underreported_size.zip")
	_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(zipBytes), PutOptions{})
	require.NoError(t, err)

	limits := testLimits()
	_, err = archiver.ExtractZip(ctx, zipPath, "zipserver_test/extract", limits)
	assert.Error(t, err)
}

func Test_ExtractRejectsOverreportedSize(t *testing.T) {
	config := emptyConfig()
	ctx := context.Background()

	storage, err := NewMemStorage()
	require.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	zipPath := "zipserver_test/lie_overreported.zip"

	zipBytes := readTestZip(t, "zip_lie_overreported_size.zip")
	_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(zipBytes), PutOptions{})
	require.NoError(t, err)

	limits := testLimits()
	limits.MaxFileSize = 1024
	_, err = archiver.ExtractZip(ctx, zipPath, "zipserver_test/extract", limits)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "Zip contains file that is too large"))
}

func Test_ExtractRejectsBadCRC(t *testing.T) {
	config := emptyConfig()
	ctx := context.Background()

	storage, err := NewMemStorage()
	require.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	zipPath := "zipserver_test/lie_bad_crc.zip"

	zipBytes := readTestZip(t, "zip_lie_bad_crc.zip")
	_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(zipBytes), PutOptions{})
	require.NoError(t, err)

	limits := testLimits()
	_, err = archiver.ExtractZip(ctx, zipPath, "zipserver_test/extract", limits)
	assert.Error(t, err)
	if err != nil {
		msg := err.Error()
		assert.True(t, strings.Contains(msg, "checksum") || strings.Contains(msg, "corrupt input"))
	}
}

func Test_ExtractRejectsZipBombBySizeLimits(t *testing.T) {
	config := emptyConfig()
	ctx := context.Background()

	storage, err := NewMemStorage()
	require.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	zipPath := "zipserver_test/zip_bomb.zip"

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	writer, err := zw.CreateHeader(&zip.FileHeader{
		Name:               "bomb.txt",
		Method:             zip.Deflate,
		UncompressedSize64: 1024 * 1024,
	})
	require.NoError(t, err)
	_, err = writer.Write(bytes.Repeat([]byte("A"), 1024*1024))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), PutOptions{})
	require.NoError(t, err)

	limits := testLimits()
	limits.MaxFileSize = 128 * 1024
	limits.MaxTotalSize = 128 * 1024

	_, err = archiver.ExtractZip(ctx, zipPath, "zipserver_test/extract", limits)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "too large"))
}

func Test_ExtractRejectsMaxInputZipSize(t *testing.T) {
	config := emptyConfig()
	ctx := context.Background()

	storage, err := NewMemStorage()
	require.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	zipPath := "zipserver_test/max_input_zip_size.zip"

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	writer, err := zw.CreateHeader(&zip.FileHeader{
		Name:               "small.txt",
		Method:             zip.Deflate,
		UncompressedSize64: 1024,
	})
	require.NoError(t, err)
	_, err = writer.Write(bytes.Repeat([]byte("A"), 1024))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), PutOptions{})
	require.NoError(t, err)

	limits := testLimits()
	limits.MaxInputZipSize = 1

	_, err = archiver.ExtractZip(ctx, zipPath, "zipserver_test/extract", limits)
	assert.Error(t, err)
	if err != nil {
		msg := err.Error()
		assert.True(t, strings.Contains(msg, "zip too large"))
	}
}

func Test_GlobFiltering(t *testing.T) {
	config := emptyConfig()
	ctx := context.Background()

	storage, err := NewMemStorage()
	assert.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	prefix := "zipserver_test/glob_test"
	zipPath := "glob_test.zip"

	withZip := func(entries []zipEntry, cb func()) {
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		(&zipLayout{entries: entries}).Write(t, zw)
		err := zw.Close()
		assert.NoError(t, err)
		_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), PutOptions{})
		assert.NoError(t, err)
		cb()
	}

	t.Run("filter by extension", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "image.png", data: []byte("png data")},
			{name: "script.js", data: []byte("js data")},
			{name: "style.css", data: []byte("css data")},
		}, func() {
			limits := testLimits()
			limits.IncludeGlob = "*.png"

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 1)
			assert.Contains(t, files[0].Key, "image.png")
		})
	})

	t.Run("filter with directory glob", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "assets/img/logo.png", data: []byte("logo")},
			{name: "assets/img/banner.png", data: []byte("banner")},
			{name: "assets/css/style.css", data: []byte("css")},
			{name: "readme.txt", data: []byte("readme")},
		}, func() {
			limits := testLimits()
			limits.IncludeGlob = "assets/**/*.png"

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 2)
		})
	})

	t.Run("empty filter extracts all", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "a.txt", data: []byte("a")},
			{name: "b.txt", data: []byte("b")},
		}, func() {
			limits := testLimits()
			limits.IncludeGlob = ""

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 2)
		})
	})

	t.Run("invalid pattern returns error", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "test.txt", data: []byte("test")},
		}, func() {
			limits := testLimits()
			limits.IncludeGlob = "[invalid"

			_, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid glob pattern")
		})
	})

	t.Run("filter no matches extracts nothing", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "file.txt", data: []byte("text")},
			{name: "data.json", data: []byte("json")},
		}, func() {
			limits := testLimits()
			limits.IncludeGlob = "*.png"

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 0)
		})
	})
}

func Test_OnlyFilesFiltering(t *testing.T) {
	config := emptyConfig()
	ctx := context.Background()

	storage, err := NewMemStorage()
	assert.NoError(t, err)

	archiver := &ArchiveExtractor{Storage: storage, Config: config}
	prefix := "zipserver_test/only_files_test"
	zipPath := "only_files_test.zip"

	withZip := func(entries []zipEntry, cb func()) {
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		(&zipLayout{entries: entries}).Write(t, zw)
		err := zw.Close()
		assert.NoError(t, err)
		_, err = storage.PutFile(ctx, config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), PutOptions{})
		assert.NoError(t, err)
		cb()
	}

	t.Run("extract specific files only", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "file1.txt", data: []byte("file1")},
			{name: "file2.txt", data: []byte("file2")},
			{name: "file3.txt", data: []byte("file3")},
		}, func() {
			limits := testLimits()
			limits.OnlyFiles = []string{"file1.txt", "file3.txt"}

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 2)

			names := make(map[string]bool)
			for _, f := range files {
				names[f.Key] = true
			}
			assert.True(t, names[prefix+"/file1.txt"])
			assert.True(t, names[prefix+"/file3.txt"])
			assert.False(t, names[prefix+"/file2.txt"])
		})
	})

	t.Run("missing files silently skipped", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "exists.txt", data: []byte("exists")},
		}, func() {
			limits := testLimits()
			limits.OnlyFiles = []string{"exists.txt", "does_not_exist.txt"}

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 1)
			assert.Contains(t, files[0].Key, "exists.txt")
		})
	})

	t.Run("empty only_files extracts all", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "a.txt", data: []byte("a")},
			{name: "b.txt", data: []byte("b")},
		}, func() {
			limits := testLimits()
			limits.OnlyFiles = []string{}

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 2)
		})
	})

	t.Run("only_files with subdirectory paths", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "dir/file1.txt", data: []byte("file1")},
			{name: "dir/file2.txt", data: []byte("file2")},
			{name: "other/file.txt", data: []byte("other")},
		}, func() {
			limits := testLimits()
			limits.OnlyFiles = []string{"dir/file1.txt", "other/file.txt"}

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 2)
		})
	})

	t.Run("only_files no matches extracts nothing", func(t *testing.T) {
		withZip([]zipEntry{
			{name: "file.txt", data: []byte("text")},
			{name: "data.json", data: []byte("json")},
		}, func() {
			limits := testLimits()
			limits.OnlyFiles = []string{"nonexistent.txt"}

			files, err := archiver.ExtractZip(ctx, zipPath, prefix, limits)
			assert.NoError(t, err)
			assert.Len(t, files, 0)
		})
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
	_, err := a.fetchZip(ctx, key, 0)
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

func (m *mockFailingStorage) PutFile(_ context.Context, _, _ string, contents io.Reader, _ PutOptions) (PutResult, error) {
	return PutResult{}, nil
}

func (m *mockFailingStorage) DeleteFile(_ context.Context, _, _ string) error {
	return nil
}

func (m *mockFailingStorage) GetReaderAt(_ context.Context, _, _ string, _ uint64) (ReaderAtCloser, int64, error) {
	return nil, 0, errors.New("not implemented")
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
