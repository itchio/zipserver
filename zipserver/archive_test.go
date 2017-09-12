package zipserver

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
	}
}

func Test_ExtractOnGCS(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		archiver := &Archiver{storage, config}

		r, err := os.Open("/home/leafo/code/go/etlua.zip")
		assert.NoError(t, err)
		defer r.Close()

		err = storage.PutFile(config.Bucket, "zipserver_test/test.zip", r, "application/zip")
		assert.NoError(t, err)

		_, err = archiver.ExtractZip("zipserver_test/test.zip", "zipserver_test/extract", testLimits())
		assert.NoError(t, err)
	})
}

type zipEntry struct {
	name                    string
	outName                 string
	data                    []byte
	expectedMimeType        string
	expectedContentEncoding string
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
	for _, entry := range zl.entries {
		func() {
			name := entry.name
			if entry.outName != "" {
				name = entry.outName
			}

			path := fmt.Sprintf("%s/%s", prefix, name)
			reader, err := storage.GetFile(bucket, path)
			assert.NoError(t, err)

			defer reader.Close()

			data, err := ioutil.ReadAll(reader)
			assert.NoError(t, err)
			assert.EqualValues(t, data, entry.data)

			h, err := storage.getHeaders(bucket, path)
			assert.NoError(t, err)
			assert.EqualValues(t, entry.expectedMimeType, h.Get("content-type"))

			if entry.expectedContentEncoding != "" {
				assert.EqualValues(t, entry.expectedContentEncoding, h.Get("content-encoding"))
			}
		}()
	}
}

func Test_ExtractInMemory(t *testing.T) {
	config := emptyConfig()

	storage, err := NewMemStorage()
	assert.NoError(t, err)

	archiver := &Archiver{storage, config}

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	zl := &zipLayout{
		entries: []zipEntry{
			zipEntry{
				name:             "file.txt",
				data:             []byte("Hello there"),
				expectedMimeType: "text/plain",
			},
			zipEntry{
				name:             "garbage.bin",
				data:             bytes.Repeat([]byte{3, 1, 5, 3, 2, 6, 1, 2, 5, 3, 4, 6, 2}, 20),
				expectedMimeType: "application/octet-stream",
			},
			zipEntry{
				name:                    "something.gz",
				data:                    []byte{0x1F, 0x8B, 0x08, 1, 5, 2, 4, 9, 3, 1, 2, 5},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gzip-without-extension",
				data:                    []byte{0x1F, 0x8B, 0x08, 9, 1, 5, 2, 3, 5, 2, 6, 4, 4},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gamedata.memgz",
				outName:                 "gamedata.mem",
				data:                    []byte{0x1F, 0x8B, 0x08, 9, 1, 5, 2, 3, 5, 2, 6, 4, 4},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gamedata.jsgz",
				outName:                 "gamedata.js",
				data:                    []byte{0x1F, 0x8B, 0x08, 9, 1, 5, 2, 3, 5, 2, 6, 4, 4},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gamedata.asm.jsgz",
				outName:                 "gamedata.asm.js",
				data:                    []byte{0x1F, 0x8B, 0x08, 9, 1, 5, 2, 3, 5, 2, 6, 4, 4},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
			zipEntry{
				name:                    "gamedata.datagz",
				outName:                 "gamedata.data",
				data:                    []byte{0x1F, 0x8B, 0x08, 9, 1, 5, 2, 3, 5, 2, 6, 4, 4},
				expectedMimeType:        "application/octet-stream",
				expectedContentEncoding: "gzip",
			},
		},
	}
	zl.Write(t, zw)

	err = zw.Close()
	assert.NoError(t, err)

	zipPath := "mem_test.zip"
	err = storage.PutFile(config.Bucket, zipPath, bytes.NewReader(buf.Bytes()), "application/octet-stream")
	assert.NoError(t, err)

	prefix := "zipserver_test/mem_test_extracted"

	_, err = archiver.ExtractZip(zipPath, prefix, testLimits())
	assert.NoError(t, err)

	zl.Check(t, storage, config.Bucket, prefix)
}
