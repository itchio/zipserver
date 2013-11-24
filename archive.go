package zip_server

import (
	"os"
	"io"
	"encoding/hex"
	"crypto/md5"
	"path"
	"strings"
	"log"
	"mime"

	"archive/zip"
)

var (
	tmpDir = "zip_tmp"
)

type Archiver struct {
	*StorageClient
	bucket string
}

func NewArchiver(client *StorageClient, bucket string) *Archiver {
	return &Archiver{client, bucket}
}

func (a *Archiver) FetchZip(key string) string {
	os.MkdirAll(tmpDir, os.ModeDir | 0777)

	hasher := md5.New()
	hasher.Write([]byte(key))
	fname := a.bucket + "_" + hex.EncodeToString(hasher.Sum(nil)) + ".zip"
	fname = path.Join(tmpDir, fname)

	src, _ := a.StorageClient.GetFile(a.bucket, key)
	defer src.Close()

	dest, _ := os.Create(fname)
	defer dest.Close()

	io.Copy(dest, src)

	return fname
}

func (a *Archiver) SendZipExtracted(prefix, fname string) {
	zipReader, _ := zip.OpenReader(fname)
	defer zipReader.Close()

	file_count := 0
	byte_count := int64(0)

	for _, file := range zipReader.File {
		if strings.HasSuffix(file.Name, "/") {
			continue
		}

		if strings.Contains(file.Name, "..") {
			continue
		}

		if path.IsAbs(file.Name) {
			continue
		}

		key := path.Join(prefix, file.Name)
		byte_count += a.SendZipFile(key, file)
		file_count += 1
	}

	log.Printf("Sent %d files", file_count)
}

// returns the bytes written
func (a *Archiver) SendZipFile(key string, file *zip.File) int64 {
	mimeType := mime.TypeByExtension(path.Ext(key))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	log.Print("Sending: " + key + " (" + mimeType + ")")

	reader, _ := file.Open()
	a.StorageClient.PutFile(a.bucket, key, reader, mimeType)
	return 0
}
