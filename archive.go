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
	"fmt"

	"archive/zip"
)

var (
	tmpDir = "zip_tmp"
)

type Archiver struct {
	*StorageClient
	*Config
}

type ExtractedFile struct {
	Key string
	Size int
}

type readerClosure func(p []byte) (int, error)

func (fn readerClosure) Read(p []byte) (int, error) {
	return fn(p)
}

// debug reader
func annotatedReader(reader io.Reader) readerClosure {
	return func (p []byte) (int, error) {
		bytesRead, err := reader.Read(p)
		log.Printf("Read %d bytes", bytesRead)
		return bytesRead, err
	}
}

// wraps a reader to fail if it reads more than max of maxBytes, also tracks
// the total amount of bytes read
func limitedReader(reader io.Reader, maxBytes int, totalBytes *int) readerClosure {
	remainingBytes := maxBytes
	return func (p []byte) (int, error) {
		bytesRead, err := reader.Read(p)
		remainingBytes -= bytesRead

		*totalBytes += bytesRead

		if remainingBytes < 0 {
			return bytesRead, fmt.Errorf("limited reader: read more than %d bytes", maxBytes)
		}

		return bytesRead, err
	}
}

func NewArchiver(config *Config) *Archiver {
	storage := NewStorageClient(config)
	return &Archiver{storage, config}
}

func (a *Archiver) fetchZip(key string) (string, error) {
	os.MkdirAll(tmpDir, os.ModeDir | 0777)

	hasher := md5.New()
	hasher.Write([]byte(key))
	fname := a.Bucket + "_" + hex.EncodeToString(hasher.Sum(nil)) + ".zip"
	fname = path.Join(tmpDir, fname)

	src, err := a.StorageClient.GetFile(a.Bucket, key)

	if err != nil {
		return "", err
	}

	defer src.Close()

	dest, err := os.Create(fname)

	if err != nil {
		return "", err
	}

	defer dest.Close()

	_, err = io.Copy(dest, src)
	if err != nil {
		return "", err
	}

	return fname, nil
}

// delete all files that have been uploaded so far
func (a *Archiver) abortUpload(files []ExtractedFile) error {
	for _, file := range files {
		a.StorageClient.DeleteFile(a.Bucket, file.Key)
	}

	return nil
}

// extracts and sends all files to prefix
func (a *Archiver) sendZipExtracted(prefix, fname string) ([]ExtractedFile, error) {
	zipReader, err := zip.OpenReader(fname)
	if err != nil {
		return nil, err
	}

	extractedFiles := []ExtractedFile{}

	defer zipReader.Close()

	file_count := 0
	byte_count := 0

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
		written, err := a.sendZipFile(key, file)

		if err != nil {
			log.Print("Failed sending: " + key + " " + err.Error())
			a.abortUpload(extractedFiles)
			return nil, err
		}

		extractedFiles = append(extractedFiles, ExtractedFile{key, int(written)})
		byte_count += written
		file_count += 1
	}

	log.Printf("Sent %d files", file_count)
	return extractedFiles, nil
}

// sends an individual file from zip
func (a *Archiver) sendZipFile(key string, file *zip.File) (int, error) {
	mimeType := mime.TypeByExtension(path.Ext(key))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	log.Print("Sending: " + key + " (" + mimeType + ")")

	bytesRead := 0

	reader, _ := file.Open()
	limited := limitedReader(reader, a.MaxFileSize, &bytesRead)
	err := a.StorageClient.PutFile(a.Bucket, key, limited, mimeType)

	if err != nil {
		return bytesRead, err
	}

	return bytesRead, nil
}

func (a *Archiver) ExtractZip(key, prefix string) ([]ExtractedFile, error) {
	fname, err := a.fetchZip(key)
	if err != nil {
		return nil, err
	}

	defer os.Remove(fname)
	prefix = path.Join(a.ExtractPrefix, prefix)
	return a.sendZipExtracted(prefix, fname)
}
