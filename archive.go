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
	maxBytes = 1024*1024
)

type Archiver struct {
	*StorageClient
	bucket string
}

type readerClosure func(p []byte) (int, error)

func (fn readerClosure) Read(p []byte) (int, error) {
	return fn(p)
}

// debug reader
func annotatedReader(reader io.Reader) readerClosure {
	return func (p []byte) (int, error) {
		bytes_read, err := reader.Read(p)
		log.Printf("Read %d bytes", bytes_read)
		return bytes_read, err
	}
}

// wraps a reader to fail if it reads more than max of maxBytes
func limitedReader(reader io.Reader, maxBytes int) readerClosure {
	remainingBytes := maxBytes
	return func (p []byte) (int, error) {
		bytes_read, err := reader.Read(p)
		remainingBytes -= bytes_read

		if remainingBytes < 0 {
			return bytes_read, fmt.Errorf("limited reader: read more than %d bytes", maxBytes)
		}
		return bytes_read, err
	}
}

func NewArchiver(client *StorageClient, bucket string) *Archiver {
	return &Archiver{client, bucket}
}

func (a *Archiver) FetchZip(key string) (string, error) {
	os.MkdirAll(tmpDir, os.ModeDir | 0777)

	hasher := md5.New()
	hasher.Write([]byte(key))
	fname := a.bucket + "_" + hex.EncodeToString(hasher.Sum(nil)) + ".zip"
	fname = path.Join(tmpDir, fname)

	src, err := a.StorageClient.GetFile(a.bucket, key)

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

// extracts and sends all files to prefix
func (a *Archiver) SendZipExtracted(prefix, fname string) error {
	zipReader, err := zip.OpenReader(fname)
	if err != nil {
		return err
	}

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
		written, err := a.sendZipFile(key, file)

		if err != nil {
			log.Print("Failed sending: " + key + " " + err.Error())
			continue
		}

		byte_count += written
		file_count += 1
	}

	log.Printf("Sent %d files", file_count)
	return nil
}

// sends an individual file from zip
func (a *Archiver) sendZipFile(key string, file *zip.File) (int64, error) {
	mimeType := mime.TypeByExtension(path.Ext(key))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	log.Print("Sending: " + key + " (" + mimeType + ")")

	reader, _ := file.Open()
	err := a.StorageClient.PutFile(a.bucket, key, limitedReader(reader, maxBytes), mimeType)

	if err != nil {
		return 0, err
	}

	return 0, nil
}

func (a *Archiver) ExtractZip(key, prefix string) error {
	fname, err := a.FetchZip(key)
	if err != nil {
		return err
	}

	defer os.Remove(fname)
	return a.SendZipExtracted(prefix, fname)
}
