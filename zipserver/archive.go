package zipserver

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime"
	"os"
	"path"
	"strings"

	"archive/zip"
)

var (
	tmpDir = "zip_tmp"
)

type Archiver struct {
	*StorageClient
	*Config
	Aborted bool
}

type ExtractedFile struct {
	Key  string
	Size int
}

func NewArchiver(config *Config) *Archiver {
	storage, err := NewStorageClient(config)

	if storage == nil {
		log.Fatal("Failed to create storage:", err)
	}

	return &Archiver{storage, config, false}
}

func (a *Archiver) fetchZip(key string) (string, error) {
	os.MkdirAll(tmpDir, os.ModeDir|0777)

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
	a.Aborted = true

	for _, file := range files {
		a.StorageClient.DeleteFile(a.Bucket, file.Key)
	}

	return nil
}

func shouldIgnoreFile(fname string) bool {
	if strings.HasSuffix(fname, "/") {
		return true
	}

	if strings.Contains(fname, "..") {
		return true
	}

	if strings.Contains(fname, "__MACOSX/") {
		return true
	}

	if path.IsAbs(fname) {
		return true
	}

	return false
}

type UploadFileTask struct {
	File *zip.File
	Key  string
}

type UploadFileResult struct {
	Error bool
	Key   string
	Size  int
}

func uploadWorker(a *Archiver, limits *ExtractLimits, files <-chan *UploadFileTask, results chan<- UploadFileResult) {
	for task := range files {
		file := task.File
		key := task.Key

		written, err := a.sendZipFile(key, file, limits)

		if err != nil {
			log.Print("Failed sending: " + key + " " + err.Error())
			results <- UploadFileResult{true, key, 0}
			return
		}

		results <- UploadFileResult{false, key, written}
	}
}

// extracts and sends all files to prefix
func (a *Archiver) sendZipExtracted(prefix, fname string, limits *ExtractLimits) ([]ExtractedFile, error) {
	zipReader, err := zip.OpenReader(fname)
	if err != nil {
		return nil, err
	}

	if len(zipReader.File) > limits.MaxNumFiles {
		return nil, fmt.Errorf("Too many files in zip (%v > %v)",
			len(zipReader.File), limits.MaxNumFiles)
	}

	extractedFiles := []ExtractedFile{}

	defer zipReader.Close()

	fileCount := 0
	byteCount := uint64(0)
	uploadedCount := 0

	for _, file := range zipReader.File {
		if shouldIgnoreFile(file.Name) {
			continue
		}

		if len(file.Name) > limits.MaxFileNameLength {
			return nil, fmt.Errorf("Zip contains file paths that are too long")
		}

		if file.UncompressedSize64 > uint64(limits.MaxFileSize) {
			return nil, fmt.Errorf("Zip contains file that is too large (%s)", file.Name)
		}

		byteCount += file.UncompressedSize64

		if byteCount > uint64(limits.MaxTotalSize) {
			return nil, fmt.Errorf("Extracted zip too large (max %v bytes)", limits.MaxTotalSize)
		}
	}

	files := make(chan *UploadFileTask, len(zipReader.File))
	results := make(chan UploadFileResult, len(zipReader.File))

	for i := 1; i <= 8; i++ {
		go uploadWorker(a, limits, files, results)
	}

	for _, file := range zipReader.File {
		if shouldIgnoreFile(file.Name) {
			continue
		}

		key := path.Join(prefix, file.Name)
		files <- &UploadFileTask{file, key}
	}

	for i := 0; i < len(zipReader.File); i++ {
		result := <-results

		if result.Error {
			a.abortUpload(extractedFiles)
			close(files)
			return nil, err
		}

		extractedFiles = append(extractedFiles, ExtractedFile{result.Key, result.Size})
		uploadedCount += result.Size

		if uploadedCount > limits.MaxTotalSize {
			a.abortUpload(extractedFiles)
			close(files)
			return nil, fmt.Errorf("Extracted zip too large (max %v bytes)", limits.MaxTotalSize)
		}

		fileCount++
	}

	close(files)
	close(results)

	log.Printf("Sent %d files", fileCount)
	return extractedFiles, nil
}

// sends an individual file from zip
func (a *Archiver) sendZipFile(key string, file *zip.File, limits *ExtractLimits) (int, error) {
	if (a.Aborted) {
		return 0, fmt.Errorf("Archive upload has been aborted")
	}

	mimeType := mime.TypeByExtension(path.Ext(key))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	log.Print("Sending: " + key + " (" + mimeType + ")")

	bytesRead := 0

	reader, _ := file.Open()
	limited := limitedReader(reader, limits.MaxFileSize, &bytesRead)
	err := a.StorageClient.PutFile(a.Bucket, key, limited, mimeType)

	if err != nil {
		return bytesRead, err
	}

	return bytesRead, nil
}

func (a *Archiver) ExtractZip(key, prefix string, limits *ExtractLimits) ([]ExtractedFile, error) {
	fname, err := a.fetchZip(key)
	if err != nil {
		return nil, err
	}

	defer os.Remove(fname)
	prefix = path.Join(a.ExtractPrefix, prefix)
	return a.sendZipExtracted(prefix, fname, limits)
}
