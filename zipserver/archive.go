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
}

type ExtractedFile struct {
	Key  string
	Size uint64
}

func NewArchiver(config *Config) *Archiver {
	storage, err := NewStorageClient(config)

	if storage == nil {
		log.Fatal("Failed to create storage:", err)
	}

	return &Archiver{storage, config}
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
	Error error
	Key   string
	Size  uint64
}

func uploadWorker(a *Archiver, limits *ExtractLimits, tasks <-chan UploadFileTask, results chan<- UploadFileResult, cancel chan struct{}, done chan struct{}) {
	defer func() { done <- struct{}{} }()

	for task := range tasks {
		file := task.File
		key := task.Key

		written, err := a.sendZipFile(key, file, limits)

		if err != nil {
			log.Print("Failed sending " + key + ": " + err.Error())
			results <- UploadFileResult{err, key, 0}
			return
		}

		results <- UploadFileResult{nil, key, written}
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
	var byteCount uint64

	fileList := []*zip.File{}

	for _, file := range zipReader.File {
		if shouldIgnoreFile(file.Name) {
			continue
		}

		if len(file.Name) > limits.MaxFileNameLength {
			return nil, fmt.Errorf("Zip contains file paths that are too long")
		}

		if file.UncompressedSize64 > limits.MaxFileSize {
			return nil, fmt.Errorf("Zip contains file that is too large (%s)", file.Name)
		}

		byteCount += file.UncompressedSize64

		if byteCount > limits.MaxTotalSize {
			return nil, fmt.Errorf("Extracted zip too large (max %v bytes)", limits.MaxTotalSize)
		}

		fileList = append(fileList, file)
	}

	tasks := make(chan UploadFileTask)
	results := make(chan UploadFileResult)
	cancel := make(chan struct{})
	done := make(chan struct{}, limits.ExtractionThreads)

	for i := 0; i < limits.ExtractionThreads; i++ {
		go uploadWorker(a, limits, tasks, results, cancel, done)
	}

	activeWorkers := limits.ExtractionThreads

	go func() {
		defer func() { close(tasks) }()
		for _, file := range fileList {
			key := path.Join(prefix, file.Name)
			task := UploadFileTask{file, key}
			select {
				case tasks <- task:
				case <-cancel:
					// Something went wrong!
					return
			}
		}
	}()

	var extractError error

	for activeWorkers > 0 {
		select {
			case result := <- results:
				if result.Error != nil {
					extractError = result.Error
					close(cancel)
				} else {
					extractedFiles = append(extractedFiles, ExtractedFile{result.Key, result.Size})
					fileCount++
				}
			case <- done:
				activeWorkers--
		}
	}

	close(results)

	if extractError != nil {
		log.Printf("Upload error: %s", extractError.Error());
		a.abortUpload(extractedFiles)
		return nil, extractError
	}

	log.Printf("Sent %d files", fileCount)
	return extractedFiles, nil
}

// sends an individual file from zip
func (a *Archiver) sendZipFile(key string, file *zip.File, limits *ExtractLimits) (uint64, error) {
	mimeType := mime.TypeByExtension(path.Ext(key))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	log.Print("Sending: " + key + " (" + mimeType + ")")

	var bytesRead uint64

	reader, _ := file.Open()
	limited := limitedReader(reader, file.UncompressedSize64, &bytesRead)
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
