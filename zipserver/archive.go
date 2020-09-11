package zipserver

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path"
	"strings"

	"archive/zip"

	errors "github.com/go-errors/errors"
)

var (
	tmpDir = "zip_tmp"
)

func init() {
	mime.AddExtensionType(".unityweb", "application/octet-stream")
}

// Archiver holds together the storage along with configuration values
// (credentials, limits etc.)
type Archiver struct {
	Storage
	*Config
}

// ExtractedFile represents a file extracted from a .zip into a GCS bucket
type ExtractedFile struct {
	Key  string
	Size uint64
}

// NewArchiver creates a new archiver from the given config
func NewArchiver(config *Config) *Archiver {
	storage, err := NewGcsStorage(config)

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

	src, err := a.Storage.GetFile(a.Bucket, key)

	if err != nil {
		return "", errors.Wrap(err, 0)
	}

	defer src.Close()

	dest, err := os.Create(fname)

	if err != nil {
		return "", errors.Wrap(err, 0)
	}

	defer dest.Close()

	_, err = io.Copy(dest, src)
	if err != nil {
		return "", errors.Wrap(err, 0)
	}

	return fname, nil
}

// delete all files that have been uploaded so far
func (a *Archiver) abortUpload(files []ExtractedFile) error {
	for _, file := range files {
		// FIXME: code quality - what if we fail here? any retry strategies?
		a.Storage.DeleteFile(a.Bucket, file.Key)
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

	if strings.Contains(fname, ".git/") {
		return true
	}

	if path.IsAbs(fname) {
		return true
	}

	return false
}

// UploadFileTask contains the information needed to extract a single file from a .zip
type UploadFileTask struct {
	File *zip.File
	Key  string
}

// UploadFileResult is successful is Error is nil - in that case, it contains the
// GCS key the file was uploaded under, and the number of bytes written for that file.
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

		resource, err := a.extractAndUploadOne(key, file, limits)

		if err != nil {
			log.Print("Failed sending " + key + ": " + err.Error())
			results <- UploadFileResult{err, key, 0}
			return
		}

		results <- UploadFileResult{nil, resource.key, resource.size}
	}
}

// extracts and sends all files to prefix
func (a *Archiver) sendZipExtracted(prefix, fname string, limits *ExtractLimits) ([]ExtractedFile, error) {
	zipReader, err := zip.OpenReader(fname)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	if len(zipReader.File) > limits.MaxNumFiles {
		err := fmt.Errorf("Too many files in zip (%v > %v)",
			len(zipReader.File), limits.MaxNumFiles)
		return nil, errors.Wrap(err, 0)
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
			err := fmt.Errorf("Zip contains file paths that are too long")
			return nil, errors.Wrap(err, 0)
		}

		if file.UncompressedSize64 > limits.MaxFileSize {
			err := fmt.Errorf("Zip contains file that is too large (%s)", file.Name)
			return nil, errors.Wrap(err, 0)
		}

		byteCount += file.UncompressedSize64

		if byteCount > limits.MaxTotalSize {
			err := fmt.Errorf("Extracted zip too large (max %v bytes)", limits.MaxTotalSize)
			return nil, errors.Wrap(err, 0)
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
		case result := <-results:
			if result.Error != nil {
				extractError = result.Error
				close(cancel)
			} else {
				extractedFiles = append(extractedFiles, ExtractedFile{result.Key, result.Size})
				fileCount++
			}
		case <-done:
			activeWorkers--
		}
	}

	close(results)

	if extractError != nil {
		log.Printf("Upload error: %s", extractError.Error())
		a.abortUpload(extractedFiles)
		return nil, extractError
	}

	log.Printf("Sent %d files", fileCount)
	return extractedFiles, nil
}

// sends an individual file from a zip
func (a *Archiver) extractAndUploadOne(key string, file *zip.File, limits *ExtractLimits) (*ResourceSpec, error) {
	readerCloser, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer readerCloser.Close()

	var reader io.Reader = readerCloser

	resource := &ResourceSpec{
		key: key,
	}

	// try determining MIME by extension
	mimeType := mime.TypeByExtension(path.Ext(key))

	if mimeType == "" {
		var buffer bytes.Buffer

		_, err = io.Copy(&buffer, io.LimitReader(reader, 512))

		mimeType = http.DetectContentType(buffer.Bytes())

		// join the bytes read and the original reader
		reader = io.MultiReader(&buffer, reader)
	}

	if mimeType == "" {
		// default mime type
		mimeType = "application/octet-stream"
	}
	resource.contentType = mimeType

	resource.applyContentEncodingRules()
	resource.applyRewriteRules()

	log.Printf("Sending: %s", resource)

	limited := limitedReader(reader, file.UncompressedSize64, &resource.size)

	err = a.Storage.PutFileWithSetup(a.Bucket, resource.key, limited, resource.setupRequest)
	if err != nil {
		return resource, errors.Wrap(err, 0)
	}

	return resource, nil
}

// ExtractZip downloads the zip at `key` to a temporary file,
// then extracts its contents and uploads each item to `prefix`
func (a *Archiver) ExtractZip(key, prefix string, limits *ExtractLimits) ([]ExtractedFile, error) {
	fname, err := a.fetchZip(key)
	if err != nil {
		return nil, err
	}

	defer os.Remove(fname)
	prefix = path.Join(a.ExtractPrefix, prefix)
	return a.sendZipExtracted(prefix, fname, limits)
}

func (a *Archiver) UploadZipFromFile(fname string, prefix string, limits *ExtractLimits) ([]ExtractedFile, error) {
	prefix = path.Join("_zipserver", prefix)
	return a.sendZipExtracted(prefix, fname, limits)
}
