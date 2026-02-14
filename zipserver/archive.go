package zipserver

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/klauspost/compress/zip"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
)

var (
	tmpDir = "zip_tmp"
)

func init() {
	if dir := os.Getenv("ZIPSERVER_TMP_DIR"); dir != "" {
		tmpDir = dir
	} else if dir := os.Getenv("RUNTIME_DIRECTORY"); dir != "" {
		tmpDir = dir
	}

	mime.AddExtensionType(".unityweb", "application/octet-stream")
	mime.AddExtensionType(".wasm", "application/wasm")
	mime.AddExtensionType(".data", "application/octet-stream") // modern unity data file
	mime.AddExtensionType(".ico", "image/x-icon")              // prevent image/vnd.microsoft.icon
}

// isIndexHtml returns true if the filename is index.html (case-insensitive)
func isIndexHtml(filename string) bool {
	base := path.Base(filename)
	return strings.EqualFold(base, "index.html")
}

// ArchiveExtractor holds together the storage along with configuration values
// (credentials, limits etc.)
type ArchiveExtractor struct {
	Storage // Source storage (for reading zips)
	*Config
	TargetStorage Storage // Optional: target storage for uploads (if nil, uses source)
	TargetBucket  string  // Optional: target bucket (if empty, uses config.Bucket)
}

// getDestinationStorage returns the storage where extracted files are written
func (a *ArchiveExtractor) getDestinationStorage() Storage {
	if a.TargetStorage != nil {
		return a.TargetStorage
	}
	return a.Storage
}

// getDestinationBucket returns the bucket where extracted files are written
func (a *ArchiveExtractor) getDestinationBucket() string {
	if a.TargetBucket != "" {
		return a.TargetBucket
	}
	return a.Bucket
}

// ExtractedFile represents a file extracted from a .zip into a GCS bucket
type ExtractedFile struct {
	Key      string
	Size     uint64
	MD5      string
	Injected bool `json:",omitempty"` // true if HTML footer was injected
}

// NewArchiveExtractor creates a new archiver from the given config
func NewArchiveExtractor(config *Config) *ArchiveExtractor {
	storage, err := NewGcsStorage(config)

	if storage == nil {
		log.Fatal("Failed to create storage:", err)
	}

	return &ArchiveExtractor{Storage: storage, Config: config}
}

// NewArchiveExtractorWithTarget creates an archiver that reads from source storage
// but writes to a different target storage
func NewArchiveExtractorWithTarget(config *Config, targetStorage Storage, targetBucket string) *ArchiveExtractor {
	storage, err := NewGcsStorage(config)

	if storage == nil {
		log.Fatal("Failed to create storage:", err)
	}

	return &ArchiveExtractor{
		Storage:       storage,
		Config:        config,
		TargetStorage: targetStorage,
		TargetBucket:  targetBucket,
	}
}

func fetchZipFilename(bucket, key string) string {
	hasher := md5.New()
	hasher.Write([]byte(key))
	return bucket + "_" + hex.EncodeToString(hasher.Sum(nil)) + ".zip"
}

func (a *ArchiveExtractor) fetchZip(ctx context.Context, key string, maxZipSize uint64) (string, error) {
	os.MkdirAll(tmpDir, os.ModeDir|0777)

	fname := fetchZipFilename(a.Bucket, key)
	fname = path.Join(tmpDir, fname)

	src, headers, err := a.Storage.GetFile(ctx, a.Bucket, key)
	if err != nil {
		return "", err
	}

	defer src.Close()

	if headers != nil && maxZipSize > 0 {
		if contentLength := headers.Get("Content-Length"); contentLength != "" {
			size, err := strconv.ParseInt(contentLength, 10, 64)
			if err != nil {
				return "", fmt.Errorf("invalid Content-Length: %w", err)
			}
			if err := checkContentLength(maxZipSize, size); err != nil {
				return "", err
			}
		}
	}

	dest, err := os.Create(fname)
	if err != nil {
		return "", err
	}
	_, err = os.Stat(fname)

	defer func() {
		dest.Close()
		// Clean up if io.Copy below errs.
		if err != nil {
			os.Remove(fname)
		}
	}()

	if maxZipSize > 0 {
		var bytesRead uint64
		_, err = io.Copy(dest, limitedReader(src, maxZipSize, &bytesRead))
	} else {
		_, err = io.Copy(dest, src)
	}
	if err != nil {
		if errors.Is(err, ErrLimitExceeded) {
			return "", fmt.Errorf("zip too large (max %d bytes)", maxZipSize)
		}
		return "", err
	}

	return fname, nil
}

// delete all files that have been uploaded so far
func (a *ArchiveExtractor) abortUpload(files []ExtractedFile) error {
	for _, file := range files {
		// FIXME: code quality - what if we fail here? any retry strategies?
		ctx := context.Background()
		a.getDestinationStorage().DeleteFile(ctx, a.getDestinationBucket(), file.Key)
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

// shouldIncludeFile returns true if the file matches the include pattern.
// An empty pattern matches all files.
func shouldIncludeFile(fname string, pattern string) (bool, error) {
	if pattern == "" {
		return true, nil
	}
	return doublestar.Match(pattern, fname)
}

// shouldIncludeFileByList returns true if the file is in the only_files list.
// An empty list matches all files.
func shouldIncludeFileByList(fname string, onlyFiles []string) bool {
	if len(onlyFiles) == 0 {
		return true
	}
	for _, allowed := range onlyFiles {
		if fname == allowed {
			return true
		}
	}
	return false
}

// UploadFileTask contains the information needed to extract a single file from a .zip
type UploadFileTask struct {
	File       *zip.File
	Key        string
	HtmlFooter string // HTML to append if this is an index.html file
}

// UploadFileResult is successful is Error is nil - in that case, it contains the
// GCS key the file was uploaded under, and the number of bytes written for that file.
type UploadFileResult struct {
	Error    error
	Key      string
	Size     uint64
	MD5      string
	Injected bool // true if HTML footer was injected into this file
}

func uploadWorker(
	ctx context.Context,
	a *ArchiveExtractor,
	tasks <-chan UploadFileTask,
	results chan<- UploadFileResult,
	done chan struct{},
) {
	defer func() { done <- struct{}{} }()

	for task := range tasks {
		file := task.File
		key := task.Key

		startTime := time.Now()
		timeout := time.Duration(a.Config.FilePutTimeout)
		uploadCtx, cancel := context.WithTimeout(ctx, timeout)
		result := a.extractAndUploadOne(uploadCtx, key, file, task.HtmlFooter)
		cancel() // Free resources now instead of deferring till func returns

		if result.Error != nil {
			elapsed := time.Since(startTime)
			// Check if this was a per-file timeout
			if errors.Is(result.Error, context.DeadlineExceeded) {
				result.Error = fmt.Errorf("file upload timed out after %v (limit %v): %s", elapsed.Round(time.Millisecond), timeout, key)
			} else if errors.Is(result.Error, context.Canceled) {
				// Check if the parent context was canceled (another worker failed or job timeout)
				if ctx.Err() != nil {
					log.Printf("Upload canceled for %s after %v (another operation failed)", key, elapsed.Round(time.Millisecond))
					results <- result
					return
				}
			}
			log.Print("Failed sending " + key + ": " + result.Error.Error())
			// Return early to abort further work on the first error.
			results <- result
			return
		}

		results <- result
	}
}

// extracts and sends all files to prefix
func (a *ArchiveExtractor) sendZipExtracted(
	ctx context.Context,
	prefix, fname string,
	limits *ExtractLimits,
) ([]ExtractedFile, error) {
	startTime := time.Now()
	zipReader, err := zip.OpenReader(fname)
	if err != nil {
		return nil, err
	}

	defer zipReader.Close()

	if len(zipReader.File) > limits.MaxNumFiles {
		if limits.MaxNumFiles > 0 {
			return nil, fmt.Errorf("Too many files in zip (%v > %v)",
				len(zipReader.File), limits.MaxNumFiles)
		}
	}

	extractedFiles := []ExtractedFile{}

	fileCount := 0
	var byteCount uint64

	fileList := []*zip.File{}

	for _, file := range zipReader.File {
		if shouldIgnoreFile(file.Name) {
			log.Printf("Ignoring file %s", file.Name)
			continue
		}

		// Check only_files list first (takes precedence if specified)
		if len(limits.OnlyFiles) > 0 {
			if !shouldIncludeFileByList(file.Name, limits.OnlyFiles) {
				continue
			}
		} else if limits.IncludeGlob != "" {
			// Check include glob filter
			included, err := shouldIncludeFile(file.Name, limits.IncludeGlob)
			if err != nil {
				return nil, fmt.Errorf("invalid glob pattern %q: %w", limits.IncludeGlob, err)
			}
			if !included {
				continue
			}
		}

		if limits.MaxFileNameLength > 0 && len(file.Name) > limits.MaxFileNameLength {
			return nil, fmt.Errorf("Zip contains file paths that are too long")
		}

		// Calculate effective file size (including potential HTML footer)
		effectiveSize := file.UncompressedSize64
		if limits.HtmlFooter != "" && isIndexHtml(file.Name) {
			effectiveSize += uint64(len(limits.HtmlFooter))
		}

		if limits.MaxFileSize > 0 && effectiveSize > limits.MaxFileSize {
			return nil, fmt.Errorf("Zip contains file that is too large (%s)", file.Name)
		}

		byteCount += effectiveSize

		if limits.MaxTotalSize > 0 && byteCount > limits.MaxTotalSize {
			return nil, fmt.Errorf("Extracted zip too large (max %v bytes)", limits.MaxTotalSize)
		}

		fileList = append(fileList, file)
	}

	tasks := make(chan UploadFileTask)
	results := make(chan UploadFileResult)
	threads := limits.ExtractionThreads
	if threads < 1 {
		threads = runtime.GOMAXPROCS(0)
		if threads < 1 {
			threads = 1
		}
	}

	done := make(chan struct{}, threads)

	// Context can be canceled by caller or when an individual task fails.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < threads; i++ {
		go uploadWorker(ctx, a, tasks, results, done)
	}

	activeWorkers := threads

	go func() {
		defer func() { close(tasks) }()
		for _, file := range fileList {
			key := path.Join(prefix, file.Name)
			htmlFooter := ""
			if limits.HtmlFooter != "" && isIndexHtml(file.Name) {
				htmlFooter = limits.HtmlFooter
			}
			task := UploadFileTask{
				File:       file,
				Key:        key,
				HtmlFooter: htmlFooter,
			}
			select {
			case tasks <- task:
			case <-ctx.Done():
				// Something went wrong!
				log.Println("Remaining tasks were canceled")
				return
			}
		}
	}()

	var extractError error

	for activeWorkers > 0 {
		select {
		case result := <-results:
			if result.Error != nil {
				// Only capture the first non-context error, or the first error if none yet
				if extractError == nil || (!errors.Is(result.Error, context.Canceled) && !errors.Is(result.Error, context.DeadlineExceeded)) {
					extractError = result.Error
				}
				cancel()
			} else {
				extractedFiles = append(extractedFiles, ExtractedFile{
					Key:      result.Key,
					Size:     result.Size,
					MD5:      result.MD5,
					Injected: result.Injected,
				})
				fileCount++
			}
		case <-done:
			activeWorkers--
		}
	}

	close(results)

	elapsed := time.Since(startTime)
	if extractError != nil {
		log.Printf("Upload error after %v: %s", elapsed.Round(time.Millisecond), extractError.Error())
		a.abortUpload(extractedFiles)
		return nil, extractError
	}

	log.Printf("Sent %d files in %v", fileCount, elapsed.Round(time.Millisecond))
	return extractedFiles, nil
}

// sends an individual file from a zip
// Caller should set the job timeout in ctx.
// htmlFooter: if non-empty, append this HTML to the file content (for index.html files)
func (a *ArchiveExtractor) extractAndUploadOne(ctx context.Context, key string, file *zip.File, htmlFooter string) UploadFileResult {
	readerCloser, err := file.Open()
	if err != nil {
		return UploadFileResult{Error: err, Key: key}
	}
	defer readerCloser.Close()

	var reader io.Reader = readerCloser

	resource := &ResourceSpec{
		key: key,
	}

	// try determining MIME by extension
	mimeType := mime.TypeByExtension(path.Ext(key))

	var buffer bytes.Buffer
	_, err = io.Copy(&buffer, io.LimitReader(reader, 512))

	if err != nil {
		return UploadFileResult{Error: err, Key: key}
	}

	contentMimeType := http.DetectContentType(buffer.Bytes())
	// join the bytes read and the original reader
	reader = io.MultiReader(&buffer, reader)

	if contentMimeType == "application/x-gzip" || contentMimeType == "application/gzip" {
		resource.contentEncoding = "gzip"

		// try to see if there's a real extension hidden beneath
		if strings.HasSuffix(key, ".gz") {
			realMimeType := mime.TypeByExtension(path.Ext(strings.TrimSuffix(key, ".gz")))

			if realMimeType != "" {
				mimeType = realMimeType
			}
		}

	} else if strings.HasSuffix(key, ".br") {
		// there is no way to detect a brotli stream by content, so we assume if it ends if .br then it's brotli
		// this path is used for Unity 2020 webgl games built with brotli compression
		resource.contentEncoding = "br"
		realMimeType := mime.TypeByExtension(path.Ext(strings.TrimSuffix(key, ".br")))

		if realMimeType != "" {
			mimeType = realMimeType
		}
	} else if mimeType == "" {
		// fall back to the extension detected from content, eg. someone uploaded a .png with wrong extension
		mimeType = contentMimeType
	}

	if mimeType == "" {
		// default mime type
		mimeType = "application/octet-stream"
	}
	resource.contentType = mimeType

	resource.applyRewriteRules()

	// Calculate expected size, accounting for HTML footer if applicable
	// Skip injection for compressed files (gzip/brotli) as we can't append to compressed streams
	expectedSize := file.UncompressedSize64
	injected := false
	if htmlFooter != "" && resource.contentEncoding == "" {
		expectedSize += uint64(len(htmlFooter))
		reader = newAppendReader(reader, htmlFooter)
		injected = true
	}

	// Pre-compress if configured and applicable
	if resource.contentEncoding == "" && shouldPreCompress(resource.key, expectedSize, a.Config) {
		compressedFile, usedCompressed, err := preCompressStreamToTemp(ctx, reader, expectedSize, a.Config)
		if err != nil {
			if errors.Is(err, ErrLimitExceeded) {
				return UploadFileResult{Error: fmt.Errorf("zip entry exceeds declared size (max %d bytes)", expectedSize), Key: resource.key}
			}
			return UploadFileResult{Error: err, Key: resource.key}
		}

		if usedCompressed {
			defer compressedFile.Cleanup()
			reader = compressedFile.Reader
			resource.contentEncoding = "gzip"
			expectedSize = compressedFile.Size
		} else {
			// Compression attempt consumed the stream. Re-open the zip entry and re-apply
			// HTML footer injection so we can stream the original bytes to storage.
			retryReaderCloser, err := file.Open()
			if err != nil {
				return UploadFileResult{Error: err, Key: resource.key}
			}
			defer retryReaderCloser.Close()

			reader = retryReaderCloser
			if injected {
				reader = newAppendReader(reader, htmlFooter)
			}
		}
	}

	if injected {
		log.Printf("Sending: %s (injected)", resource)
	} else {
		log.Printf("Sending: %s", resource)
	}

	limited := limitedReader(reader, expectedSize, &resource.size)

	putResult, err := a.getDestinationStorage().PutFile(ctx, a.getDestinationBucket(), resource.key, limited, resource.ToPutOptions())
	if err != nil {
		if errors.Is(err, ErrLimitExceeded) {
			return UploadFileResult{Error: fmt.Errorf("zip entry exceeds declared size (max %d bytes)", expectedSize), Key: resource.key}
		}
		return UploadFileResult{Error: err, Key: resource.key}
	}

	globalMetrics.TotalExtractedFiles.Add(1)

	return UploadFileResult{
		Key:      resource.key,
		Size:     resource.size,
		MD5:      putResult.MD5,
		Injected: injected,
	}
}

// ExtractZip downloads the zip at `key` to a temporary file,
// then extracts its contents and uploads each item to `prefix`
// Caller should set the job timeout in ctx.
func (a *ArchiveExtractor) ExtractZip(
	ctx context.Context,
	key, prefix string,
	limits *ExtractLimits,
) ([]ExtractedFile, error) {
	fname, err := a.fetchZip(ctx, key, limits.MaxInputZipSize)
	if err != nil {
		return nil, err
	}

	defer os.Remove(fname)
	prefix = path.Join(a.ExtractPrefix, prefix)
	return a.sendZipExtracted(ctx, prefix, fname, limits)
}

// Caller should set the job timeout in ctx.
func (a *ArchiveExtractor) UploadZipFromFile(
	ctx context.Context,
	fname, prefix string,
	limits *ExtractLimits,
) ([]ExtractedFile, error) {
	if limits.MaxInputZipSize > 0 {
		info, err := os.Stat(fname)
		if err != nil {
			return nil, err
		}
		if info.Size() > int64(limits.MaxInputZipSize) {
			return nil, fmt.Errorf("zip too large (max %d bytes)", limits.MaxInputZipSize)
		}
	}
	prefix = path.Join("_zipserver", prefix)
	return a.sendZipExtracted(ctx, prefix, fname, limits)
}
