package zipserver

import (
	"github.com/klauspost/compress/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type fileTuple struct {
	Filename string
	Size     uint64
}

// List lists files in a zip archive
func (o *Operations) List(ctx context.Context, params ListParams) ListResult {
	if params.Key != "" {
		return o.listFromBucket(ctx, params.Key)
	}
	if params.URL != "" {
		return o.listFromURL(ctx, params.URL)
	}
	if params.File != "" {
		return o.listFromFile(params.File)
	}
	return ListResult{Err: fmt.Errorf("either Key, URL, or File must be specified")}
}

func (o *Operations) listFromBucket(ctx context.Context, key string) ListResult {
	storage, err := NewGcsStorage(o.config)
	if err != nil {
		return ListResult{Err: err}
	}

	readerAt, size, err := storage.GetReaderAt(ctx, o.config.Bucket, key, o.config.MaxInputZipSize)
	if err != nil {
		return ListResult{Err: err}
	}
	defer readerAt.Close()

	if o.config.MaxInputZipSize > 0 {
		if err := checkContentLength(o.config.MaxInputZipSize, size); err != nil {
			return ListResult{Err: err}
		}
	}

	zipFile, err := zip.NewReader(readerAt, size)
	if err != nil {
		return ListResult{Err: err}
	}

	if o.config.MaxListFiles > 0 && len(zipFile.File) > o.config.MaxListFiles {
		return ListResult{Err: fmt.Errorf("zip too many files (max %d)", o.config.MaxListFiles)}
	}

	var files []fileTuple
	for _, file := range zipFile.File {
		files = append(files, fileTuple{
			Filename: file.Name,
			Size:     file.UncompressedSize64,
		})
	}

	return ListResult{Files: files}
}

func (o *Operations) listFromURL(ctx context.Context, url string) ListResult {
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return ListResult{Err: err}
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return ListResult{Err: err}
	}
	defer response.Body.Close()

	if err := checkContentLength(o.config.MaxInputZipSize, response.ContentLength); err != nil {
		return ListResult{Err: err}
	}

	var body []byte
	if o.config.MaxInputZipSize > 0 {
		var bytesRead uint64
		body, err = io.ReadAll(limitedReaderWithCancel(response.Body, o.config.MaxInputZipSize, &bytesRead, func() {
			cancel()
			_ = response.Body.Close()
		}))
	} else {
		body, err = io.ReadAll(response.Body)
	}
	if err != nil {
		return ListResult{Err: err}
	}

	return o.listZipBytes(body)
}

func (o *Operations) listFromFile(path string) ListResult {
	zipFile, err := zip.OpenReader(path)
	if err != nil {
		return ListResult{Err: err}
	}
	defer zipFile.Close()

	if o.config.MaxListFiles > 0 && len(zipFile.File) > o.config.MaxListFiles {
		return ListResult{Err: fmt.Errorf("zip too many files (max %d)", o.config.MaxListFiles)}
	}

	var files []fileTuple
	for _, file := range zipFile.File {
		files = append(files, fileTuple{
			Filename: file.Name,
			Size:     file.UncompressedSize64,
		})
	}

	return ListResult{Files: files}
}

func (o *Operations) listZipBytes(body []byte) ListResult {
	zipFile, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return ListResult{Err: err}
	}

	if o.config.MaxListFiles > 0 && len(zipFile.File) > o.config.MaxListFiles {
		return ListResult{Err: fmt.Errorf("zip too many files (max %d)", o.config.MaxListFiles)}
	}

	var files []fileTuple
	for _, file := range zipFile.File {
		files = append(files, fileTuple{
			Filename: file.Name,
			Size:     file.UncompressedSize64,
		})
	}

	return ListResult{Files: files}
}

func checkContentLength(maxBytes uint64, contentLength int64) error {
	if maxBytes == 0 || contentLength <= 0 {
		return nil
	}
	if uint64(contentLength) > maxBytes {
		return fmt.Errorf("zip too large (max %d bytes)", maxBytes)
	}
	return nil
}

func listHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(globalConfig.FileGetTimeout))
	defer cancel()

	if err := r.ParseForm(); err != nil {
		return fmt.Errorf("failed to parse form: %w", err)
	}
	params := r.Form

	localConfig := *globalConfig
	if maxZipSizeStr := params.Get("maxInputZipSize"); maxZipSizeStr != "" {
		maxZipSize, err := strconv.ParseUint(maxZipSizeStr, 10, 64)
		if err != nil {
			return err
		}
		localConfig.MaxInputZipSize = maxZipSize
	}

	ops := NewOperations(&localConfig)
	listParams := ListParams{}

	key, err := getParam(params, "key")
	if err == nil {
		listParams.Key = key
	} else {
		url, err := getParam(params, "url")
		if err == nil {
			listParams.URL = url
		} else {
			return errors.New("missing key or url")
		}
	}

	result := ops.List(ctx, listParams)
	if result.Err != nil {
		return result.Err
	}

	return writeJSONMessage(w, result.Files)
}
