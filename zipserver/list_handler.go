package zipserver

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	return ListResult{Err: fmt.Errorf("either Key or URL must be specified")}
}

func (o *Operations) listFromBucket(ctx context.Context, key string) ListResult {
	storage, err := NewGcsStorage(o.config)
	if err != nil {
		return ListResult{Err: err}
	}

	reader, _, err := storage.GetFile(ctx, o.config.Bucket, key)
	if err != nil {
		return ListResult{Err: err}
	}
	defer reader.Close()

	body, err := io.ReadAll(reader)
	if err != nil {
		return ListResult{Err: err}
	}

	return o.listZipBytes(body)
}

func (o *Operations) listFromURL(ctx context.Context, url string) ListResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ListResult{Err: err}
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return ListResult{Err: err}
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return ListResult{Err: err}
	}

	return o.listZipBytes(body)
}

func (o *Operations) listZipBytes(body []byte) ListResult {
	zipFile, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return ListResult{Err: err}
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

func listHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(globalConfig.FileGetTimeout))
	defer cancel()

	params := r.URL.Query()

	ops := NewOperations(globalConfig)
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
