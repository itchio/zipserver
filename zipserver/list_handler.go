package zipserver

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

type fileTuple struct {
	Filename string
	Size     uint64
}

func listZip(body []byte, w http.ResponseWriter, r *http.Request) error {
	zipFile, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))

	if err != nil {
		return err
	}

	var filesOut []fileTuple

	for _, file := range zipFile.File {
		filesOut = append(filesOut, fileTuple{
			file.Name, file.UncompressedSize64,
		})
	}

	return writeJSONMessage(w, filesOut)
}

func listFromBucket(ctx context.Context, key string, w http.ResponseWriter, r *http.Request) error {
	storage, err := NewGcsStorage(globalConfig)
	if storage == nil {
		return err
	}

	reader, _, err := storage.GetFile(ctx, globalConfig.Bucket, key)
	if err != nil {
		return err
	}

	defer reader.Close()

	body, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	return listZip(body, w, r)
}

func listFromUrl(ctx context.Context, url string, w http.ResponseWriter, r *http.Request) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	return listZip(body, w, r)
}

func listHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(globalConfig.FileGetTimeout))
	defer cancel()

	params := r.URL.Query()

	key, err := getParam(params, "key")
	if err == nil {
		return listFromBucket(ctx, key, w, r)
	}

	url, err := getParam(params, "url")
	if err == nil {
		return listFromUrl(ctx, url, w, r)
	}

	return errors.New("missing key or url")
}
