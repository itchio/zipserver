package zipserver

import (
	"archive/zip"
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
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

func listFromBucket(key string, w http.ResponseWriter, r *http.Request) error {
	storage, err := NewGcsStorage(config)

	if storage == nil {
		return err
	}

	reader, err := storage.GetFile(config.Bucket, key)
	defer reader.Close()

	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(reader)

	if err != nil {
		return err
	}

	return listZip(body, w, r)
}

func listFromUrl(url string, w http.ResponseWriter, r *http.Request) error {
	response, err := http.Get(url)
	if err != nil {
		return err
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)

	return listZip(body, w, r)
}

func listHandler(w http.ResponseWriter, r *http.Request) error {
	params := r.URL.Query()

	key, err := getParam(params, "key")

	if err == nil {
		return listFromBucket(key, w, r)
	}

	url, err := getParam(params, "url")

	if err == nil {
		return listFromUrl(url, w, r)
	}

	return errors.New("missing key or url")
}
