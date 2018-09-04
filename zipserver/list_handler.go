package zipserver

import (
	"archive/zip"
	"bytes"
	"io/ioutil"
	"net/http"
)

type fileTuple struct {
	Filename string
	Size     uint64
}

func listHandler(w http.ResponseWriter, r *http.Request) error {
	params := r.URL.Query()

	key, err := getParam(params, "key")

	if err != nil {
		return err
	}

	storage, err := NewGcsStorage(config)

	if storage == nil {
		return err
	}

	reader, err := storage.GetFile(config.Bucket, key)

	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(reader)

	if err != nil {
		return err
	}

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
