package zipserver

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	errors "github.com/go-errors/errors"
)

type memoryHttpHandler struct {
	storage *MemStorage
	bucket  string
	prefix  string
}

var _ http.Handler = (*memoryHttpHandler)(nil)

func printError(err error) {
	if se, ok := err.(*errors.Error); ok {
		log.Printf("error: %s", se.ErrorStack())
	} else {
		log.Printf("error: %s", se.Error())
	}
}

func dumpError(w http.ResponseWriter, err error) {
	printError(err)
	w.WriteHeader(500)
	w.Write([]byte("Internal error"))
}

func (mhh *memoryHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")

	objectPath := fmt.Sprintf("%s/%s", mhh.prefix, path)
	log.Printf("Requesting %s", objectPath)

	reader, err := mhh.storage.GetFile(mhh.bucket, objectPath)
	if err != nil {
		printError(err)
		w.WriteHeader(404)
		w.Write([]byte("Not found"))
		return
	}
	defer reader.Close()

	headers, err := mhh.storage.getHeaders(mhh.bucket, objectPath)
	if err != nil {
		dumpError(w, err)
		return
	}

	log.Printf("Headers: %v", headers)

	for k, vv := range headers {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	w.WriteHeader(200)

	_, err = io.Copy(w, reader)
	if err != nil {
		dumpError(w, err)
		return
	}
}

// ServeZip takes the path to zip file in the local fs and serves
// it as http
func ServeZip(serve string) error {
	config := &defaultConfig
	config.Bucket = "local"

	storage, err := NewMemStorage()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	reader, err := os.Open(serve)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	key := "serve.zip"
	err = storage.PutFile(config.Bucket, key, reader, "application/zip")
	if err != nil {
		return errors.Wrap(err, 0)
	}

	archiver := &Archiver{storage, config}

	prefix := "extracted"
	_, err = archiver.ExtractZip(key, prefix, DefaultExtractLimits(config))
	if err != nil {
		return errors.Wrap(err, 0)
	}

	handler := &memoryHttpHandler{storage, config.Bucket, prefix}

	s := &http.Server{
		Addr:    "localhost:8091",
		Handler: handler,
	}
	log.Printf("Listening on %s...", s.Addr)
	return s.ListenAndServe()
}
