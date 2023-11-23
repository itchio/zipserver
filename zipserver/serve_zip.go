package zipserver

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	errors "github.com/go-errors/errors"
)

type memoryHttpHandler struct {
	storage        *MemStorage
	bucket         string
	prefix         string
	fileGetTimeout time.Duration
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

	ctx, cancel := context.WithTimeout(r.Context(), mhh.fileGetTimeout)
	defer cancel()

	reader, headers, err := mhh.storage.GetFile(ctx, mhh.bucket, objectPath)
	if err != nil {
		printError(err)
		w.WriteHeader(404)
		w.Write([]byte("Not found"))
		return
	}
	defer reader.Close()

	if headers != nil {
		log.Printf("Headers: %v", headers)

		for k, vv := range headers {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
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
func ServeZip(config *Config, serve string) error {
	config.Bucket = "local"

	storage, err := NewMemStorage()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	reader, err := os.Open(serve)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
	defer cancel()

	putCtx, putCtxCancel := context.WithTimeout(ctx, time.Duration(config.FilePutTimeout))
	defer putCtxCancel()

	key := "serve.zip"
	err = storage.PutFile(putCtx, config.Bucket, key, reader, "application/zip")
	if err != nil {
		return errors.Wrap(err, 0)
	}

	archiver := &Archiver{storage, config}

	prefix := "extracted"
	_, err = archiver.ExtractZip(ctx, key, prefix, DefaultExtractLimits(config))
	if err != nil {
		return errors.Wrap(err, 0)
	}

	handler := &memoryHttpHandler{
		storage:        storage,
		bucket:         config.Bucket,
		prefix:         prefix,
		fileGetTimeout: time.Duration(config.FileGetTimeout),
	}

	s := &http.Server{
		Addr:    "localhost:8091",
		Handler: handler,
	}
	log.Printf("Listening on %s...", s.Addr)
	return s.ListenAndServe()
}
