package zip_server

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
)

func slurpHandler(w http.ResponseWriter, r *http.Request) error {
	params := r.URL.Query()

	key, err := getParam(params, "key")
	if err != nil {
		return err
	}

	url, err := getParam(params, "url")
	if err != nil {
		return err
	}

	contentType := params.Get("content_type")
	maxBytesStr := params.Get("max_bytes")
	acl := params.Get("acl")
	contentDisposition := params.Get("content_disposition")

	maxBytes := 0
	if maxBytesStr != "" {
		maxBytes, err = strconv.Atoi(maxBytesStr)
		if err != nil {
			return err
		}
	}

	process := func() error {
		client := http.Client{}
		res, err := client.Get(url)

		if err != nil {
			return err
		}

		defer res.Body.Close()

		if res.StatusCode != 200 {
			return fmt.Errorf("Failed to fetch file: %d", res.StatusCode)
		}

		if contentType == "" {
			contentType = res.Header.Get("Content-Type")
		}

		if contentType == "" {
			contentType = "application/octet-stream"
		}

		body := io.Reader(res.Body)

		contentLengthStr := res.Header.Get("Content-Length")
		if maxBytes > 0 {
			if contentLengthStr != "" {
				contentLength, err := strconv.Atoi(contentLengthStr)
				if err != nil && contentLength > maxBytes {
					return fmt.Errorf("Content-Length is greater than max bytes (%d > %d)",
						contentLength, maxBytes)
				}
			}

			bytesRead := 0
			body = limitedReader(body, maxBytes, &bytesRead)
		}

		log.Print("Uploading ", contentType, " (size: ", contentLengthStr, ") to ", key)
		log.Print("ACL: ", acl)
		log.Print("Content-Disposition: ", contentDisposition)

		storage := NewStorageClient(config)

		return storage.PutFileWithSetup(config.Bucket, key, body, func(req *http.Request) error {
			req.Header.Add("Content-Type", contentType)

			if contentDisposition != "" {
				req.Header.Add("Content-Disposition", contentDisposition)
			}

			req.Header.Add("x-goog-acl", acl)
			return nil
		})
	}

	return process()
}
