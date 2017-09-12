package zipserver

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

func slurpHandler(w http.ResponseWriter, r *http.Request) error {
	params := r.URL.Query()

	key, err := getParam(params, "key")
	if err != nil {
		return err
	}

	slurpURL, err := getParam(params, "url")
	if err != nil {
		return err
	}

	contentType := params.Get("content_type")
	maxBytesStr := params.Get("max_bytes")
	acl := params.Get("acl")
	contentDisposition := params.Get("content_disposition")

	var maxBytes uint64
	if maxBytesStr != "" {
		maxBytes, err = strconv.ParseUint(maxBytesStr, 10, 64)
		if err != nil {
			return err
		}
	}

	process := func() error {
		log.Print("Fetching URL: ", slurpURL)
		client := http.Client{}
		res, err := client.Get(slurpURL)

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

		if maxBytes > 0 {
			if uint64(res.ContentLength) > maxBytes {
				return fmt.Errorf("Content-Length is greater than max bytes (%d > %d)",
					res.ContentLength, maxBytes)
			}

			var bytesRead uint64
			body = limitedReader(body, maxBytes, &bytesRead)
		}

		log.Print("Uploading ", contentType, " (size: ", res.ContentLength, ") to ", key)
		log.Print("ACL: ", acl)
		log.Print("Content-Disposition: ", contentDisposition)

		storage, err := NewStorageClient(config)

		if storage == nil {
			log.Fatal("Failed to create storage:", err)
		}

		return storage.PutFileWithSetup(config.Bucket, key, body, func(req *http.Request) error {
			req.Header.Add("Content-Type", contentType)

			if contentDisposition != "" {
				req.Header.Add("Content-Disposition", contentDisposition)
			}

			req.Header.Add("x-goog-acl", acl)
			return nil
		})
	}

	asyncURL := params.Get("async")
	if asyncURL == "" {
		err = process()
		if err != nil {
			return writeJSONError(w, "SlurpError", err)
		}

		return writeJSONMessage(w, struct {
			Success bool
		}{true})
	}

	go (func() {
		err = process()
		log.Print("Notifying " + asyncURL)

		resValues := url.Values{}
		if err != nil {
			resValues.Add("Type", "SlurpError")
			resValues.Add("Error", err.Error())
		} else {
			resValues.Add("Success", "true")
		}

		_, err = http.PostForm(asyncURL, resValues)
		if err != nil {
			log.Print("Failed to deliver callback: " + err.Error())
		}
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
