package zipserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func slurpHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(config.JobTimeout))
	defer cancel()

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

	process := func(ctx context.Context) error {
		getCtx, cancel := context.WithTimeout(ctx, time.Duration(config.FileGetTimeout))
		defer cancel()

		log.Print("Fetching URL: ", slurpURL)

		req, err := http.NewRequestWithContext(getCtx, http.MethodGet, slurpURL, nil)
		if err != nil {
			return err
		}

		res, err := http.DefaultClient.Do(req)
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

		storage, err := NewGcsStorage(config)

		if storage == nil {
			log.Fatal("Failed to create storage:", err)
		}

		putCtx, cancel := context.WithTimeout(ctx, time.Duration(config.FilePutTimeout))
		defer cancel()

		return storage.PutFileWithSetup(putCtx, config.Bucket, key, body, func(req *http.Request) error {
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
		err = process(ctx)
		if err != nil {
			return writeJSONError(w, "SlurpError", err)
		}

		return writeJSONMessage(w, struct {
			Success bool
		}{true})
	}

	go (func() {
		// This job is expected to outlive the incoming request, so create a detached context.
		ctx := context.Background()

		err = process(ctx)
		log.Print("Notifying " + asyncURL)

		resValues := url.Values{}
		if err != nil {
			resValues.Add("Type", "SlurpError")
			resValues.Add("Error", err.Error())
		} else {
			resValues.Add("Success", "true")
		}

		ctx, cancel := context.WithTimeout(ctx, time.Duration(config.AsyncNotificationTimeout))
		defer cancel()

		outBody := bytes.NewBufferString(resValues.Encode())
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, asyncURL, outBody)
		if err != nil {
			log.Printf("Failed to create callback request: %v", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Failed to deliver callback: %v", err)
			return
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent {
			body, err := io.ReadAll(res.Body)
			if err != nil {
				log.Printf("Read notification response: %v", err)
				return
			}
			log.Printf("Notification response: %s %s", res.Status, string(body))
		}
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
