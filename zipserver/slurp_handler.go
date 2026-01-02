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

var slurpLockTable = NewLockTable()

// Slurp downloads a file from a URL and stores it in storage
func (o *Operations) Slurp(ctx context.Context, params SlurpParams) SlurpResult {
	getCtx, cancel := context.WithTimeout(ctx, time.Duration(o.config.FileGetTimeout))
	defer cancel()

	log.Print("Fetching URL: ", params.URL)

	req, err := http.NewRequestWithContext(getCtx, http.MethodGet, params.URL, nil)
	if err != nil {
		return SlurpResult{Err: err}
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return SlurpResult{Err: err}
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return SlurpResult{Err: fmt.Errorf("failed to fetch file: %d", res.StatusCode)}
	}

	contentType := params.ContentType
	if contentType == "" {
		contentType = res.Header.Get("Content-Type")
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	body := io.Reader(res.Body)

	if params.MaxBytes > 0 {
		if uint64(res.ContentLength) > params.MaxBytes {
			return SlurpResult{Err: fmt.Errorf("Content-Length is greater than max bytes (%d > %d)",
				res.ContentLength, params.MaxBytes)}
		}

		var bytesRead uint64
		body = limitedReader(body, params.MaxBytes, &bytesRead)
	}

	log.Print("Uploading ", contentType, " (size: ", res.ContentLength, ") to ", params.Key)
	log.Print("ACL: ", params.ACL)
	log.Print("Content-Disposition: ", params.ContentDisposition)

	storage, err := NewGcsStorage(o.config)
	if err != nil {
		return SlurpResult{Err: fmt.Errorf("failed to create storage: %v", err)}
	}

	putCtx, putCancel := context.WithTimeout(ctx, time.Duration(o.config.FilePutTimeout))
	defer putCancel()

	err = storage.PutFileWithSetup(putCtx, o.config.Bucket, params.Key, body, func(req *http.Request) error {
		req.Header.Add("Content-Type", contentType)

		if params.ContentDisposition != "" {
			req.Header.Add("Content-Disposition", params.ContentDisposition)
		}

		req.Header.Add("x-goog-acl", params.ACL)
		return nil
	})

	if err != nil {
		return SlurpResult{Err: err}
	}

	return SlurpResult{}
}

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

	ops := NewOperations(globalConfig)
	slurpParams := SlurpParams{
		Key:                key,
		URL:                slurpURL,
		ContentType:        contentType,
		MaxBytes:           maxBytes,
		ACL:                acl,
		ContentDisposition: contentDisposition,
	}

	process := func(ctx context.Context) error {
		if !slurpLockTable.tryLockKey(key) {
			return fmt.Errorf("key is currently being processed: %s", key)
		}
		defer slurpLockTable.releaseKey(key)

		result := ops.Slurp(ctx, slurpParams)
		return result.Err
	}

	asyncURL := params.Get("async")
	if asyncURL == "" {
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

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
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

		err = process(ctx)
		log.Print("Notifying " + asyncURL)

		resValues := url.Values{}
		if err != nil {
			resValues.Add("Type", "SlurpError")
			resValues.Add("Error", err.Error())
		} else {
			resValues.Add("Success", "true")
		}

		notifyCtx, notifyCancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.AsyncNotificationTimeout))
		defer notifyCancel()

		outBody := bytes.NewBufferString(resValues.Encode())
		req, err := http.NewRequestWithContext(notifyCtx, http.MethodPost, asyncURL, outBody)
		if err != nil {
			log.Printf("Failed to create callback request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		_, err = http.DefaultClient.Do(req)
		if err != nil {
			log.Print("Failed to deliver callback: " + err.Error())
		}
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
