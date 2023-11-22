package zipserver

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
)

var copyLockTable = NewLockTable()

// notify the callback URL of task completion
func notifyCallback(callbackURL string, resValues url.Values) error {
	notifyCtx, notifyCancel := context.WithTimeout(context.Background(), time.Duration(config.AsyncNotificationTimeout))
	defer notifyCancel()

	outBody := bytes.NewBufferString(resValues.Encode())
	req, err := http.NewRequestWithContext(notifyCtx, http.MethodPost, callbackURL, outBody)
	if err != nil {
		log.Printf("Failed to create callback request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Print("Failed to deliver callback: " + err.Error())
		return err
	}

	response.Body.Close()

	return nil
}

// notify the callback URL that an error happened
func notifyError(callbackURL string, err error) error {
	message := url.Values{}
	message.Add("Success", "false")
	message.Add("Error", err.Error())
	return notifyCallback(callbackURL, message)
}

// The copy handler will asynchronously copy a file from google cloud storage
// to the target S3 compatible storage provider
func copyHandler(w http.ResponseWriter, r *http.Request) error {
	params := r.URL.Query()
	key, err := getParam(params, "key")
	if err != nil {
		return err
	}

	callbackURL, err := getParam(params, "callback")
	if err != nil {
		return err
	}

	hasLock := copyLockTable.tryLockKey(key)

	if !hasLock {
		// already being extracted in another handler, ask consumer to wait
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	go (func() {
		defer extractLockTable.releaseKey(key)

		jobCtx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
		defer cancel()

		storage, err := NewGcsStorage(config)

		if storage == nil {
			log.Fatal("Failed to create storage: ", err)
		}

		targetStorage, err := NewS3Storage(config)

		if storage == nil {
			log.Fatal("Failed to create storage: ", err)
		}

		startTime := time.Now()

		reader, err := storage.GetFile(jobCtx, config.Bucket, key)
		defer reader.Close()

		if err != nil {
			log.Print("Failed to get file", err)
			notifyError(callbackURL, err)
			return
		}

		// transfer the reader to s3
		// TODO: get the actual mime type from the GetFile request
		log.Print("Starting transfer: ", key)
		err = targetStorage.PutFile(jobCtx, config.S3Bucket, key, reader, "application/octet-stream")

		if err != nil {
			log.Print("Failed to copy file: ", err)
			notifyError(callbackURL, err)
			return
		}

		log.Print("Transfer complete " + callbackURL)
		resValues := url.Values{}
		resValues.Add("Success", "true")
		resValues.Add("Key", key)
		resValues.Add("Duration", fmt.Sprintf("%f", time.Since(startTime).Seconds()))

		notifyCallback(callbackURL, resValues)
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}