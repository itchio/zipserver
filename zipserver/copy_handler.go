package zipserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

var copyLockTable = NewLockTable()

func formatBytes(b float64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%.2f B", b)
	}
	div, exp := float64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", b/div, "kMGTPE"[exp])
}

// notify the callback URL of task completion
func notifyCallback(callbackURL string, resValues url.Values) error {
	notifyCtx, notifyCancel := context.WithTimeout(context.Background(), time.Duration(config.AsyncNotificationTimeout))
	defer notifyCancel()

	outBody := bytes.NewBufferString(resValues.Encode())
	req, err := http.NewRequestWithContext(notifyCtx, http.MethodPost, callbackURL, outBody)
	if err != nil {
		log.Print("Failed to create callback request: ", err)
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Print("Failed to deliver callback: ", err)
		return err
	}

	if response.StatusCode != http.StatusOK {
		log.Printf("Callback returned unexpected code: %d %s", response.StatusCode, callbackURL)
		bodyBytes, _ := io.ReadAll(response.Body)
		bodyString := string(bodyBytes)
		log.Print(bodyString)
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

	targetName, err := getParam(params, "target")
	if err != nil {
		return err
	}

	expectedBucket, _ := getParam(params, "bucket")

	hasLock := copyLockTable.tryLockKey(key)

	if !hasLock {
		// already being extracted in another handler, ask consumer to wait
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	go (func() {
		defer copyLockTable.releaseKey(key)

		jobCtx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
		defer cancel()

		storage, err := NewGcsStorage(config)

		if storage == nil {
			notifyError(callbackURL, fmt.Errorf("Failed to create source storage: %v", err))
			return
		}

		targetStorage, err := NewStorageByName(config, targetName)

		if err != nil {
			notifyError(callbackURL, fmt.Errorf("Failed to create target storage: %v", err))
			return
		}

		targetBucket := targetStorage.config.Bucket

		if expectedBucket != "" && expectedBucket != targetBucket {
			notifyError(callbackURL, fmt.Errorf("Expected bucket does not match target bucket: %s != %s", expectedBucket, targetBucket))
			return
		}

		startTime := time.Now()

		reader, headers, err := storage.GetFile(jobCtx, config.Bucket, key)

		if err != nil {
			log.Print("Failed to get file: ", err)
			notifyError(callbackURL, err)
			return
		}

		defer reader.Close()

		mReader := newMeasuredReader(reader)

		uploadHeaders := http.Header{}

		contentType := headers.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		uploadHeaders.Set("Content-Type", contentType)

		contentDisposition := headers.Get("Content-Disposition")
		if contentDisposition != "" {
			uploadHeaders.Set("Content-Disposition", contentDisposition)
		}

		log.Print("Starting transfer: ", key, " ", uploadHeaders)
		checksumMd5, err := targetStorage.PutFile(jobCtx, targetBucket, key, mReader, uploadHeaders)

		if err != nil {
			log.Print("Failed to copy file: ", err)
			notifyError(callbackURL, err)
			return
		}

		globalMetrics.TotalCopiedFiles.Add(1)
		log.Print("Transfer complete: ", key,
			", bytes read: ", formatBytes(float64(mReader.BytesRead)),
			", duration: ", mReader.Duration.Seconds(),
			", speed: ", formatBytes(mReader.TransferSpeed()), "/s")

		resValues := url.Values{}
		resValues.Add("Success", "true")
		resValues.Add("Key", key)
		resValues.Add("Duration", fmt.Sprintf("%.4fs", time.Since(startTime).Seconds()))
		resValues.Add("Size", fmt.Sprintf("%d", mReader.BytesRead))
		resValues.Add("Md5", checksumMd5)

		notifyCallback(callbackURL, resValues)
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
