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

// Copy copies a file from primary storage to a target storage
func (o *Operations) Copy(ctx context.Context, params CopyParams) CopyResult {
	storageTargetConfig := o.config.GetStorageTargetByName(params.TargetName)
	if storageTargetConfig == nil {
		return CopyResult{Err: fmt.Errorf("invalid target: %s", params.TargetName)}
	}

	targetBucket := storageTargetConfig.Bucket

	if params.ExpectedBucket != "" && params.ExpectedBucket != targetBucket {
		return CopyResult{Err: fmt.Errorf("expected bucket does not match target bucket: %s != %s", params.ExpectedBucket, targetBucket)}
	}

	storage, err := NewGcsStorage(o.config)
	if err != nil {
		return CopyResult{Err: fmt.Errorf("failed to create source storage: %v", err)}
	}

	targetStorage, err := storageTargetConfig.NewStorageClient()
	if err != nil {
		return CopyResult{Err: fmt.Errorf("failed to create target storage: %v", err)}
	}

	startTime := time.Now()

	reader, headers, err := storage.GetFile(ctx, o.config.Bucket, params.Key)
	if err != nil {
		return CopyResult{Err: fmt.Errorf("failed to get file: %v", err)}
	}
	defer reader.Close()

	mReader := newMeasuredReader(reader)

	contentType := headers.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	opts := PutOptions{
		ContentType:        contentType,
		ContentDisposition: headers.Get("Content-Disposition"),
		ContentEncoding:    headers.Get("Content-Encoding"),
		ACL:                ACLPublicRead,
	}

	log.Print("Starting transfer: [", params.TargetName, "] ", targetBucket, "/", params.Key, " ", opts)
	result, err := targetStorage.PutFile(ctx, targetBucket, params.Key, mReader, opts)
	if err != nil {
		return CopyResult{Err: fmt.Errorf("failed to copy file: %v", err)}
	}

	globalMetrics.TotalCopiedFiles.Add(1)
	log.Print("Transfer complete: [", params.TargetName, "] ", targetBucket, "/", params.Key,
		", bytes read: ", formatBytes(float64(mReader.BytesRead)),
		", duration: ", mReader.Duration.Seconds(),
		", speed: ", formatBytes(mReader.TransferSpeed()), "/s")

	return CopyResult{
		Key:      params.Key,
		Duration: fmt.Sprintf("%.4fs", time.Since(startTime).Seconds()),
		Size:     mReader.BytesRead,
		Md5:      result.MD5,
	}
}

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
	notifyCtx, notifyCancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.AsyncNotificationTimeout))
	defer notifyCancel()

	outBody := bytes.NewBufferString(resValues.Encode())
	req, err := http.NewRequestWithContext(notifyCtx, http.MethodPost, callbackURL, outBody)
	if err != nil {
		globalMetrics.TotalCallbackFailures.Add(1)
		log.Print("Failed to create callback request: ", err)
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		globalMetrics.TotalCallbackFailures.Add(1)
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
	globalMetrics.TotalErrors.Add(1)

	message := url.Values{}
	message.Add("Success", "false")
	message.Add("Error", err.Error())
	return notifyCallback(callbackURL, message)
}

// The copy handler will asynchronously copy a file from primary storage to the
// storage specified by target
func copyHandler(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return fmt.Errorf("failed to parse form: %w", err)
	}
	params := r.Form
	key, err := getParam(params, "key")
	if err != nil {
		return err
	}

	callbackURL := params.Get("callback")

	targetName, err := getParam(params, "target")
	if err != nil {
		return err
	}

	storageTargetConfig := globalConfig.GetStorageTargetByName(targetName)
	if storageTargetConfig == nil {
		return fmt.Errorf("invalid target: %s", targetName)
	}

	expectedBucket, _ := getParam(params, "bucket")

	lockKey := fmt.Sprintf("%s:%s", targetName, key)

	hasLock := copyLockTable.tryLockKey(lockKey)

	if !hasLock {
		// already being copied in another handler, ask consumer to wait
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	ops := NewOperations(globalConfig)
	copyParams := CopyParams{
		Key:            key,
		TargetName:     targetName,
		ExpectedBucket: expectedBucket,
	}

	// sync codepath
	if callbackURL == "" {
		defer copyLockTable.releaseKey(lockKey)

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

		result := ops.Copy(ctx, copyParams)
		if result.Err != nil {
			globalMetrics.TotalErrors.Add(1)
			return writeJSONError(w, "CopyError", result.Err)
		}

		return writeJSONMessage(w, struct {
			Success  bool
			Key      string
			Duration string
			Size     int64
			Md5      string
		}{true, result.Key, result.Duration, result.Size, result.Md5})
	}

	// async codepath
	go (func() {
		defer copyLockTable.releaseKey(lockKey)

		jobCtx, cancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

		result := ops.Copy(jobCtx, copyParams)

		if result.Err != nil {
			if callbackURL == "-" {
				globalMetrics.TotalErrors.Add(1)
				log.Printf("CopyError async (callback=-): %v", result.Err)
				return
			}
			notifyError(callbackURL, result.Err)
			return
		}

		if callbackURL != "-" {
			resValues := url.Values{}
			resValues.Add("Success", "true")
			resValues.Add("Key", result.Key)
			resValues.Add("Duration", result.Duration)
			resValues.Add("Size", fmt.Sprintf("%d", result.Size))
			resValues.Add("Md5", result.Md5)

			notifyCallback(callbackURL, resValues)
		}
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
