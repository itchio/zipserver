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

// Copy copies a file from primary storage to a target storage, or within primary
// storage if no target is specified. If DestKey is set, the file is written to
// that key; otherwise it uses the source Key.
func (o *Operations) Copy(ctx context.Context, params CopyParams) CopyResult {
	if err := params.Validate(o.config); err != nil {
		return CopyResult{Err: err}
	}

	destKey := params.DestKeyOrKey()

	// Create source storage (always primary)
	storage, err := NewGcsStorage(o.config)
	if err != nil {
		return CopyResult{Err: fmt.Errorf("failed to create source storage: %v", err)}
	}

	var targetStorage Storage
	var targetBucket string
	var targetLabel string

	if params.TargetName == "" {
		// Same-storage copy: reuse source storage for both read and write
		targetStorage = storage
		targetBucket = o.config.Bucket
		targetLabel = "primary"
	} else {
		// Cross-storage copy: read from primary, write to target
		storageTargetConfig := o.config.GetStorageTargetByName(params.TargetName)
		targetBucket = storageTargetConfig.Bucket
		targetStorage, err = storageTargetConfig.NewStorageClient()
		if err != nil {
			return CopyResult{Err: fmt.Errorf("failed to create target storage: %v", err)}
		}
		targetLabel = params.TargetName
	}

	startTime := time.Now()

	reader, headers, err := storage.GetFile(ctx, o.config.Bucket, params.Key)
	if err != nil {
		return CopyResult{Err: fmt.Errorf("failed to get file: %v", err)}
	}
	defer reader.Close()

	contentType := headers.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	contentEncoding := headers.Get("Content-Encoding")

	// Check if we should inject HTML footer
	injected := false
	var finalReader io.Reader = reader
	if params.HtmlFooter != "" && contentEncoding == "" {
		finalReader = newAppendReader(reader, params.HtmlFooter)
		injected = true
	}

	mReader := newMeasuredReader(finalReader)

	contentDisposition := ""
	if !params.StripContentDisposition {
		contentDisposition = headers.Get("Content-Disposition")
	}

	opts := PutOptions{
		ContentType:        contentType,
		ContentDisposition: contentDisposition,
		ContentEncoding:    contentEncoding,
		ACL:                ACLPublicRead,
	}

	if injected {
		log.Print("Starting transfer (injected): [", targetLabel, "] ", targetBucket, "/", destKey, " ", opts)
	} else {
		log.Print("Starting transfer: [", targetLabel, "] ", targetBucket, "/", destKey, " ", opts)
	}
	result, err := targetStorage.PutFile(ctx, targetBucket, destKey, mReader, opts)
	if err != nil {
		return CopyResult{Err: fmt.Errorf("failed to copy file: %v", err)}
	}

	globalMetrics.TotalCopiedFiles.Add(1)
	log.Print("Transfer complete: [", targetLabel, "] ", targetBucket, "/", destKey,
		", bytes read: ", formatBytes(float64(mReader.BytesRead)),
		", duration: ", mReader.Duration.Seconds(),
		", speed: ", formatBytes(mReader.TransferSpeed()), "/s")

	return CopyResult{
		Key:      destKey,
		Duration: fmt.Sprintf("%.4fs", time.Since(startTime).Seconds()),
		Size:     mReader.BytesRead,
		Md5:      result.MD5,
		Injected: injected,
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

// The copy handler asynchronously copies a file from primary storage to either:
// - A target storage (when target is specified)
// - A different key within primary storage (when only dest_key is specified)
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
	targetName := params.Get("target")
	destKey := params.Get("dest_key")

	expectedBucket, _ := getParam(params, "bucket")
	htmlFooter := params.Get("html_footer")
	stripContentDisposition := params.Get("strip_content_disposition") == "true"

	copyParams := CopyParams{
		Key:                     key,
		DestKey:                 destKey,
		TargetName:              targetName,
		ExpectedBucket:          expectedBucket,
		HtmlFooter:              htmlFooter,
		StripContentDisposition: stripContentDisposition,
	}
	if err := copyParams.Validate(globalConfig); err != nil {
		return err
	}

	// Use dest_key for lock if provided, otherwise use key
	lockDestKey := copyParams.DestKeyOrKey()
	lockTargetName := targetName
	if lockTargetName == "" {
		lockTargetName = "primary"
	}
	lockKey := fmt.Sprintf("%s:%s", lockTargetName, lockDestKey)

	hasLock := copyLockTable.tryLockKey(lockKey)

	if !hasLock {
		// already being copied in another handler, ask consumer to wait
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	ops := NewOperations(globalConfig)
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
			Injected bool `json:",omitempty"`
		}{true, result.Key, result.Duration, result.Size, result.Md5, result.Injected})
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
			if result.Injected {
				resValues.Add("Injected", "true")
			}

			notifyCallback(callbackURL, resValues)
		}
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
