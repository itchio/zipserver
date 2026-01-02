package zipserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
)

// mutex for keys currently being extracted
var extractLockTable = NewLockTable()

// Extract performs a zip extraction operation
func (o *Operations) Extract(ctx context.Context, params ExtractParams) ExtractResult {
	limits := params.Limits
	if limits == nil {
		limits = DefaultExtractLimits(o.config)
	}

	archiver := NewArchiver(o.config)

	var files []ExtractedFile
	var err error

	if params.File != "" {
		// Extract from local file
		files, err = archiver.UploadZipFromFile(ctx, params.File, params.Prefix, limits)
	} else if params.Key != "" {
		// Extract from storage
		files, err = archiver.ExtractZip(ctx, params.Key, params.Prefix, limits)
	} else {
		return ExtractResult{Err: fmt.Errorf("either Key or File must be specified")}
	}

	if err != nil {
		return ExtractResult{Err: err}
	}

	return ExtractResult{ExtractedFiles: files}
}

func loadLimits(params url.Values, config *Config) *ExtractLimits {
	limits := DefaultExtractLimits(config)

	{
		maxFileSize, err := getUint64Param(params, "maxFileSize")
		if err == nil {
			limits.MaxFileSize = maxFileSize
		}
	}

	{
		maxTotalSize, err := getUint64Param(params, "maxTotalSize")
		if err == nil {
			limits.MaxTotalSize = maxTotalSize
		}
	}

	{
		maxNumFiles, err := getIntParam(params, "maxNumFiles")
		if err == nil {
			limits.MaxNumFiles = maxNumFiles
		}
	}

	{
		maxFileNameLength, err := getIntParam(params, "maxFileNameLength")
		if err == nil {
			limits.MaxFileNameLength = maxFileNameLength
		}
	}

	return limits
}

func extractHandler(w http.ResponseWriter, r *http.Request) error {
	params := r.URL.Query()
	key, err := getParam(params, "key")
	if err != nil {
		return err
	}

	prefix, err := getParam(params, "prefix")
	if err != nil {
		return err
	}

	hasLock := extractLockTable.tryLockKey(key)
	if !hasLock {
		// already being extracted in another handler, ask consumer to wait
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	limits := loadLimits(params, globalConfig)
	ops := NewOperations(globalConfig)

	extractParams := ExtractParams{
		Key:    key,
		Prefix: prefix,
		Limits: limits,
	}

	// sync codepath
	asyncURL := params.Get("async")
	if asyncURL == "" {
		defer extractLockTable.releaseKey(key)

		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

		result := ops.Extract(ctx, extractParams)
		if result.Err != nil {
			globalMetrics.TotalErrors.Add(1)
			return writeJSONError(w, "ExtractError", result.Err)
		}

		return writeJSONMessage(w, struct {
			Success        bool
			ExtractedFiles []ExtractedFile
		}{true, result.ExtractedFiles})
	}

	// async codepath
	go (func() {
		defer extractLockTable.releaseKey(key)

		// This job is expected to outlive the incoming request, so create a detached context.
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

		result := ops.Extract(ctx, extractParams)
		resValues := url.Values{}

		if result.Err != nil {
			errMessage := result.Err.Error()

			if errors.Is(result.Err, context.DeadlineExceeded) {
				errMessage = "Zip extraction timed out"
			}

			globalMetrics.TotalErrors.Add(1)
			resValues.Add("Type", "ExtractError")
			resValues.Add("Error", errMessage)
			log.Print("Extraction failed ", result.Err)
		} else {
			resValues.Add("Success", "true")
			for idx, extractedFile := range result.ExtractedFiles {
				resValues.Add(fmt.Sprintf("ExtractedFiles[%d][Key])", idx+1),
					extractedFile.Key)
				resValues.Add(fmt.Sprintf("ExtractedFiles[%d][Size])", idx+1),
					fmt.Sprintf("%v", extractedFile.Size))
			}
		}

		log.Print("Notifying " + asyncURL)

		nofityCtx, nofifyCancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.AsyncNotificationTimeout))
		defer nofifyCancel()

		outBody := bytes.NewBufferString(resValues.Encode())
		req, err := http.NewRequestWithContext(nofityCtx, http.MethodPost, asyncURL, outBody)
		if err != nil {
			log.Printf("Failed to create callback request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		asyncResponse, err := http.DefaultClient.Do(req)
		if err == nil {
			asyncResponse.Body.Close()
		} else {
			log.Print("Failed to deliver callback: " + err.Error())
		}
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
