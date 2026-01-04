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

	var archiver *ArchiveExtractor

	if params.TargetName != "" {
		// Validate target exists
		storageTargetConfig := o.config.GetStorageTargetByName(params.TargetName)
		if storageTargetConfig == nil {
			return ExtractResult{Err: fmt.Errorf("invalid target: %s", params.TargetName)}
		}

		// Check readonly flag
		if storageTargetConfig.Readonly {
			return ExtractResult{Err: fmt.Errorf("target %s is readonly", params.TargetName)}
		}

		// Create target storage client
		targetStorage, err := storageTargetConfig.NewStorageClient()
		if err != nil {
			return ExtractResult{Err: fmt.Errorf("failed to create target storage: %v", err)}
		}

		archiver = NewArchiveExtractorWithTarget(o.config, targetStorage, storageTargetConfig.Bucket)
	} else {
		archiver = NewArchiveExtractor(o.config)
	}

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

func loadLimits(params url.Values, config *Config) (*ExtractLimits, error) {
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

	{
		maxZipSize, err := getUint64Param(params, "maxInputZipSize")
		if err == nil {
			limits.MaxInputZipSize = maxZipSize
		}
	}

	filter := params.Get("filter")
	onlyFiles := params["only_files[]"]

	if filter != "" && len(onlyFiles) > 0 {
		return nil, fmt.Errorf("only_files[] and filter cannot be used together")
	}

	if filter != "" {
		limits.IncludeGlob = filter
	}

	if len(onlyFiles) > 0 {
		limits.OnlyFiles = onlyFiles
	}

	return limits, nil
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

	// Optional target parameter
	targetName := params.Get("target")

	// Use a lock key that includes target to allow parallel extractions to different targets
	lockKey := key
	if targetName != "" {
		lockKey = fmt.Sprintf("%s:%s", targetName, key)
	}

	hasLock := extractLockTable.tryLockKey(lockKey)
	if !hasLock {
		// already being extracted in another handler, ask consumer to wait
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	limits, err := loadLimits(params, globalConfig)
	if err != nil {
		extractLockTable.releaseKey(lockKey)
		return writeJSONError(w, "InvalidParams", err)
	}
	ops := NewOperations(globalConfig)

	extractParams := ExtractParams{
		Key:        key,
		Prefix:     prefix,
		Limits:     limits,
		TargetName: targetName,
	}

	// sync codepath
	asyncURL := params.Get("async")
	if asyncURL == "" {
		defer extractLockTable.releaseKey(lockKey)

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
		defer extractLockTable.releaseKey(lockKey)

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
