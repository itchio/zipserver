package zipserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// validateKeysInExtractPrefix checks that all keys are within the extract prefix.
// This is used to restrict deletions on primary storage to only extracted files.
func validateKeysInExtractPrefix(keys []string, extractPrefix string) error {
	cleanPrefix := path.Clean(extractPrefix)
	prefix := cleanPrefix
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var invalid []string
	for _, key := range keys {
		if key == "" || path.IsAbs(key) {
			invalid = append(invalid, key)
			continue
		}
		cleanKey := path.Clean(key)
		if cleanKey == "." || cleanKey == ".." || strings.HasPrefix(cleanKey, "../") {
			invalid = append(invalid, key)
			continue
		}
		if !strings.HasPrefix(cleanKey, prefix) {
			invalid = append(invalid, key)
		}
	}
	if len(invalid) > 0 {
		return fmt.Errorf("keys must be within extract prefix %q: %v", extractPrefix, invalid)
	}
	return nil
}

var deleteLockTable = NewLockTable()

// Delete deletes files from a target storage.
// If TargetName is empty, deletes from primary storage (restricted to ExtractPrefix).
func (o *Operations) Delete(ctx context.Context, params DeleteParams) DeleteOperationResult {
	startTime := time.Now()

	var targetStorage Storage
	var targetBucket string
	var targetName string
	var err error

	if params.TargetName == "" {
		// Primary storage: validate keys are within extract prefix
		if err := validateKeysInExtractPrefix(params.Keys, o.config.ExtractPrefix); err != nil {
			return DeleteOperationResult{Err: err}
		}

		targetStorage, err = NewGcsStorage(o.config)
		if err != nil {
			return DeleteOperationResult{Err: fmt.Errorf("failed to create primary storage: %v", err)}
		}
		targetBucket = o.config.Bucket
		targetName = ""
	} else {
		// Named target storage
		storageTargetConfig := o.config.GetStorageTargetByName(params.TargetName)
		if storageTargetConfig == nil {
			return DeleteOperationResult{Err: fmt.Errorf("invalid target: %s", params.TargetName)}
		}

		if storageTargetConfig.Readonly {
			return DeleteOperationResult{Err: fmt.Errorf("target %s is readonly", params.TargetName)}
		}

		targetStorage, err = storageTargetConfig.NewStorageClient()
		if err != nil {
			return DeleteOperationResult{Err: fmt.Errorf("failed to create target storage: %v", err)}
		}
		targetBucket = storageTargetConfig.Bucket
		targetName = params.TargetName
	}

	numWorkers := o.config.ExtractionThreads
	if numWorkers < 1 {
		numWorkers = 4
	}

	tasks := make(chan DeleteTask)
	results := make(chan DeleteResult)
	done := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go deleteWorker(ctx, targetStorage, targetBucket, targetName, tasks, results, done)
	}

	// Send tasks to workers
	go func() {
		defer close(tasks)
		for _, key := range params.Keys {
			select {
			case tasks <- DeleteTask{Key: key}:
			case <-ctx.Done():
				log.Println("Delete tasks were canceled")
				return
			}
		}
	}()

	// Collect results
	activeWorkers := numWorkers
	deletedCount := 0
	var deleteErrors []DeleteError

	displayName := targetName
	if displayName == "" {
		displayName = "primary"
	}

	for activeWorkers > 0 {
		select {
		case result := <-results:
			if result.Error != nil {
				deleteErrors = append(deleteErrors, DeleteError{
					Key:   result.Key,
					Error: result.Error.Error(),
				})
			} else {
				deletedCount++
				globalMetrics.TotalDeletedFiles.Add(1)
				log.Printf("Deleted [%s] %s", displayName, result.Key)
			}
		case <-done:
			activeWorkers--
		}
	}

	close(results)

	duration := time.Since(startTime)
	log.Printf("Delete complete: [%s] deleted %d/%d files, duration: %.4fs", displayName, deletedCount, len(params.Keys), duration.Seconds())

	return DeleteOperationResult{
		TotalKeys:   len(params.Keys),
		DeletedKeys: deletedCount,
		Duration:    fmt.Sprintf("%.4fs", duration.Seconds()),
		Errors:      deleteErrors,
	}
}

type DeleteTask struct {
	Key string
}

type DeleteResult struct {
	Key   string
	Error error
}

type DeleteError struct {
	Key   string `json:"key"`
	Error string `json:"error"`
}

func deleteWorker(
	ctx context.Context,
	storage Storage,
	bucket, targetName string,
	tasks <-chan DeleteTask,
	results chan<- DeleteResult,
	done chan struct{},
) {
	defer func() { done <- struct{}{} }()

	for task := range tasks {
		lockKey := task.Key
		if targetName != "" {
			lockKey = fmt.Sprintf("%s:%s", targetName, task.Key)
		}
		if !deleteLockTable.tryLockKey(lockKey) {
			results <- DeleteResult{Key: task.Key, Error: fmt.Errorf("key is locked")}
			continue
		}

		err := storage.DeleteFile(ctx, bucket, task.Key)
		deleteLockTable.releaseKey(lockKey)
		results <- DeleteResult{Key: task.Key, Error: err}
	}
}

// The delete handler will asynchronously delete files from the storage specified by target
func deleteHandler(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "invalid method", http.StatusMethodNotAllowed)
		return nil
	}

	if err := r.ParseForm(); err != nil {
		return fmt.Errorf("failed to parse form: %w", err)
	}

	rawKeys := r.Form["keys[]"]
	if len(rawKeys) == 0 {
		return fmt.Errorf("missing keys[] parameter")
	}
	keys := make([]string, 0, len(rawKeys))
	for _, key := range rawKeys {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("keys[] contains empty value")
		}
		keys = append(keys, key)
	}

	params := r.Form

	callbackURL := params.Get("callback")

	// target is optional; if not provided, delete from primary storage (restricted to ExtractPrefix)
	// Validation of target and prefix constraints is handled by Operations.Delete
	targetName := params.Get("target")

	ops := NewOperations(globalConfig)
	deleteParams := DeleteParams{
		Keys:       keys,
		TargetName: targetName,
	}

	// sync codepath
	if callbackURL == "" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

		result := ops.Delete(ctx, deleteParams)
		if result.Err != nil {
			globalMetrics.TotalErrors.Add(1)
			return writeJSONError(w, "DeleteError", result.Err)
		}

		return writeJSONMessage(w, struct {
			Success     bool
			TotalKeys   int
			DeletedKeys int
			Duration    string
			Errors      []DeleteError
		}{len(result.Errors) == 0, result.TotalKeys, result.DeletedKeys, result.Duration, result.Errors})
	}

	// async codepath
	go func() {
		jobCtx, cancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

		result := ops.Delete(jobCtx, deleteParams)

		if result.Err != nil {
			if callbackURL == "-" {
				globalMetrics.TotalErrors.Add(1)
				log.Printf("DeleteError async (callback=-): %v", result.Err)
				return
			}
			notifyError(callbackURL, result.Err)
			return
		}

		if callbackURL != "-" {
			resValues := url.Values{}
			resValues.Add("Success", fmt.Sprintf("%t", len(result.Errors) == 0))
			resValues.Add("TotalKeys", fmt.Sprintf("%d", result.TotalKeys))
			resValues.Add("DeletedKeys", fmt.Sprintf("%d", result.DeletedKeys))
			resValues.Add("Duration", result.Duration)

			if len(result.Errors) > 0 {
				errorsJSON, _ := json.Marshal(result.Errors)
				resValues.Add("Errors", string(errorsJSON))
			}

			notifyCallback(callbackURL, resValues)
		} else if len(result.Errors) > 0 {
			globalMetrics.TotalErrors.Add(1)
			log.Printf("Delete async (callback=-) had %d errors: %v", len(result.Errors), result.Errors)
		}
	}()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
