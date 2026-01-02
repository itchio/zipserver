package zipserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var deleteLockTable = NewLockTable()

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
	storage *S3Storage,
	bucket, targetName string,
	tasks <-chan DeleteTask,
	results chan<- DeleteResult,
	done chan struct{},
) {
	defer func() { done <- struct{}{} }()

	for task := range tasks {
		lockKey := fmt.Sprintf("%s:%s", targetName, task.Key)
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

	callbackURL, err := getParam(params, "callback")
	if err != nil {
		return err
	}

	targetName, err := getParam(params, "target")
	if err != nil {
		return err
	}

	storageTargetConfig := globalConfig.GetStorageTargetByName(targetName)
	if storageTargetConfig == nil {
		return fmt.Errorf("invalid target: %s", targetName)
	}

	targetBucket := storageTargetConfig.Bucket

	go func() {
		jobCtx, cancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.JobTimeout))
		defer cancel()

		targetStorage, err := storageTargetConfig.NewStorageClient()
		if err != nil {
			notifyError(callbackURL, fmt.Errorf("failed to create target storage: %v", err))
			return
		}

		numWorkers := globalConfig.ExtractionThreads
		if numWorkers < 1 {
			numWorkers = 4
		}

		tasks := make(chan DeleteTask)
		results := make(chan DeleteResult)
		done := make(chan struct{}, numWorkers)

		for i := 0; i < numWorkers; i++ {
			go deleteWorker(jobCtx, targetStorage, targetBucket, targetName, tasks, results, done)
		}

		// Send tasks to workers
		go func() {
			defer close(tasks)
			for _, key := range keys {
				select {
				case tasks <- DeleteTask{Key: key}:
				case <-jobCtx.Done():
					log.Println("Delete tasks were canceled")
					return
				}
			}
		}()

		// Collect results
		activeWorkers := numWorkers
		deletedCount := 0
		var deleteErrors []DeleteError

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
				}
			case <-done:
				activeWorkers--
			}
		}

		close(results)

		log.Printf("Delete complete: [%s] deleted %d/%d files", targetName, deletedCount, len(keys))

		resValues := url.Values{}
		resValues.Add("Success", fmt.Sprintf("%t", len(deleteErrors) == 0))
		resValues.Add("TotalKeys", fmt.Sprintf("%d", len(keys)))
		resValues.Add("DeletedKeys", fmt.Sprintf("%d", deletedCount))

		if len(deleteErrors) > 0 {
			errorsJSON, _ := json.Marshal(deleteErrors)
			resValues.Add("Errors", string(errorsJSON))
		}

		notifyCallback(callbackURL, resValues)
	}()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
