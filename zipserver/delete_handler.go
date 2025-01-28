package zipserver

// bulk delete files from a storage target to clean up previous extractions of copies

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// TODO: we should have a global storage:bucket:key lock table to prevent concurrent operations across all handlers
var deleteLockTable = NewLockTable()

type DeleteResult struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// deleteHandler handles bulk deletion of files from a specified storage target.
// It processes an HTTP request with an array of keys to delete, validates the storage target,
// and performs concurrent file deletions with optional asynchronous callback notification.
//
// The function supports two modes of operation:
// 1. Synchronous: Returns immediate results of deletion attempts
// 2. Asynchronous: Sends results to a specified callback URL while returning an immediate response
//
// Parameters:
//   - w: HTTP response writer for sending the response
//   - r: HTTP request containing deletion parameters
//
// Request form parameters:
//   - keys[]: Array of file keys to delete (required)
//   - target: Name of the storage target (required)
//   - callback: Optional URL to receive asynchronous deletion results
//
// Returns an error if request parsing, parameter validation, or processing fails.
// Handles concurrent deletions with per-key locking to prevent race conditions.
func deleteHandler(w http.ResponseWriter, r *http.Request) error {
	err := r.ParseForm()
	if err != nil {
		return err
	}

	keys, ok := r.Form["keys[]"]
	if !ok {
		return fmt.Errorf("Missing keys[] parameter")
	}

	callbackURL, err := getParam(r.Form, "callback")
	if err != nil {
		return err
	}

	targetName, err := getParam(r.Form, "target")
	if err != nil {
		return err
	}

	storageTargetConfig := globalConfig.GetStorageTargetByName(targetName)
	if storageTargetConfig == nil {
		return fmt.Errorf("Invalid target: %s", targetName)
	}

	if storageTargetConfig.Readonly {
		return fmt.Errorf("Target storage is readonly: %s", targetName)
	}

	targetBucket := storageTargetConfig.Bucket

	var wg sync.WaitGroup
	var result sync.Map

	for _, key := range keys {
		lockKey := fmt.Sprintf("%s:%s", targetName, key)
		if !deleteLockTable.tryLockKey(lockKey) {
			result.Store(key, DeleteResult{
				Success: false,
				Error:   "Failed to acquire lock",
			})
			continue
		}

		wg.Add(1)
		go (func(key string) {
			defer wg.Done()
			defer deleteLockTable.releaseKey(lockKey)
			jobCtx, cancel := context.WithTimeout(context.Background(), time.Duration(globalConfig.JobTimeout))
			defer cancel()

			storage, err := storageTargetConfig.NewStorageClient()
			if err != nil {
				log.Print("Failed to create target storage for delete file: ", err)
				result.Store(key, DeleteResult{
					Success: false,
					Error:   err.Error(),
				})
				return
			}

			err = storage.DeleteFile(jobCtx, targetBucket, key)

			if err != nil {
				log.Print("Failed to delete file: ", err)
				result.Store(key, DeleteResult{
					Success: false,
					Error:   err.Error(),
				})
				return
			}

			result.Store(key, DeleteResult{
				Success: true,
			})
		})(key)
	}

	waitForResult := func() map[string]DeleteResult {
		wg.Wait()

		finalResult := make(map[string]DeleteResult)
		result.Range(func(key, value interface{}) bool {
			finalResult[key.(string)] = value.(DeleteResult)
			return true
		})

		return finalResult
	}

	if callbackURL == "" {
		deletedFiles := waitForResult()

		return writeJSONMessage(w, struct {
			Success      bool
			DeletedFiles map[string]DeleteResult
		}{true, deletedFiles})
	} else {
		go (func() {
			result := waitForResult()
			resValues := url.Values{"Success": {"true"}}
			for key, deleteResult := range result {
				resValues.Add("DeletedFiles["+key+"][Success]", fmt.Sprintf("%v", deleteResult.Success))
				if !deleteResult.Success {
					resValues.Add("DeletedFiles["+key+"][Error]", deleteResult.Error)
				}
			}

			notifyCallback(callbackURL, resValues)
		})()

		return writeJSONMessage(w, struct {
			Processing bool
			Async      bool
		}{true, true})
	}

}
