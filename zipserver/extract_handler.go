package zipserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type LockTable struct {
	// maps aren't thread-safe in golang, this protects openKeys
	sync.Mutex
	// empty struct is zero-width, we're using that map as a set (no values)
	openKeys map[string]struct{}
}

func NewLockTable() *LockTable {
	return &LockTable{
		openKeys: make(map[string]struct{}),
	}
}

// tryLockKey tries acquiring the lock for a given key
// it returns true if we successfully acquired the lock,
// false if the key is locked by someone else
func (lt *LockTable) tryLockKey(key string) bool {
	lt.Lock()
	defer lt.Unlock()

	// test for key existence
	if _, ok := lt.openKeys[key]; ok {
		// locked by someone else
		return false
	}
	lt.openKeys[key] = struct{}{}
	return true
}

func (lt *LockTable) releaseKey(key string) {
	lt.Lock()
	defer lt.Unlock()

	// delete key from map so the map doesn't keep growing
	delete(lt.openKeys, key)
}

// create static instance of LockTable for this code file to use
var extractLockTable = NewLockTable()

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

	limits := loadLimits(params, config)

	process := func(ctx context.Context) ([]ExtractedFile, error) {
		archiver := NewArchiver(config)
		files, err := archiver.ExtractZip(ctx, key, prefix, limits)

		return files, err
	}

	// sync codepath
	asyncURL := params.Get("async")
	if asyncURL == "" {
		defer extractLockTable.releaseKey(key)

		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(config.JobTimeout))
		defer cancel()

		extracted, err := process(ctx)
		if err != nil {
			return writeJSONError(w, "ExtractError", err)
		}

		return writeJSONMessage(w, struct {
			Success        bool
			ExtractedFiles []ExtractedFile
		}{true, extracted})
	}

	// async codepath
	go (func() {
		defer extractLockTable.releaseKey(key)

		// This job is expected to outlive the incoming request, so create a detached context.
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
		defer cancel()

		extracted, err := process(ctx)
		resValues := url.Values{}

		if err != nil {
			errMessage := err.Error()

			if errors.Is(err, context.DeadlineExceeded) {
				errMessage = "Zip extraction timed out"
			}

			resValues.Add("Type", "ExtractError")
			resValues.Add("Error", errMessage)
			log.Print("Extraction failed ", err)
		} else {
			resValues.Add("Success", "true")
			for idx, extractedFile := range extracted {
				resValues.Add(fmt.Sprintf("ExtractedFiles[%d][Key])", idx+1),
					extractedFile.Key)
				resValues.Add(fmt.Sprintf("ExtractedFiles[%d][Size])", idx+1),
					fmt.Sprintf("%v", extractedFile.Size))
			}
		}

		log.Print("Notifying " + asyncURL)

		nofityCtx, nofifyCancel := context.WithTimeout(context.Background(), time.Duration(config.AsyncNotificationTimeout))
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
