package zipserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var shared struct {
	// maps aren't thread-safe in golang, this protects openKeys
	sync.Mutex
	// empty struct is zero-width, we're using that map as a set (no values)
	openKeys map[string]struct{}
}

func init() {
	shared.openKeys = make(map[string]struct{})
}

// tryLockKey tries acquiring the lock for a given key
// it returns true if we successfully acquired the lock,
// false if the key is locked by someone else
func tryLockKey(key string) bool {
	shared.Lock()
	defer shared.Unlock()

	// test for key existence
	if _, ok := shared.openKeys[key]; ok {
		// locked by someone else
		return false
	}
	shared.openKeys[key] = struct{}{}
	return true
}

func releaseKey(key string) {
	shared.Lock()
	defer shared.Unlock()

	// delete key from map so the map doesn't keep growing
	delete(shared.openKeys, key)
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

	fileListParam := params.Get("only_files")
	if err != nil {
		return err
	}
	var fileList []string
	if fileListParam != "" {
		if err := json.Unmarshal([]byte(fileListParam), &fileList); err != nil {
			return fmt.Errorf("unmarshal files param: %w", err)
		}
		if len(fileList) == 0 {
			return fmt.Errorf("fileList param was empty")
		}
	}

	hasLock := tryLockKey(key)
	if !hasLock {
		// already being extracted in another handler, ask consumer to wait
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	limits := loadLimits(params, config)

	analyzer := &GameAnalyzer{
		onlyExtractFiles: fileList,
	}

	process := func(ctx context.Context) ([]ExtractedFile, error) {
		archiver := NewArchiver(config)
		files, err := archiver.ExtractZip(ctx, key, prefix, limits, analyzer)

		return files, err
	}

	// sync codepath
	asyncURL := params.Get("async")
	if asyncURL == "" {
		defer releaseKey(key)

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
		defer releaseKey(key)

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

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Failed to deliver callback: %v", err)
			return
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent {
			body, err := io.ReadAll(res.Body)
			if err != nil {
				log.Printf("Read notification response: %v", err)
				return
			}
			log.Printf("Notification response: %s %s", res.Status, string(body))
		}
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
