package zipserver

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
)

var shared struct {
	// maps aren't thread-safe in golang, this protects openKeys
	sync.Mutex
	openKeys map[string]interface{}
}

func init() {
	shared.openKeys = make(map[string]interface{})
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
	shared.openKeys[key] = nil
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

func zipHandler(w http.ResponseWriter, r *http.Request) error {
	params := r.URL.Query()
	key, err := getParam(params, "key")
	if err != nil {
		return err
	}

	prefix, err := getParam(params, "prefix")
	if err != nil {
		return err
	}

	hasLock := tryLockKey(key)
	if !hasLock {
		// already being extracted in another handler, ask consumer to wait
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	limits := loadLimits(params, config)

	process := func() ([]ExtractedFile, error) {
		archiver := NewArchiver(config)
		files, err := archiver.ExtractZip(key, prefix, limits)

		return files, err
	}

	// sync codepath
	asyncURL := params.Get("async")
	if asyncURL == "" {
		defer releaseKey(key)

		extracted, err := process()
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

		extracted, err := process()
		resValues := url.Values{}

		if err != nil {
			resValues.Add("Type", "ExtractError")
			resValues.Add("Error", err.Error())
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
		asyncResponse, err := http.PostForm(asyncURL, resValues)
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
