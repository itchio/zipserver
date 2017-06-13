package zipserver

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var shared struct {
	sync.Mutex
	openKeys map[string]bool
}

func init() {
	shared.openKeys = make(map[string]bool)
}

func keyBusy(key string) bool {
	shared.Lock()
	defer shared.Unlock()
	return shared.openKeys[key]
}

func lockKey(key string) {
	shared.Lock()
	defer shared.Unlock()
	shared.openKeys[key] = true
}

func releaseKey(key string) {
	shared.Lock()
	defer shared.Unlock()
	shared.openKeys[key] = false
}

// release the key later to give the initial requester time to update the
// database
func releaseKeyLater(key string) {
	go func() {
		<-time.After(10 * time.Second)
		releaseKey(key)
	}()
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

	if keyBusy(key) {
		return writeJSONMessage(w, struct{ Processing bool }{true})
	}

	limits := loadLimits(params, config)

	process := func() ([]ExtractedFile, error) {
		lockKey(key)
		archiver := NewArchiver(config)
		files, err := archiver.ExtractZip(key, prefix, limits)

		if err != nil {
			releaseKey(key)
		} else {
			releaseKeyLater(key)
		}

		return files, err
	}

	asyncURL := params.Get("async")
	if asyncURL == "" {
		extracted, err := process()
		if err != nil {
			return writeJSONError(w, "ExtractError", err)
		}

		return writeJSONMessage(w, struct {
			Success        bool
			ExtractedFiles []ExtractedFile
		}{true, extracted})
	}

	go (func() {
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
