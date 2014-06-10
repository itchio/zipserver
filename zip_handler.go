package zip_server

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

	process := func() ([]ExtractedFile, error) {
		lockKey(key)
		archiver := NewArchiver(config)
		files, err := archiver.ExtractZip(key, prefix)

		if err != nil {
			releaseKey(key)
		} else {
			releaseKeyLater(key)
		}

		return files, err
	}

	asyncURL := params["async"]
	if len(asyncURL) == 0 {
		extracted, err := process()
		if err != nil {
			return writeJSONMessage(w, struct {
				Type  string
				Error string
			}{"ExtractError", err.Error()})
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

		log.Print("notifying " + asyncURL[0])
		_, err = http.PostForm(asyncURL[0], resValues)
		if err != nil {
			log.Print("Failed to deliver callback: " + err.Error())
		}
	})()

	return writeJSONMessage(w, struct {
		Processing bool
		Async      bool
	}{true, true})
}
