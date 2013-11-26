package zip_server

import (
	"net/http"
	"net/url"
	"time"
	"encoding/json"
	"log"

	"fmt"
	"sync"
)

var config *Config

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

type errorHandler func(http.ResponseWriter, *http.Request) error

func (fn errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := fn(w,r); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

// get the first value of param or error
func getParam(params url.Values, name string) (string, error) {
	vals := params[name]

	if len(vals) == 0 {
		return "", fmt.Errorf("Missing param %v", name)
	}

	val := vals[0]

	if len(val) == 0 {
		return "", fmt.Errorf("Missing param %v", name)
	}

	return val, nil
}

func writeJsonMessage(w http.ResponseWriter, msg interface{}) error {
	blob, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	w.Header()["Content-Type"] = []string{"application/json"}
	w.Write(blob)
	return nil
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
		return writeJsonMessage(w, struct{Processing bool}{true})
	}

	lockKey(key)

	archiver := NewArchiver(config)
	extracted, err := archiver.ExtractZip(key, prefix)

	if err != nil {
		releaseKey(key)
		return writeJsonMessage(w, struct{
			Type string
			Error string
		}{"ExtractError", err.Error()})
	}

	releaseKeyLater(key)
	return writeJsonMessage(w, struct{
		Success bool
		ExtractedFiles []ExtractedFile
	}{true, extracted})
}

func StartZipServer(listenTo string, _config *Config) error {
	config = _config
	http.Handle("/", errorHandler(zipHandler))
	log.Print("Listening on: " + listenTo)
	return http.ListenAndServe(listenTo, nil)
}


