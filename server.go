package zip_server

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"

	"fmt"
)

var config *Config

type errorHandler func(http.ResponseWriter, *http.Request) error

func (fn errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := fn(w, r); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

// get the first value of param or error
func getParam(params url.Values, name string) (string, error) {
	val := params.Get(name)

	if val == "" {
		return "", fmt.Errorf("Missing param %v", name)
	}

	return val, nil
}

func writeJSONMessage(w http.ResponseWriter, msg interface{}) error {
	blob, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	w.Header()["Content-Type"] = []string{"application/json"}
	w.Write(blob)
	return nil
}

func writeJSONError(w http.ResponseWriter, kind string, err error) error {
	return writeJSONMessage(w, struct {
		Type  string
		Error string
	}{kind, err.Error()})
}

func StartZipServer(listenTo string, _config *Config) error {
	config = _config
	http.Handle("/extract_zip", errorHandler(zipHandler))
	http.Handle("/slurp", errorHandler(slurpHandler))

	log.Print("Listening on: " + listenTo)
	return http.ListenAndServe(listenTo, nil)
}
