package zipserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var globalConfig *Config
var errUnauthorized = errors.New("unauthorized")

type wrapErrors func(http.ResponseWriter, *http.Request) error

func (fn wrapErrors) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	globalMetrics.TotalRequests.Add(1)

	rec := &responseRecorder{ResponseWriter: w}

	if !authorizedRequest(globalConfig, r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(rec, errUnauthorized.Error(), http.StatusUnauthorized)
		logAccess(r, rec.Status(), rec.bytes)
		return
	}

	if err := fn(rec, r); err != nil {
		globalMetrics.TotalErrors.Add(1)
		log.Println("Error", r.Method, r.URL.Path, err)
		http.Error(rec, err.Error(), 500)
	}

	logAccess(r, rec.Status(), rec.bytes)
}

func authorizedRequest(config *Config, r *http.Request) bool {
	if config == nil || config.AuthBearerToken == "" {
		return true
	}

	auth := r.Header.Get("Authorization")
	scheme, token, ok := strings.Cut(auth, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.Contains(token, " ") {
		return false
	}

	return constantTimeStringEqual(token, config.AuthBearerToken)
}

func constantTimeStringEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// get the first value of param or error
func getParam(params url.Values, name string) (string, error) {
	val := params.Get(name)

	if val == "" {
		return "", fmt.Errorf("Missing param %v", name)
	}

	return val, nil
}

func getUint64Param(params url.Values, name string) (uint64, error) {
	valStr, err := getParam(params, name)
	if err != nil {
		return 0, err
	}

	valUint64, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return valUint64, nil
}

func getIntParam(params url.Values, name string) (int, error) {
	valStr, err := getParam(params, name)
	if err != nil {
		return 0, err
	}

	valInt, err := strconv.Atoi(valStr)
	if err != nil {
		return 0, err
	}

	return valInt, nil
}

func writeJSONMessage(w http.ResponseWriter, msg any) error {
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
		Success bool
		Type    string
		Error   string
	}{false, kind, err.Error()})
}

func versionHandler(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "zipserver %s\n", globalConfig.Version)
	fmt.Fprintf(w, "  commit: %s\n", globalConfig.CommitSHA)
	fmt.Fprintf(w, "  built:  %s\n", globalConfig.BuildTime)
	return nil
}

func statusHandler(w http.ResponseWriter, r *http.Request) error {
	copyKeys := copyLockTable.GetLocks()
	extractKeys := extractLockTable.GetLocks()
	slurpKeys := slurpLockTable.GetLocks()
	deleteKeys := deleteLockTable.GetLocks()
	pendingCallbacks := globalCallbackQueue.GetPending()

	return writeJSONMessage(w, struct {
		CopyLocks        []KeyInfo             `json:"copy_locks"`
		ExtractLocks     []KeyInfo             `json:"extract_locks"`
		SlurpLocks       []KeyInfo             `json:"slurp_locks"`
		DeleteLocks      []KeyInfo             `json:"delete_locks"`
		PendingCallbacks []PendingCallbackInfo `json:"pending_callbacks"`
	}{
		CopyLocks:        copyKeys,
		ExtractLocks:     extractKeys,
		SlurpLocks:       slurpKeys,
		DeleteLocks:      deleteKeys,
		PendingCallbacks: pendingCallbacks,
	})
}

// StartZipServer starts listening for extract and slurp requests
func StartZipServer(listenTo string, _config *Config) error {
	globalConfig = _config
	// Size the process-wide compression limiter up front so the field is never
	// written while serving (e.g. by list_handler copying globalConfig).
	globalConfig.getCompressLimiter()

	if err := initAccessLog(globalConfig.AccessLog); err != nil {
		return err
	}
	if globalConfig.AccessLog != "" {
		log.Print("Access log: " + globalConfig.AccessLog)
	}
	if globalConfig.AuthBearerToken != "" {
		log.Print("Bearer token authentication: enabled")
	} else {
		log.Print("Bearer token authentication: disabled (all endpoints are public)")
	}

	mux := http.NewServeMux()

	mux.Handle("/", wrapErrors(versionHandler))

	// Extract a .zip file (downloaded from GCS), stores each
	// individual file on GCS in a given bucket/prefix
	mux.Handle("/extract", wrapErrors(extractHandler))

	mux.Handle("/copy", wrapErrors(copyHandler))
	mux.Handle("/delete", wrapErrors(deleteHandler))

	// show the files in the zip
	mux.Handle("/list", wrapErrors(listHandler))

	// Download a file from an http{,s} URL and store it on GCS
	mux.Handle("/slurp", wrapErrors(slurpHandler))

	mux.Handle("/peek", wrapErrors(peekHandler))

	mux.Handle("/status", wrapErrors(statusHandler))
	mux.Handle("/metrics", wrapErrors(metricsHandler))

	log.Print("Listening on: " + listenTo)

	server := &http.Server{Addr: listenTo, Handler: mux}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err := <-serverErr:
		return err
	case sig := <-signals:
		// A second signal falls through to the default handler and kills the process.
		signal.Stop(signals)
		return drainServer(server, sig)
	}
}

// drainServer stops accepting connections, then waits for in-flight
// requests, detached async jobs, and pending callbacks up to ShutdownTimeout.
func drainServer(server *http.Server, sig os.Signal) error {
	timeout := time.Duration(globalConfig.ShutdownTimeout)
	log.Printf("Received %s, draining (timeout %v)", sig, timeout)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Print("Drain: closing listener, waiting for in-flight requests")
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Drain: HTTP shutdown incomplete: %s", err)
	}

	log.Printf("Drain: waiting for async jobs: %s", describeActiveWork())
	if err := waitGroupWithContext(ctx, &activeJobs); err != nil {
		log.Printf("Drain: deadline reached, exiting with work still running: %s", describeActiveWork())
		return nil
	}

	log.Printf("Drain: waiting for %d pending callbacks", len(globalCallbackQueue.GetPending()))
	if err := globalCallbackQueue.Wait(ctx); err != nil {
		log.Printf("Drain: deadline reached, exiting with %d callbacks still pending", len(globalCallbackQueue.GetPending()))
		return nil
	}

	log.Print("Drain complete")
	return nil
}

// describeActiveWork summarizes held locks and pending callbacks for drain logs.
func describeActiveWork() string {
	parts := []string{}
	describe := func(label string, locks []KeyInfo) {
		if len(locks) == 0 {
			return
		}
		keys := make([]string, len(locks))
		for i, lock := range locks {
			keys[i] = lock.Key
		}
		parts = append(parts, fmt.Sprintf("%d %s [%s]", len(locks), label, strings.Join(keys, ", ")))
	}
	describe("extract", extractLockTable.GetLocks())
	describe("copy", copyLockTable.GetLocks())
	describe("slurp", slurpLockTable.GetLocks())
	describe("delete", deleteLockTable.GetLocks())
	if pending := len(globalCallbackQueue.GetPending()); pending > 0 {
		parts = append(parts, fmt.Sprintf("%d pending callbacks", pending))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, "; ")
}
