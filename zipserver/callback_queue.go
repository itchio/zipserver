package zipserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// fallback per-attempt timeout when AsyncNotificationTimeout is unset
const defaultCallbackTimeout = 5 * time.Second

// how much of a failed callback response body gets logged
const callbackBodyLogLimit = 1024

var globalCallbackQueue = NewCallbackQueue()

// CallbackQueue holds callbacks awaiting delivery and retries them on a
// backoff schedule. Delivery is in-memory only: pending callbacks are lost on
// restart, so callers should treat the callback as best-effort and keep their
// own deadline as a backstop.
type CallbackQueue struct {
	mu      sync.Mutex
	pending map[int64]*PendingCallback
	nextID  int64
	wg      jobCounter
}

// PendingCallback tracks one callback through its delivery attempts. Mutable
// fields (Attempts, NextAttempt, LastError) are only accessed while holding
// the queue mutex.
type PendingCallback struct {
	ID          int64
	URL         string
	Values      url.Values
	CreatedAt   time.Time
	Attempts    int
	NextAttempt time.Time
	LastError   string

	// snapshotted from config at enqueue time; the worker goroutine must not
	// read globalConfig, which tests swap out from under long-lived workers
	timeout time.Duration
	delays  []time.Duration
}

// PendingCallbackInfo is a snapshot of a pending callback for the status
// endpoint. URLs and errors are redacted since callback URLs may carry
// tokens in their query string.
type PendingCallbackInfo struct {
	URL            string
	CreatedAt      time.Time
	PendingSeconds float64
	Attempts       int
	NextAttempt    time.Time
	LastError      string
}

func NewCallbackQueue() *CallbackQueue {
	return &CallbackQueue{
		pending: make(map[int64]*PendingCallback),
	}
}

// Enqueue schedules delivery of values to callbackURL and returns
// immediately. The POST is attempted once plus once per entry in
// config.CallbackRetryDelays; a callback that still fails after the final
// attempt is dropped.
func (q *CallbackQueue) Enqueue(config *Config, callbackURL string, values url.Values) {
	timeout := time.Duration(config.AsyncNotificationTimeout)
	if timeout <= 0 {
		timeout = defaultCallbackTimeout
	}

	delays := make([]time.Duration, len(config.CallbackRetryDelays))
	for i, d := range config.CallbackRetryDelays {
		delays[i] = time.Duration(d)
	}

	q.mu.Lock()
	q.nextID++
	entry := &PendingCallback{
		ID:          q.nextID,
		URL:         callbackURL,
		Values:      values,
		CreatedAt:   time.Now(),
		NextAttempt: time.Now(),
		timeout:     timeout,
		delays:      delays,
	}
	q.pending[entry.ID] = entry
	q.mu.Unlock()

	q.wg.Add(1)
	go q.deliver(entry)
}

// Wait blocks until every pending callback has been delivered or dropped, or
// ctx expires.
func (q *CallbackQueue) Wait(ctx context.Context) error {
	return q.wg.Wait(ctx)
}

func (q *CallbackQueue) deliver(entry *PendingCallback) {
	defer q.wg.Done()
	totalAttempts := len(entry.delays) + 1

	for attempt := 0; ; attempt++ {
		final := attempt >= len(entry.delays)
		err := attemptCallback(entry, final)

		if err == nil {
			q.mu.Lock()
			entry.Attempts = attempt + 1
			delete(q.pending, entry.ID)
			q.mu.Unlock()
			return
		}

		globalMetrics.TotalCallbackFailures.Add(1)
		errText := describeCallbackError(err)

		if final {
			globalMetrics.TotalCallbackPermanentFailures.Add(1)
			log.Printf("Callback permanently failed after %d attempts: %s: %s",
				totalAttempts, redactURL(entry.URL), errText)

			q.mu.Lock()
			delete(q.pending, entry.ID)
			q.mu.Unlock()
			return
		}

		delay := entry.delays[attempt]
		globalMetrics.TotalCallbackRetries.Add(1)
		log.Printf("Callback failed (attempt %d/%d), retrying in %v: %s: %s",
			attempt+1, totalAttempts, delay, redactURL(entry.URL), errText)

		q.mu.Lock()
		entry.Attempts = attempt + 1
		entry.LastError = errText
		entry.NextAttempt = time.Now().Add(delay)
		q.mu.Unlock()

		time.Sleep(delay)
	}
}

// attemptCallback makes a single delivery attempt. Any response outside the
// 2xx range counts as a failure so a temporarily unhealthy receiver gets the
// callback again.
func attemptCallback(entry *PendingCallback, final bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), entry.timeout)
	defer cancel()

	outBody := bytes.NewBufferString(entry.Values.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, entry.URL, outBody)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// drain (capped) so the connection can be reused between attempts
	bodyExcerpt, _ := io.ReadAll(io.LimitReader(response.Body, callbackBodyLogLimit))
	io.Copy(io.Discard, response.Body)

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if final && len(bodyExcerpt) > 0 {
			log.Printf("Callback response body: %s", bodyExcerpt)
		}
		return fmt.Errorf("callback returned status %d", response.StatusCode)
	}

	return nil
}

// GetPending returns a snapshot of callbacks awaiting delivery, for the
// status endpoint.
func (q *CallbackQueue) GetPending() []PendingCallbackInfo {
	q.mu.Lock()
	defer q.mu.Unlock()

	infos := make([]PendingCallbackInfo, 0, len(q.pending))
	for _, entry := range q.pending {
		infos = append(infos, PendingCallbackInfo{
			URL:            redactURL(entry.URL),
			CreatedAt:      entry.CreatedAt,
			PendingSeconds: time.Since(entry.CreatedAt).Seconds(),
			Attempts:       entry.Attempts,
			NextAttempt:    entry.NextAttempt,
			LastError:      entry.LastError,
		})
	}
	return infos
}

// redactURL strips credentials, query, and fragment, which may contain
// signed tokens
func redactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<unparseable url>"
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// describeCallbackError renders an error for logs and the status endpoint;
// url.Error embeds the full request URL, so rebuild it around the redacted one
func describeCallbackError(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Sprintf("%s %s: %v", urlErr.Op, redactURL(urlErr.URL), urlErr.Err)
	}
	return err.Error()
}

// notify the callback URL of task completion; delivery is queued and retried
// in the background, so this returns immediately
func notifyCallback(callbackURL string, resValues url.Values) {
	globalCallbackQueue.Enqueue(globalConfig, callbackURL, resValues)
}

// notify the callback URL that an error happened
func notifyError(callbackURL string, err error) {
	globalMetrics.TotalErrors.Add(1)

	message := url.Values{}
	message.Add("Success", "false")
	message.Add("Error", err.Error())
	notifyCallback(callbackURL, message)
}
