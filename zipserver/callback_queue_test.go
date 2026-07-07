package zipserver

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func callbackTestConfig(delays ...time.Duration) *Config {
	config := &Config{
		AsyncNotificationTimeout: Duration(2 * time.Second),
		CallbackRetryDelays:      make([]Duration, 0, len(delays)),
	}
	for _, d := range delays {
		config.CallbackRetryDelays = append(config.CallbackRetryDelays, Duration(d))
	}
	return config
}

// waitEmpty polls until the queue has no pending callbacks or the timeout
// elapses
func (q *CallbackQueue) waitEmpty(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		q.mu.Lock()
		n := len(q.pending)
		q.mu.Unlock()
		if n == 0 {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

func TestCallbackQueue_FirstAttemptSuccess(t *testing.T) {
	retriesBefore := globalMetrics.TotalCallbackRetries.Load()
	permanentBefore := globalMetrics.TotalCallbackPermanentFailures.Load()

	var requests atomic.Int32
	received := make(chan url.Values, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.NoError(t, r.ParseForm())
		received <- r.Form
		// any 2xx counts as delivered
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	queue := NewCallbackQueue()
	values := url.Values{}
	values.Add("Success", "true")
	values.Add("Key", "some/file.txt")
	queue.Enqueue(callbackTestConfig(50*time.Millisecond), server.URL, values)

	require.True(t, queue.waitEmpty(5*time.Second), "queue should drain")

	form := <-received
	assert.Equal(t, "true", form.Get("Success"))
	assert.Equal(t, "some/file.txt", form.Get("Key"))
	assert.Equal(t, int32(1), requests.Load(), "should deliver in one attempt")
	assert.Equal(t, retriesBefore, globalMetrics.TotalCallbackRetries.Load())
	assert.Equal(t, permanentBefore, globalMetrics.TotalCallbackPermanentFailures.Load())
}

func TestCallbackQueue_RetryThenSucceed(t *testing.T) {
	retriesBefore := globalMetrics.TotalCallbackRetries.Load()
	failuresBefore := globalMetrics.TotalCallbackFailures.Load()
	permanentBefore := globalMetrics.TotalCallbackPermanentFailures.Load()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// non-2xx responses must be treated as failed deliveries
		if requests.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	queue := NewCallbackQueue()
	config := callbackTestConfig(10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	queue.Enqueue(config, server.URL, url.Values{"Success": {"true"}})

	require.True(t, queue.waitEmpty(5*time.Second), "queue should drain")

	assert.Equal(t, int32(3), requests.Load(), "two failures then a success")
	assert.Equal(t, retriesBefore+2, globalMetrics.TotalCallbackRetries.Load())
	assert.Equal(t, failuresBefore+2, globalMetrics.TotalCallbackFailures.Load())
	assert.Equal(t, permanentBefore, globalMetrics.TotalCallbackPermanentFailures.Load())
}

func TestCallbackQueue_PermanentFailure(t *testing.T) {
	retriesBefore := globalMetrics.TotalCallbackRetries.Load()
	failuresBefore := globalMetrics.TotalCallbackFailures.Load()
	permanentBefore := globalMetrics.TotalCallbackPermanentFailures.Load()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	queue := NewCallbackQueue()
	config := callbackTestConfig(5*time.Millisecond, 5*time.Millisecond)
	queue.Enqueue(config, server.URL, url.Values{"Success": {"true"}})

	require.True(t, queue.waitEmpty(5*time.Second), "queue should drain")

	assert.Equal(t, int32(3), requests.Load(), "initial attempt plus two retries")
	assert.Equal(t, retriesBefore+2, globalMetrics.TotalCallbackRetries.Load())
	assert.Equal(t, failuresBefore+3, globalMetrics.TotalCallbackFailures.Load())
	assert.Equal(t, permanentBefore+1, globalMetrics.TotalCallbackPermanentFailures.Load())
}

func TestCallbackQueue_EmptyDelaysSingleAttempt(t *testing.T) {
	retriesBefore := globalMetrics.TotalCallbackRetries.Load()
	permanentBefore := globalMetrics.TotalCallbackPermanentFailures.Load()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	queue := NewCallbackQueue()
	queue.Enqueue(callbackTestConfig(), server.URL, url.Values{"Success": {"true"}})

	require.True(t, queue.waitEmpty(5*time.Second), "queue should drain")

	assert.Equal(t, int32(1), requests.Load(), "no retries with an empty schedule")
	assert.Equal(t, retriesBefore, globalMetrics.TotalCallbackRetries.Load())
	assert.Equal(t, permanentBefore+1, globalMetrics.TotalCallbackPermanentFailures.Load())
}

func TestCallbackQueue_GetPendingRedactsURL(t *testing.T) {
	// a server that is already closed produces transport errors, whose text
	// embeds the full request URL
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	server.Close()

	queue := NewCallbackQueue()
	callbackURL := server.URL + "/notify?token=secret123"
	queue.Enqueue(callbackTestConfig(500*time.Millisecond), callbackURL, url.Values{"Success": {"true"}})

	// wait for the first attempt to fail and be recorded
	var pending []PendingCallbackInfo
	require.Eventually(t, func() bool {
		pending = queue.GetPending()
		return len(pending) == 1 && pending[0].Attempts >= 1
	}, 5*time.Second, 5*time.Millisecond, "first failed attempt should be visible")

	assert.Equal(t, server.URL+"/notify", pending[0].URL, "query string should be stripped")
	assert.NotEmpty(t, pending[0].LastError)
	assert.NotContains(t, pending[0].LastError, "secret123")
	assert.False(t, pending[0].NextAttempt.IsZero())
	assert.False(t, strings.Contains(pending[0].URL, "token"), "no query params in status output")

	require.True(t, queue.waitEmpty(5*time.Second), "queue should drain")
}
