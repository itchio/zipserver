package zipserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupServerTest saves the package-level globalConfig and accessLogger,
// restores them on cleanup, and disables access logging for the test.
func setupServerTest(t *testing.T) {
	t.Helper()
	previousConfig := globalConfig
	previousAccessLogger := accessLogger
	t.Cleanup(func() {
		globalConfig = previousConfig
		accessLogger = previousAccessLogger
	})
	accessLogger = nil
}

func TestWrapErrorsBearerAuth(t *testing.T) {
	setupServerTest(t)

	tests := []struct {
		name           string
		token          string
		authHeader     string
		wantStatus     int
		wantCalled     bool
		wantAuthHeader string
	}{
		{
			name:       "disabled without token",
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
		{
			name:           "missing header",
			token:          "secret-token",
			wantStatus:     http.StatusUnauthorized,
			wantAuthHeader: "Bearer",
		},
		{
			name:           "malformed header",
			token:          "secret-token",
			authHeader:     "Bearer",
			wantStatus:     http.StatusUnauthorized,
			wantAuthHeader: "Bearer",
		},
		{
			name:           "wrong token",
			token:          "secret-token",
			authHeader:     "Bearer wrong-token",
			wantStatus:     http.StatusUnauthorized,
			wantAuthHeader: "Bearer",
		},
		{
			name:       "correct token",
			token:      "secret-token",
			authHeader: "Bearer secret-token",
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
		{
			name:       "case-insensitive bearer scheme",
			token:      "secret-token",
			authHeader: "bearer secret-token",
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			globalConfig = &Config{AuthBearerToken: tt.token}
			called := false
			handler := wrapErrors(func(w http.ResponseWriter, r *http.Request) error {
				called = true
				w.WriteHeader(http.StatusOK)
				return nil
			})

			req := httptest.NewRequest(http.MethodGet, "/status", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantCalled, called)
			assert.Equal(t, tt.wantAuthHeader, w.Header().Get("WWW-Authenticate"))
		})
	}
}

func TestWrapErrorsHandlerErrorStillReturns500(t *testing.T) {
	setupServerTest(t)
	globalConfig = &Config{AuthBearerToken: "secret-token"}

	handler := wrapErrors(func(w http.ResponseWriter, r *http.Request) error {
		return errors.New("boom")
	})

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "boom")
}

func TestDrainServerWaitsForJobs(t *testing.T) {
	oldConfig := globalConfig
	globalConfig = &Config{ShutdownTimeout: Duration(5 * time.Second)}
	defer func() { globalConfig = oldConfig }()

	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}

	release := make(chan struct{})
	finished := make(chan struct{})
	startJob(func() {
		<-release
		close(finished)
	})

	drained := make(chan error, 1)
	go func() { drained <- drainServer(server, syscall.SIGTERM) }()

	select {
	case <-drained:
		t.Fatal("drain returned before job finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-finished

	select {
	case err := <-drained:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return after job finished")
	}
}

func TestDrainServerDeadline(t *testing.T) {
	oldConfig := globalConfig
	globalConfig = &Config{ShutdownTimeout: Duration(100 * time.Millisecond)}
	defer func() { globalConfig = oldConfig }()

	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}

	release := make(chan struct{})
	startJob(func() { <-release })
	defer close(release)

	start := time.Now()
	require.NoError(t, drainServer(server, syscall.SIGTERM))
	assert.Less(t, time.Since(start), 2*time.Second)
}

// An async copy (callback set, no sync_timeout) must hold the drain open
// until it finishes.
func TestDrainServerWaitsForAsyncCopy(t *testing.T) {
	oldConfig := globalConfig
	globalConfig = copyHandlerTestConfig()
	globalConfig.CopyMaxConcurrent = 1
	globalConfig.ShutdownTimeout = Duration(5 * time.Second)
	defer func() { globalConfig = oldConfig }()

	// Hold the only copy slot so the job blocks before touching storage.
	limiter := globalConfig.getCopyLimiter()
	require.NoError(t, limiter.acquire(context.Background()))
	released := false
	defer func() {
		if !released {
			limiter.release()
		}
	}()

	form := url.Values{}
	form.Set("key", "some/file.txt")
	form.Set("target", "test-target")
	form.Set("callback", "-")

	req := httptest.NewRequest(http.MethodPost, "/copy", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	require.NoError(t, copyHandler(w, req))
	assert.Contains(t, w.Body.String(), `"Async":true`)

	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}
	drained := make(chan error, 1)
	go func() { drained <- drainServer(server, syscall.SIGTERM) }()

	select {
	case <-drained:
		t.Fatal("drain returned while async copy was still blocked")
	case <-time.After(100 * time.Millisecond):
	}

	limiter.release()
	released = true

	select {
	case err := <-drained:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return after async copy finished")
	}
}
