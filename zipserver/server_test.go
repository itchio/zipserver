package zipserver

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
