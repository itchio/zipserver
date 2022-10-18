package zipserver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Locks(t *testing.T) {
	// not the best test, more like a basic sanity check
	hasLock := tryLockKey("foo")

	assert.True(t, hasLock, "should acquire foo")

	hasLock = tryLockKey("foo")
	assert.False(t, hasLock, "should not acquire foo again")

	hasLock = tryLockKey("bar")
	assert.True(t, hasLock, "should acquire bar")

	releaseKey("foo")
	hasLock = tryLockKey("bar")
	assert.False(t, hasLock, "should not acquire bar again")

	hasLock = tryLockKey("foo")
	assert.True(t, hasLock, "should acquire foo again")
}

func Test_Limits(t *testing.T) {
	var values url.Values

	el := loadLimits(values, &defaultConfig)
	assert.EqualValues(t, el.MaxFileSize, defaultConfig.MaxFileSize)

	const customMaxFileSize = 9428
	values, err := url.ParseQuery(fmt.Sprintf("maxFileSize=%d", customMaxFileSize))
	require.NoError(t, err)

	el = loadLimits(values, &defaultConfig)
	assert.EqualValues(t, el.MaxFileSize, customMaxFileSize)
}

func TestExtractHandlerMissingQueryParam(t *testing.T) {
	testServer := httptest.NewServer(errorHandler(extractHandler))
	defer testServer.Close()
	res, err := http.Get(testServer.URL + "/extract")
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
}
