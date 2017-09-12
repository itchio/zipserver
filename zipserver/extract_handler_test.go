package zipserver

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err)

	el = loadLimits(values, &defaultConfig)
	assert.EqualValues(t, el.MaxFileSize, customMaxFileSize)
}
