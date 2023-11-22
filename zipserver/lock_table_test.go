package zipserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_LockTable(t *testing.T) {
	// Create a new lock table for the test
	lt := NewLockTable()

	// not the best test, more like a basic sanity check
	hasLock := lt.tryLockKey("foo")

	assert.True(t, hasLock, "should acquire foo")

	hasLock = lt.tryLockKey("foo")
	assert.False(t, hasLock, "should not acquire foo again")

	hasLock = lt.tryLockKey("bar")
	assert.True(t, hasLock, "should acquire bar")

	lt.releaseKey("foo")
	hasLock = lt.tryLockKey("bar")
	assert.False(t, hasLock, "should not acquire bar again")

	hasLock = lt.tryLockKey("foo")
	assert.True(t, hasLock, "should acquire foo again")
}
