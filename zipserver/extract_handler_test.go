package zipserver

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func Test_LimitsWithFilter(t *testing.T) {
	values, err := url.ParseQuery("filter=*.png")
	assert.NoError(t, err)

	el := loadLimits(values, &defaultConfig)
	assert.EqualValues(t, "*.png", el.IncludeGlob)

	// empty filter should not be set
	values, err = url.ParseQuery("")
	assert.NoError(t, err)

	el = loadLimits(values, &defaultConfig)
	assert.EqualValues(t, "", el.IncludeGlob)
}
