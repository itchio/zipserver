package zipserver

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Limits(t *testing.T) {
	var values url.Values

	el, err := loadLimits(values, &defaultConfig)
	assert.NoError(t, err)
	assert.EqualValues(t, el.MaxFileSize, defaultConfig.MaxFileSize)

	const customMaxFileSize = 9428
	values, err = url.ParseQuery(fmt.Sprintf("maxFileSize=%d", customMaxFileSize))
	assert.NoError(t, err)

	el, err = loadLimits(values, &defaultConfig)
	assert.NoError(t, err)
	assert.EqualValues(t, el.MaxFileSize, customMaxFileSize)
}

func Test_LimitsWithFilter(t *testing.T) {
	values, err := url.ParseQuery("filter=*.png")
	assert.NoError(t, err)

	el, err := loadLimits(values, &defaultConfig)
	assert.NoError(t, err)
	assert.EqualValues(t, "*.png", el.IncludeGlob)

	// empty filter should not be set
	values, err = url.ParseQuery("")
	assert.NoError(t, err)

	el, err = loadLimits(values, &defaultConfig)
	assert.NoError(t, err)
	assert.EqualValues(t, "", el.IncludeGlob)
}

func Test_LimitsWithOnlyFiles(t *testing.T) {
	values, err := url.ParseQuery("only_files[]=file1.txt&only_files[]=dir/file2.txt")
	assert.NoError(t, err)

	el, err := loadLimits(values, &defaultConfig)
	assert.NoError(t, err)
	assert.EqualValues(t, []string{"file1.txt", "dir/file2.txt"}, el.OnlyFiles)
}

func Test_LimitsOnlyFilesAndFilterMutuallyExclusive(t *testing.T) {
	values, err := url.ParseQuery("filter=*.png&only_files[]=file1.txt")
	assert.NoError(t, err)

	_, err = loadLimits(values, &defaultConfig)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be used together")
}

func Test_LimitsWithHtmlFooter(t *testing.T) {
	footer := "<script src=\"analytics.js\"></script>"
	values, err := url.ParseQuery("html_footer=" + url.QueryEscape(footer))
	assert.NoError(t, err)

	el, err := loadLimits(values, &defaultConfig)
	assert.NoError(t, err)
	assert.EqualValues(t, footer, el.HtmlFooter)

	// empty footer should not be set
	values, err = url.ParseQuery("")
	assert.NoError(t, err)

	el, err = loadLimits(values, &defaultConfig)
	assert.NoError(t, err)
	assert.EqualValues(t, "", el.HtmlFooter)
}
