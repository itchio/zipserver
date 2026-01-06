package zipserver

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// createTestZip creates an in-memory zip file with the given entries
func createTestZip(t *testing.T, entries map[string][]byte) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	for name, data := range entries {
		writer, err := zw.CreateHeader(&zip.FileHeader{
			Name:               name,
			UncompressedSize64: uint64(len(data)),
		})
		require.NoError(t, err)
		_, err = writer.Write(data)
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestExtractHandler_AsyncCallback(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		targetName := "mem-target-extract-async"
		defer ClearNamedMemStorage(targetName)

		config.ExtractionThreads = 4
		config.AsyncNotificationTimeout = Duration(10 * time.Second)
		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "target-bucket",
		}}

		oldConfig := globalConfig
		globalConfig = config
		defer func() { globalConfig = oldConfig }()

		ctx := context.Background()

		zipData := createTestZip(t, map[string][]byte{
			"index.html": []byte("<html><body>Hello</body></html>"),
			"other.txt":  []byte("other content"),
		})

		// Upload zip to primary storage (GCS)
		_, err := storage.PutFile(ctx, config.Bucket, "zipserver_test/async_test.zip", bytes.NewReader(zipData), PutOptions{
			ContentType: "application/zip",
		})
		require.NoError(t, err)

		// Set up callback server to capture the async notification
		callbackReceived := make(chan url.Values, 1)
		callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.NoError(t, r.ParseForm())
			callbackReceived <- r.Form
			w.WriteHeader(http.StatusOK)
		}))
		defer callbackServer.Close()

		// Make request to extract handler with async callback
		form := url.Values{}
		form.Set("key", "zipserver_test/async_test.zip")
		form.Set("prefix", "zipserver_test/async_extracted")
		form.Set("target", targetName)
		form.Set("async", callbackServer.URL)
		form.Set("html_footer", "<script>injected</script>")

		req := httptest.NewRequest(http.MethodPost, "/extract", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()

		err = extractHandler(w, req)
		require.NoError(t, err)

		// Verify immediate response indicates async processing
		var immediateResp struct {
			Processing bool
			Async      bool
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &immediateResp))
		assert.True(t, immediateResp.Processing)
		assert.True(t, immediateResp.Async)

		// Wait for callback
		select {
		case callbackData := <-callbackReceived:
			assert.Equal(t, "true", callbackData.Get("Success"))

			// Find the index.html entry and verify Injected is set
			foundIndex := false
			foundOther := false
			for i := 1; i <= 10; i++ {
				key := callbackData.Get(fmt.Sprintf("ExtractedFiles[%d][Key])", i))
				if key == "" {
					break
				}
				injected := callbackData.Get(fmt.Sprintf("ExtractedFiles[%d][Injected])", i))

				if strings.HasSuffix(key, "/index.html") {
					foundIndex = true
					assert.Equal(t, "true", injected, "index.html should have Injected=true")
				} else if strings.HasSuffix(key, "/other.txt") {
					foundOther = true
					assert.Empty(t, injected, "other.txt should not have Injected field")
				}
			}
			assert.True(t, foundIndex, "should have found index.html in callback")
			assert.True(t, foundOther, "should have found other.txt in callback")

		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for async callback")
		}
	})
}

func TestExtractHandler_SyncWithInjected(t *testing.T) {
	withGoogleCloudStorage(t, func(storage Storage, config *Config) {
		targetName := "mem-target-extract-sync"
		defer ClearNamedMemStorage(targetName)

		config.ExtractionThreads = 4
		config.StorageTargets = []StorageConfig{{
			Name:   targetName,
			Type:   Mem,
			Bucket: "target-bucket",
		}}

		oldConfig := globalConfig
		globalConfig = config
		defer func() { globalConfig = oldConfig }()

		ctx := context.Background()

		zipData := createTestZip(t, map[string][]byte{
			"index.html": []byte("<html><body>Hello</body></html>"),
			"other.txt":  []byte("other content"),
		})

		// Upload zip to primary storage (GCS)
		_, err := storage.PutFile(ctx, config.Bucket, "zipserver_test/sync_test.zip", bytes.NewReader(zipData), PutOptions{
			ContentType: "application/zip",
		})
		require.NoError(t, err)

		// Make sync request (no async parameter)
		form := url.Values{}
		form.Set("key", "zipserver_test/sync_test.zip")
		form.Set("prefix", "zipserver_test/sync_extracted")
		form.Set("target", targetName)
		form.Set("html_footer", "<script>injected</script>")

		req := httptest.NewRequest(http.MethodPost, "/extract", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()

		err = extractHandler(w, req)
		require.NoError(t, err)

		// Parse sync response
		var resp struct {
			Success        bool
			ExtractedFiles []ExtractedFile
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.True(t, resp.Success)
		assert.Len(t, resp.ExtractedFiles, 2)

		// Verify Injected field
		for _, f := range resp.ExtractedFiles {
			if strings.HasSuffix(f.Key, "/index.html") {
				assert.True(t, f.Injected, "index.html should have Injected=true")
			} else {
				assert.False(t, f.Injected, "other.txt should have Injected=false")
			}
		}
	})
}
