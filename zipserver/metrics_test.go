package zipserver

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Metrics(t *testing.T) {
	metrics := &MetricsCounter{}

	// Test initial values
	assert.Equal(t, int64(0), metrics.TotalRequests.Load())
	assert.Equal(t, int64(0), metrics.TotalExtractedFiles.Load())

	metrics.TotalRequests.Add(1)
	assert.Equal(t, int64(1), metrics.TotalRequests.Load())

	metrics.TotalExtractedFiles.Add(1)
	assert.Equal(t, int64(1), metrics.TotalExtractedFiles.Load())

	// create a temp byte buffer wrapped in metricsReader to test updating BytesDownloaded
	buf := bytes.NewBufferString("testing")
	reader := metricsReader(buf, &metrics.TotalBytesDownloaded)

	// Read from the reader to trigger the metrics update
	_, _ = ioutil.ReadAll(reader)

	config := &Config{
		MetricsHost: "localhost",
	}

	expectedMetrics := `zipserver_requests_total{host="localhost"} 1
zipserver_errors_total{host="localhost"} 0
zipserver_extracted_files_total{host="localhost"} 1
zipserver_copied_files_total{host="localhost"} 0
zipserver_downloaded_bytes_total{host="localhost"} 7
zipserver_uploaded_bytes_total{host="localhost"} 0
`
	assert.Equal(t, expectedMetrics, metrics.RenderMetrics(config))
}
