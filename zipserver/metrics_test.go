package zipserver

import (
	"bytes"
	"io"
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
	_, _ = io.ReadAll(reader)

	config := &Config{
		MetricsHost: "localhost",
	}

	rendered := metrics.RenderMetrics(config)

	// Check counter metrics (exact values)
	assert.Contains(t, rendered, `zipserver_requests_total{host="localhost"} 1`)
	assert.Contains(t, rendered, `zipserver_errors_total{host="localhost"} 0`)
	assert.Contains(t, rendered, `zipserver_extracted_files_total{host="localhost"} 1`)
	assert.Contains(t, rendered, `zipserver_copied_files_total{host="localhost"} 0`)
	assert.Contains(t, rendered, `zipserver_deleted_files_total{host="localhost"} 0`)
	assert.Contains(t, rendered, `zipserver_downloaded_bytes_total{host="localhost"} 7`)
	assert.Contains(t, rendered, `zipserver_uploaded_bytes_total{host="localhost"} 0`)

	// Check CPU metrics are present (values vary per run)
	assert.Contains(t, rendered, `zipserver_cpu_user_seconds_total{host="localhost"}`)
	assert.Contains(t, rendered, `zipserver_cpu_system_seconds_total{host="localhost"}`)
}
