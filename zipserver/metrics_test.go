package zipserver

import (
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

	// Test RenderMetrics
	expectedMetrics := `zipserver_requests_total 1
zipserver_errors_total 0
zipserver_extracted_files_total 1
zipserver_copied_files_total 0
`

	assert.Equal(t, expectedMetrics, metrics.RenderMetrics())
}
