package zipserver

import (
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
)

var globalMetrics = &MetricsCounter{}

type MetricsCounter struct {
	TotalRequests       atomic.Int64 `metric:"zipserver_requests_total""`
	TotalErrors         atomic.Int64 `metric:"zipserver_errors_total""`
	TotalExtractedFiles atomic.Int64 `metric:"zipserver_extracted_files_total"`
	TotalCopiedFiles    atomic.Int64 `metric:"zipserver_copied_files_total"`
	// TODO: bytes downloaded, bytes uploaded
}

// render the metrics in a prometheus compatible format
func (m *MetricsCounter) RenderMetrics(config *Config) string {
	var metrics strings.Builder

	valueOfMetrics := reflect.ValueOf(m).Elem()

	hostname := config.MetricsHost
	if hostname == "" {
		hostname, _ = os.Hostname()
	}

	for i := 0; i < valueOfMetrics.NumField(); i++ {
		metricTag := valueOfMetrics.Type().Field(i).Tag.Get("metric")
		if metricTag == "" {
			continue
		}
		fieldValue := valueOfMetrics.Field(i).Addr().Interface().(*atomic.Int64).Load()

		metrics.WriteString(fmt.Sprintf("%s{host=\"%s\"} %v\n", metricTag, hostname, fieldValue))

	}

	return metrics.String()
}

// render the global metrics
func metricsHandler(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(globalMetrics.RenderMetrics(globalConfig)))
	return nil
}
