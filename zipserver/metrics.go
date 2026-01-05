package zipserver

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
)

var globalMetrics = &MetricsCounter{}

type MetricsCounter struct {
	TotalRequests         atomic.Int64 `metric:"zipserver_requests_total"`
	TotalErrors           atomic.Int64 `metric:"zipserver_errors_total"`
	TotalCallbackFailures atomic.Int64 `metric:"zipserver_callback_failures_total"`
	TotalExtractedFiles   atomic.Int64 `metric:"zipserver_extracted_files_total"`
	TotalCopiedFiles      atomic.Int64 `metric:"zipserver_copied_files_total"`
	TotalDeletedFiles     atomic.Int64 `metric:"zipserver_deleted_files_total"`
	TotalSlurpedFiles     atomic.Int64 `metric:"zipserver_slurped_files_total"`
	TotalBytesDownloaded  atomic.Int64 `metric:"zipserver_downloaded_bytes_total"`
	TotalBytesUploaded    atomic.Int64 `metric:"zipserver_uploaded_bytes_total"`
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

	userCPU, systemCPU := getCPUTime()
	metrics.WriteString(fmt.Sprintf("zipserver_cpu_user_seconds_total{host=\"%s\"} %f\n", hostname, userCPU))
	metrics.WriteString(fmt.Sprintf("zipserver_cpu_system_seconds_total{host=\"%s\"} %f\n", hostname, systemCPU))

	return metrics.String()
}

// getCPUTime returns the cumulative user and system CPU time for this process
func getCPUTime() (userSeconds, systemSeconds float64) {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0, 0
	}
	userSeconds = float64(rusage.Utime.Sec) + float64(rusage.Utime.Usec)/1e6
	systemSeconds = float64(rusage.Stime.Sec) + float64(rusage.Stime.Usec)/1e6
	return
}

// wrap a reader to count bytes read into the counter
func metricsReader(reader io.Reader, counter *atomic.Int64) readerClosure {
	return func(p []byte) (int, error) {
		bytesRead, err := reader.Read(p)
		counter.Add(int64(bytesRead))
		return bytesRead, err
	}
}

type metricsReadCloser struct {
	io.ReadCloser
	counter *atomic.Int64
}

// Read reads data from the underlying io.ReadCloser, tracking the bytes read
func (mrc metricsReadCloser) Read(p []byte) (int, error) {
	bytesRead, err := mrc.ReadCloser.Read(p)
	mrc.counter.Add(int64(bytesRead))
	return bytesRead, err
}

// Close closes the underlying io.ReadCloser and returns the result
func (mrc metricsReadCloser) Close() error {
	return mrc.ReadCloser.Close()
}

// http endpoint to render the global metrics
func metricsHandler(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(globalMetrics.RenderMetrics(globalConfig)))
	return nil
}
