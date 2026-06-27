package zipserver

import (
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

// accessLogger writes one line per HTTP request in the Combined Log Format.
// It is nil when access logging is disabled.
var accessLogger *log.Logger

// initAccessLog opens the access log at path and configures accessLogger. An
// empty path disables access logging; the special value "-" writes to stdout.
func initAccessLog(path string) error {
	accessLogger = nil

	if path == "" {
		return nil
	}

	var out *os.File
	if path == "-" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		out = f
	}

	// We format the whole line ourselves (including the timestamp), so the
	// logger adds no prefix or flags of its own.
	accessLogger = log.New(out, "", 0)
	return nil
}

// responseRecorder wraps http.ResponseWriter to capture the status code and the
// number of bytes written to the body, for access logging.
type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytes += n
	return n, err
}

func (r *responseRecorder) Status() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// logAccess writes a single Combined Log Format entry for a finished request.
func logAccess(r *http.Request, status, bytes int) {
	if accessLogger == nil {
		return
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	referer := r.Referer()
	if referer == "" {
		referer = "-"
	}
	userAgent := r.UserAgent()
	if userAgent == "" {
		userAgent = "-"
	}

	accessLogger.Printf("%s - - [%s] \"%s %s %s\" %d %d %q %q",
		host,
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		r.Method,
		r.RequestURI,
		r.Proto,
		status,
		bytes,
		referer,
		userAgent,
	)
}
