package zipserver

import (
	"errors"
	"fmt"
	"io"
	"log"
	"time"
)

var ErrLimitExceeded = errors.New("limit exceeded")

type LimitExceededError struct {
	MaxBytes uint64
}

func (e *LimitExceededError) Error() string {
	return fmt.Sprintf("file too large (max %d bytes)", e.MaxBytes)
}

func (e *LimitExceededError) Unwrap() error {
	return ErrLimitExceeded
}

type readerClosure func(p []byte) (int, error)

func (fn readerClosure) Read(p []byte) (int, error) {
	return fn(p)
}

// debug reader
func annotatedReader(reader io.Reader) readerClosure {
	return func(p []byte) (int, error) {
		bytesRead, err := reader.Read(p)
		log.Printf("Read %d bytes", bytesRead)
		return bytesRead, err
	}
}

// wraps a reader to fail if it reads more than max of maxBytes, also tracks
// the total amount of bytes read
func limitedReader(reader io.Reader, maxBytes uint64, totalBytes *uint64) readerClosure {
	return func(p []byte) (int, error) {
		bytesRead, err := reader.Read(p)
		*totalBytes += uint64(bytesRead)

		if *totalBytes > maxBytes {
			return bytesRead, &LimitExceededError{MaxBytes: maxBytes}
		}

		return bytesRead, err
	}
}

// limitedReaderWithCancel behaves like limitedReader but invokes onLimit when the
// limit is exceeded. Use this to terminate upstream connections early.
func limitedReaderWithCancel(reader io.Reader, maxBytes uint64, totalBytes *uint64, onLimit func()) readerClosure {
	return func(p []byte) (int, error) {
		bytesRead, err := reader.Read(p)
		*totalBytes += uint64(bytesRead)

		if *totalBytes > maxBytes {
			if onLimit != nil {
				onLimit()
			}
			return bytesRead, &LimitExceededError{MaxBytes: maxBytes}
		}

		return bytesRead, err
	}
}

type measuredReader struct {
	reader    io.Reader     // The underlying reader
	BytesRead int64         // Total bytes read
	StartTime time.Time     // Time when reading started
	Duration  time.Duration // Duration of the read operation
}

func newMeasuredReader(r io.Reader) *measuredReader {
	return &measuredReader{
		reader:    r,
		StartTime: time.Now(),
	}
}

// Read reads data from the underlying io.Reader, tracking the bytes read and duration
func (mr *measuredReader) Read(p []byte) (int, error) {
	n, err := mr.reader.Read(p)
	mr.BytesRead += int64(n)
	mr.Duration = time.Since(mr.StartTime)

	return n, err
}

// TransferSpeed returns the average transfer speed in bytes per second
func (mr *measuredReader) TransferSpeed() float64 {
	if mr.Duration.Seconds() == 0 {
		return 0
	}
	return float64(mr.BytesRead) / mr.Duration.Seconds()
}

// appendReader wraps a reader and appends additional bytes at the end
type appendReader struct {
	reader     io.Reader
	appendData []byte
	appendPos  int
	readerDone bool
}

func newAppendReader(reader io.Reader, appendData string) *appendReader {
	return &appendReader{
		reader:     reader,
		appendData: []byte(appendData),
	}
}

func (r *appendReader) Read(p []byte) (int, error) {
	if !r.readerDone {
		n, err := r.reader.Read(p)
		if err == io.EOF {
			r.readerDone = true
			// Don't return EOF yet if we have data to append
			if n > 0 {
				return n, nil
			}
			// Fall through to append logic
		} else {
			return n, err
		}
	}

	// Append phase: copy remaining appendData
	if r.appendPos >= len(r.appendData) {
		return 0, io.EOF
	}

	n := copy(p, r.appendData[r.appendPos:])
	r.appendPos += n

	if r.appendPos >= len(r.appendData) {
		return n, io.EOF
	}
	return n, nil
}
