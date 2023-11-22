package zipserver

import (
	"fmt"
	"io"
	"log"
	"time"
)

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
			return bytesRead, fmt.Errorf("File too large (max %d bytes)", maxBytes)
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
