package zip_server

import (
	"fmt"
	"io"
	"log"
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
func limitedReader(reader io.Reader, maxBytes int, totalBytes *int) readerClosure {
	remainingBytes := maxBytes
	return func(p []byte) (int, error) {
		bytesRead, err := reader.Read(p)
		remainingBytes -= bytesRead

		*totalBytes += bytesRead

		if remainingBytes < 0 {
			return bytesRead, fmt.Errorf("File too large (max %d bytes)", maxBytes)
		}

		return bytesRead, err
	}
}
