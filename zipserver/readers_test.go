package zipserver

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_annotatedReader(t *testing.T) {
	s := "Hello, world"

	sr := bytes.NewReader([]byte(s))
	ar := annotatedReader(sr)

	buf := make([]byte, 4)
	var totalBytes int
	for {
		n, err := ar.Read(buf)
		totalBytes += n
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
	}
	assert.EqualValues(t, len(s), totalBytes)
}

func Test_limitedReader(t *testing.T) {
	s := "Hello, world"

	sr := bytes.NewReader([]byte(s))
	var totalBytes uint64
	lr := limitedReader(sr, 128, &totalBytes)

	result, err := io.ReadAll(lr)
	assert.NoError(t, err)
	assert.EqualValues(t, s, string(result))
	assert.EqualValues(t, len(s), totalBytes)

	sr.Seek(0, io.SeekStart)
	lr = limitedReader(sr, 5, &totalBytes)
	_, err = io.ReadAll(lr)
	assert.Error(t, err)
}

func Test_limitedReaderWithCancel(t *testing.T) {
	s := "Hello, world"

	sr := bytes.NewReader([]byte(s))
	var totalBytes uint64
	called := false
	lr := limitedReaderWithCancel(sr, 5, &totalBytes, func() {
		called = true
	})

	_, err := io.ReadAll(lr)
	assert.Error(t, err)
	assert.True(t, called)
}

func Test_appendReader(t *testing.T) {
	t.Run("appends data to reader", func(t *testing.T) {
		original := bytes.NewReader([]byte("Hello"))
		appendStr := " World"
		reader := newAppendReader(original, appendStr)

		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, "Hello World", string(data))
	})

	t.Run("handles empty original", func(t *testing.T) {
		original := bytes.NewReader([]byte(""))
		appendStr := "Only Append"
		reader := newAppendReader(original, appendStr)

		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, "Only Append", string(data))
	})

	t.Run("handles empty append", func(t *testing.T) {
		original := bytes.NewReader([]byte("Original"))
		reader := newAppendReader(original, "")

		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, "Original", string(data))
	})

	t.Run("works with small buffer reads", func(t *testing.T) {
		original := bytes.NewReader([]byte("ABC"))
		appendStr := "DEF"
		reader := newAppendReader(original, appendStr)

		buf := make([]byte, 2)
		var result []byte
		for {
			n, err := reader.Read(buf)
			result = append(result, buf[:n]...)
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)
		}
		assert.Equal(t, "ABCDEF", string(result))
	})

	t.Run("handles single byte reads", func(t *testing.T) {
		original := bytes.NewReader([]byte("AB"))
		appendStr := "CD"
		reader := newAppendReader(original, appendStr)

		buf := make([]byte, 1)
		var result []byte
		for {
			n, err := reader.Read(buf)
			result = append(result, buf[:n]...)
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)
		}
		assert.Equal(t, "ABCD", string(result))
	})
}
