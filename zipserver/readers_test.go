package zipserver

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, err)
	}
	assert.EqualValues(t, len(s), totalBytes)
}

func Test_limitedReader(t *testing.T) {
	s := "Hello, world"

	sr := bytes.NewReader([]byte(s))
	var totalBytes uint64
	lr := limitedReader(sr, 128, &totalBytes)

	result, err := io.ReadAll(lr)
	require.NoError(t, err)
	assert.EqualValues(t, s, string(result))
	assert.EqualValues(t, len(s), totalBytes)

	sr.Seek(0, os.SEEK_SET)
	lr = limitedReader(sr, 5, &totalBytes)
	result, err = io.ReadAll(lr)
	assert.Error(t, err)
}
