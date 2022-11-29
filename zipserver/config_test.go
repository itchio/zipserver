package zipserver

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Config(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "zipserver-config")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpFile.Name())

	writeConfigBytes := func(bytes []byte) {
		t.Helper()

		_, err := tmpFile.Seek(0, os.SEEK_SET)
		require.NoError(t, err)

		_, err = tmpFile.Write(bytes)
		require.NoError(t, err)
	}

	writeConfig := func(c *Config) {
		t.Helper()

		bytes, err := json.Marshal(c)
		require.NoError(t, err)
		writeConfigBytes(bytes)
	}

	assertConfigError := func() {
		_, err = LoadConfig(tmpFile.Name())
		assert.Error(t, err)
	}

	writeConfigBytes([]byte("{"))
	assertConfigError()

	writeConfig(&Config{})
	assertConfigError()

	writeConfig(&Config{
		PrivateKeyPath: "/foo/bar.pem",
	})
	assertConfigError()

	writeConfig(&Config{
		PrivateKeyPath: "/foo/bar.pem",
		ClientEmail:    "foobar@example.org",
	})
	assertConfigError()

	writeConfig(&Config{
		PrivateKeyPath: "/foo/bar.pem",
		ClientEmail:    "foobar@example.org",
		Bucket:         "chicken",
	})
	assertConfigError()

	writeConfig(&Config{
		PrivateKeyPath: "/foo/bar.pem",
		ClientEmail:    "foobar@example.org",
		Bucket:         "chicken",
		ExtractPrefix:  "saca",
		MaxFileSize:    92,
	})

	c, err := LoadConfig(tmpFile.Name())
	require.NoError(t, err)

	assert.EqualValues(t, "/foo/bar.pem", c.PrivateKeyPath)
	assert.EqualValues(t, 92, c.MaxFileSize)
	assert.Equal(t, 5*time.Minute, time.Duration(c.JobTimeout))
	assert.Equal(t, 1*time.Minute, time.Duration(c.FileGetTimeout))
	assert.Equal(t, 1*time.Minute, time.Duration(c.FilePutTimeout))
	assert.Equal(t, 5*time.Second, time.Duration(c.AsyncNotificationTimeout))

	assert.True(t, c.String() != "")
}
