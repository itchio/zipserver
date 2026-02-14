package zipserver

import (
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Config(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "zipserver-config")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpFile.Name())

	writeConfigBytes := func(bytes []byte) {
		_, err := tmpFile.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tmpFile.Write(bytes)
		if err != nil {
			t.Fatal(err)
		}
	}

	writeConfig := func(c *Config) {
		bytes, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
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
	assert.NoError(t, err)

	assert.EqualValues(t, "/foo/bar.pem", c.PrivateKeyPath)
	assert.EqualValues(t, 92, c.MaxFileSize)
	assert.Equal(t, 5*time.Minute, time.Duration(c.JobTimeout))
	assert.Equal(t, 1*time.Minute, time.Duration(c.FileGetTimeout))
	assert.Equal(t, 1*time.Minute, time.Duration(c.FilePutTimeout))
	assert.Equal(t, 5*time.Second, time.Duration(c.AsyncNotificationTimeout))
	assert.Equal(t, defaultPreCompressMaxConcurrent, c.PreCompressMaxConcurrent)

	assert.True(t, c.String() != "")
}
