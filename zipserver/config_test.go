package zipserver

import (
	"encoding/json"
	"io"
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
	assert.Equal(t, uint64(1024*1024), c.MaxPeekBytes)

	assert.True(t, c.String() != "")
}

func Test_ConfigCompressionFields(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "zipserver-config-compression")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	configJSON := []byte(`{
		"PrivateKeyPath": "/foo/bar.pem",
		"ClientEmail": "foobar@example.org",
		"Bucket": "primary",
		"ExtractPrefix": "extract",
		"StorageTargets": [
			{
				"Name": "compressed-target",
				"Type": "Mem",
				"Bucket": "target",
				"ExtractPrefix": "target-extract",
				"CompressEnabled": true,
				"CompressExtensions": [".js"],
				"CompressMinSize": 128,
				"CompressMaxConcurrent": 3,
				"CompressLevel": 4
			}
		]
	}`)
	require.NoError(t, os.WriteFile(tmpFile.Name(), configJSON, 0o600))

	c, err := LoadConfig(tmpFile.Name())
	require.NoError(t, err)

	require.Len(t, c.StorageTargets, 1)
	target := c.StorageTargets[0]
	assert.Equal(t, "target-extract", target.ExtractPrefix)
	assert.True(t, target.CompressEnabled)
	assert.Equal(t, []string{".js"}, target.CompressExtensions)
	assert.EqualValues(t, 128, target.CompressMinSize)
	assert.Equal(t, 3, target.CompressMaxConcurrent)
	assert.Equal(t, 4, target.CompressLevel)
}
