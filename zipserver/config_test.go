package zipserver

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Config(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "zipserver-config")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte(`{
		"PrivateKeyPath": "/foo/bar.pem",
		"MaxFileSize": 92
	}`))
	if err != nil {
		t.Fatal(err)
	}

	_, err = LoadConfig(tmpFile.Name())
	assert.Error(t, err)

	_, err = tmpFile.Seek(0, os.SEEK_SET)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tmpFile.Write([]byte(`{
		"PrivateKeyPath": "/foo/bar.pem",
		"ClientEmail": "foobar@example.org",
		"Bucket": "chicken",
		"ExtractPrefix": "saca",
		"MaxFileSize": 92
	}`))
	if err != nil {
		t.Fatal(err)
	}

	c, err := LoadConfig(tmpFile.Name())
	assert.NoError(t, err)

	assert.EqualValues(t, "/foo/bar.pem", c.PrivateKeyPath)
	assert.EqualValues(t, 92, c.MaxFileSize)
}
