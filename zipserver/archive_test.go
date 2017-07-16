package zipserver

import "testing"

func TestExtractZip(t *testing.T) {
	client, config := getClient(t)
	archiver := &Archiver{client, config}

	limits := &ExtractLimits{
		MaxFileSize:       1024 * 1024 * 200,
		MaxTotalSize:      1024 * 1024 * 500,
		MaxNumFiles:       100,
		MaxFileNameLength: 80,
		ExtractionThreads: 4,
	}

	err := client.PutFileFromFname(config.Bucket, "zipserver_test/test.zip", "/home/leafo/code/go/etlua.zip")
	if err != nil {
		t.Fatal(err)
	}

	_, err = archiver.ExtractZip("zipserver_test/test.zip", "zipserver_test/extract", limits)

	if err != nil {
		t.Fatal(err)
	}
}
