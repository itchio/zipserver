package zipserver

import (
	"bytes"
	"github.com/klauspost/compress/zip"
	"testing"
)

func TestCheckContentLength(t *testing.T) {
	if err := checkContentLength(10, -1); err != nil {
		t.Fatalf("expected no error for unknown length, got %v", err)
	}
	if err := checkContentLength(10, 10); err != nil {
		t.Fatalf("expected no error for exact limit, got %v", err)
	}
	if err := checkContentLength(10, 11); err == nil {
		t.Fatalf("expected error for over limit")
	}
}

func TestListZipBytesMaxListFiles(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for range 3 {
		f, err := zw.CreateHeader(&zip.FileHeader{Name: "file"})
		if err != nil {
			t.Fatalf("create header: %v", err)
		}
		if _, err := f.Write([]byte("x")); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}

	ops := &Operations{config: &Config{MaxListFiles: 2}}
	result := ops.listZipBytes(buf.Bytes())
	if result.Err == nil {
		t.Fatalf("expected error for too many files")
	}
}
