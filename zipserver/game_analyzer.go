package zipserver

import (
	"bytes"
	"io"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/go-errors/errors"
)

// GameAnalyzer uses rules applying to HTML5 game uploads.
// gzip-compressed files are marked with the appropriate content type and encoding.
//
// If onlyExtractFiles is non-nil, files whose names not in the list will be skipped.
type GameAnalyzer struct {
	onlyExtractFiles []string
}

func (d GameAnalyzer) shouldExtract(name string) bool {
	if d.onlyExtractFiles == nil {
		return true
	}
	for _, v := range d.onlyExtractFiles {
		if name == v {
			return true
		}
	}
	return false
}

func (d GameAnalyzer) Analyze(r io.Reader, filename string) (AnalyzeResult, error) {
	res := AnalyzeResult{}

	if !d.shouldExtract(filename) {
		return res, ErrSkipped
	}

	mimeType := mime.TypeByExtension(path.Ext(filename))

	var buffer bytes.Buffer
	_, err := io.Copy(&buffer, io.LimitReader(r, 512))
	if err != nil {
		return res, errors.Wrap(err, 0)
	}

	contentMimeType := http.DetectContentType(buffer.Bytes())
	extension := path.Ext(filename)

	if contentMimeType == "application/x-gzip" || contentMimeType == "application/gzip" {
		res.ContentEncoding = "gzip"

		// try to see if there's a real extension hidden beneath
		if extension == ".gz" {
			realMimeType := mime.TypeByExtension(path.Ext(strings.TrimSuffix(filename, ".gz")))
			if realMimeType != "" {
				mimeType = realMimeType
			}
		} else {
			// To support gzip-compressed exports from Unity 5.5 and below, rename file.
			// https://docs.unity3d.com/550/Documentation/Manual/webgl-deploying.html
			if replacement, ok := unityExtReplacements[extension]; ok {
				res.RenameTo = strings.TrimSuffix(filename, extension) + replacement
			}
		}
	} else if extension == ".br" {
		// there is no way to detect a brotli stream by content, so we assume if it ends if .br then it's brotli
		// this path is used for Unity 2020 webgl games built with brotli compression
		res.ContentEncoding = "br"
		realMimeType := mime.TypeByExtension(path.Ext(strings.TrimSuffix(filename, ".br")))
		if realMimeType != "" {
			mimeType = realMimeType
		}
	} else if mimeType == "" {
		// fall back to the extension detected from content, eg. someone uploaded a .png with wrong extension
		mimeType = contentMimeType
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	res.ContentType = mimeType

	return res, nil
}

var unityExtReplacements = map[string]string{
	".jsgz":      ".js",
	".datagz":    ".data",
	".memgz":     ".mem",
	".unity3dgz": ".unity3d",
}
