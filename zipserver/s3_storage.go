package zipserver

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Storage struct {
	Session *session.Session
	config  *StorageConfig
}

func NewS3Storage(config *StorageConfig) (*S3Storage, error) {
	var creds *credentials.Credentials

	if config.S3AccessKeyID == "" || config.S3SecretKey == "" {
		creds = credentials.NewEnvCredentials()
	} else {
		creds = credentials.NewStaticCredentials(config.S3AccessKeyID, config.S3SecretKey, "")
	}

	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
		Endpoint:    aws.String(config.S3Endpoint),
		Region:      aws.String(config.S3Region),
	})

	if err != nil {
		return nil, err
	}

	return &S3Storage{
		config:  config,
		Session: sess,
	}, nil
}

// Compile-time check that S3Storage implements Storage interface
var _ Storage = (*S3Storage)(nil)

// PutFile implements Storage interface - uploads a file with the given options
func (c *S3Storage) PutFile(ctx context.Context, bucket, key string, contents io.Reader, opts PutOptions) (PutResult, error) {
	uploader := s3manager.NewUploaderWithClient(s3.New(c.Session), func(u *s3manager.Uploader) {
		u.PartSize = 1024 * 1024 * 50 // 50Mb per part to avoid excess API calls
	})

	contents = metricsReader(contents, &globalMetrics.TotalBytesUploaded)

	hash := md5.New()

	// duplicate reads into the md5 hasher
	multi := io.TeeReader(contents, hash)

	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   multi,
	}

	if opts.ContentType != "" {
		uploadInput.ContentType = aws.String(opts.ContentType)
	}
	if opts.ContentDisposition != "" {
		uploadInput.ContentDisposition = aws.String(opts.ContentDisposition)
	}
	if opts.ContentEncoding != "" {
		uploadInput.ContentEncoding = aws.String(opts.ContentEncoding)
	}
	if opts.ACL != "" {
		uploadInput.ACL = aws.String(string(opts.ACL))
	}

	_, err := uploader.UploadWithContext(ctx, uploadInput)

	if err != nil {
		return PutResult{}, err
	}

	// Compute the checksum from the hash
	checksum := hash.Sum(nil)

	return PutResult{
		MD5: fmt.Sprintf("%x", checksum),
	}, nil
}

// get some specific metadata for file
func (c *S3Storage) HeadFile(ctx context.Context, bucket, key string) (url.Values, error) {
	svc := s3.New(c.Session)
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := svc.HeadObjectWithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	out := url.Values{}
	if result.ChecksumSHA256 != nil {
		out.Add("ChecksumSHA256", *result.ChecksumSHA256)
	}

	if result.ContentType != nil {
		out.Add("ContentType", *result.ContentType)
	}

	if result.ContentLength != nil {
		out.Add("ContentLength", strconv.FormatInt(*result.ContentLength, 10))
	}

	return out, nil
}

func (c *S3Storage) DeleteFile(ctx context.Context, bucket, key string) error {
	svc := s3.New(c.Session)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err := svc.DeleteObjectWithContext(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

// s3ReaderAt implements ReaderAtCloser using S3 range requests
type s3ReaderAt struct {
	svc       *s3.S3
	bucket    string
	key       string
	size      int64
	maxBytes  uint64 // maximum total bytes to read (0 = unlimited)
	bytesRead uint64 // total bytes read so far
	ctx       context.Context
}

func (r *s3ReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	toRead := uint64(end - off + 1)
	if r.maxBytes > 0 && r.bytesRead+toRead > r.maxBytes {
		return 0, fmt.Errorf("max read limit exceeded (%d bytes)", r.maxBytes)
	}

	rangeStr := fmt.Sprintf("bytes=%d-%d", off, end)
	result, err := r.svc.GetObjectWithContext(r.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		return 0, err
	}
	defer result.Body.Close()

	n, err = io.ReadFull(result.Body, p[:end-off+1])
	r.bytesRead += uint64(n)
	return n, err
}

func (r *s3ReaderAt) Close() error {
	return nil // No resources to release
}

// GetReaderAt returns a ReaderAt for the file, suitable for random access reads.
// This is more efficient than GetFile for operations that only need partial file access.
// maxBytes limits the total bytes that can be read (0 = unlimited).
func (c *S3Storage) GetReaderAt(ctx context.Context, bucket, key string, maxBytes uint64) (ReaderAtCloser, int64, error) {
	svc := s3.New(c.Session)

	// Get file size via HeadObject
	head, err := svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, err
	}

	size := int64(0)
	if head.ContentLength != nil {
		size = *head.ContentLength
	}
	if size == 0 {
		return nil, 0, fmt.Errorf("server did not return Content-Length")
	}

	return &s3ReaderAt{
		svc:      svc,
		bucket:   bucket,
		key:      key,
		size:     size,
		maxBytes: maxBytes,
		ctx:      ctx,
	}, size, nil
}

// GetFile implements Storage interface - downloads a file from S3
func (c *S3Storage) GetFile(ctx context.Context, bucket, key string) (io.ReadCloser, http.Header, error) {
	svc := s3.New(c.Session)
	result, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, nil, err
	}

	headers := http.Header{}
	if result.ContentType != nil {
		headers.Set("Content-Type", *result.ContentType)
	}
	if result.ContentDisposition != nil {
		headers.Set("Content-Disposition", *result.ContentDisposition)
	}
	if result.ContentEncoding != nil {
		headers.Set("Content-Encoding", *result.ContentEncoding)
	}

	return result.Body, headers, nil
}
