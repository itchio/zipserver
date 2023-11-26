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

// upload file and return md5 checksum of transferred bytes
func (c *S3Storage) PutFile(ctx context.Context, bucket, key string, contents io.Reader, uploadHeaders http.Header) (string, error) {
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

	if contentType := uploadHeaders.Get("Content-Type"); contentType != "" {
		uploadInput.ContentType = aws.String(contentType)
	}

	if contentDisposition := uploadHeaders.Get("Content-Disposition"); contentDisposition != "" {
		uploadInput.ContentDisposition = aws.String(contentDisposition)
	}

	_, err := uploader.UploadWithContext(ctx, uploadInput)

	if err != nil {
		return "", err
	}

	// Compute the checksum from the hash.
	checksum := hash.Sum(nil)

	// Convert the checksum to a hexadecimal string.
	checksumStr := fmt.Sprintf("%x", checksum)

	return checksumStr, nil
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
