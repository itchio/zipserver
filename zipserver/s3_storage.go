package zipserver

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Storage struct {
	Session *session.Session
}

func NewS3Storage(config *Config) (*S3Storage, error) {
	var creds *credentials.Credentials

	if config.S3AccessKeyID == "" || config.S3SecretKey == "" {
		creds = credentials.NewEnvCredentials()
	} else {
		creds = credentials.NewStaticCredentials(config.S3AccessKeyID, config.S3SecretKey, "")
	}

	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
		Endpoint:    aws.String(config.S3Endpoint),
	})

	if err != nil {
		return nil, err
	}

	return &S3Storage{
		Session: sess,
	}, nil
}

func (c *S3Storage) PutFile(ctx context.Context, bucket, key string, contents io.Reader, mimeType string) error {
	uploader := s3manager.NewUploaderWithClient(s3.New(c.Session))

	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        contents,
		ContentType: aws.String(mimeType),
	})

	if err != nil {
		return err
	}

	return nil
}
