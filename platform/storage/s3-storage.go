package storage

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Storage struct {
	Client        *s3.Client
	PresignClient *s3.PresignClient
	Bucket        string
}

func NewS3Storage(endpoint, accessKeyID, secretAccessKey, bucket string) (*S3Storage, error) {
	if endpoint == "" ||
		accessKeyID == "" ||
		secretAccessKey == "" ||
		bucket == "" {
		return nil, fmt.Errorf("missing R2 or S3 environment variables")
	}

	cfg := aws.Config{
		Region: "auto",
		Credentials: credentials.NewStaticCredentialsProvider(
			accessKeyID,
			secretAccessKey,
			"",
		),
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &S3Storage{
		Client:        client,
		PresignClient: s3.NewPresignClient(client),
		Bucket:        bucket,
	}, nil
}
