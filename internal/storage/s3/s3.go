package s3store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/logeable/mongomig/internal/config"
)

// Backend implements storage.Backend for S3-compatible endpoints.
type Backend struct {
	bucket string
	cli    *s3.Client
	up     *manager.Uploader
	down   *manager.Downloader
}

func New(ctx context.Context, s *config.S3) (*Backend, error) {
	if s == nil || s.Endpoint == "" {
		return nil, fmt.Errorf("s3 config missing")
	}
	region := s.Region
	if region == "" {
		region = "us-east-1"
	}
	awsc, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.AccessKeyID, s.SecretAccessKey, "")),
	)
	if err != nil {
		return nil, err
	}
	cli := s3.NewFromConfig(awsc, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(strings.TrimRight(s.Endpoint, "/"))
		o.UsePathStyle = s.UsePathStyle
	})
	return &Backend{
		bucket: s.Bucket,
		cli:    cli,
		up:     manager.NewUploader(cli),
		down:   manager.NewDownloader(cli),
	}, nil
}

func (b *Backend) UploadFile(ctx context.Context, localPath, key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = b.up.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	return err
}

func (b *Backend) DownloadFile(ctx context.Context, key, localPath string) error {
	if err := os.MkdirAll(filepath.Dir(localPath), 0o750); err != nil {
		return err
	}
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = b.down.Download(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (b *Backend) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	paginator := s3.NewListObjectsV2Paginator(b.cli, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	})
	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}
	return keys, nil
}

func (b *Backend) DeletePrefix(ctx context.Context, prefix string) error {
	keys, err := b.ListKeys(ctx, prefix)
	if err != nil {
		return err
	}
	const batch = 1000
	for i := 0; i < len(keys); i += batch {
		j := i + batch
		if j > len(keys) {
			j = len(keys)
		}
		objs := make([]types.ObjectIdentifier, 0, j-i)
		for _, k := range keys[i:j] {
			objs = append(objs, types.ObjectIdentifier{Key: aws.String(k)})
		}
		_, err := b.cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.bucket),
			Delete: &types.Delete{Objects: objs, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
