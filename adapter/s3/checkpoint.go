package s3adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
)

// CheckpointStore is a CheckpointStore implementation backed by AWS S3 (or an S3-compatible service).
//
// Object key structure:
//
//	{prefix}/{partitionID}  (if prefix="" then just {partitionID})
type CheckpointStore struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewCheckpointStore creates an S3-backed CheckpointStore.
// If prefix is an empty string, partitionID is used directly as the object key.
func NewCheckpointStore(client *s3.Client, bucket, prefix string) *CheckpointStore {
	return &CheckpointStore{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}
}

// Save stores a snapshot of the partition to S3. Overwrites any existing object.
func (s *CheckpointStore) Save(ctx context.Context, partitionID string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.objectKey(partitionID)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("s3.CheckpointStore: put object: %w", err)
	}
	return nil
}

// Load reads and returns a snapshot of the partition from S3.
// Returns (nil, nil) if the object does not exist.
func (s *CheckpointStore) Load(ctx context.Context, partitionID string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectKey(partitionID)),
	})
	if err != nil {
		if isNoSuchKey(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("s3.CheckpointStore: get object: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3.CheckpointStore: read body: %w", err)
	}
	return data, nil
}

// Delete removes the snapshot for the partition from S3. No-op if the object does not exist.
func (s *CheckpointStore) Delete(ctx context.Context, partitionID string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectKey(partitionID)),
	})
	if err != nil {
		return fmt.Errorf("s3.CheckpointStore: delete object: %w", err)
	}
	return nil
}

func (s *CheckpointStore) objectKey(partitionID string) string {
	if s.prefix == "" {
		return partitionID
	}
	return s.prefix + "/" + partitionID
}

// isNoSuchKey determines whether the error is an S3 "NoSuchKey" error.
// Uses error code string comparison instead of type assertion for broader compatibility with S3-compatible services.
func isNoSuchKey(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey"
}
