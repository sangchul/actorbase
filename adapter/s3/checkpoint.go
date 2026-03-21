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

// CheckpointStore는 AWS S3(또는 S3-compatible) 기반 CheckpointStore 구현체.
//
// 오브젝트 키 구조:
//
//	{prefix}/{partitionID}  (prefix="" 이면 {partitionID})
type CheckpointStore struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewCheckpointStore는 S3 CheckpointStore를 생성한다.
// prefix가 빈 문자열이면 partitionID가 그대로 오브젝트 키가 된다.
func NewCheckpointStore(client *s3.Client, bucket, prefix string) *CheckpointStore {
	return &CheckpointStore{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}
}

// Save는 파티션의 스냅샷을 S3에 저장한다. 기존 오브젝트가 있으면 덮어쓴다.
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

// Load는 파티션의 스냅샷을 S3에서 읽어 반환한다.
// 오브젝트가 없으면 (nil, nil)을 반환한다.
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

// Delete는 파티션의 스냅샷을 S3에서 삭제한다. 오브젝트가 없으면 no-op.
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

// isNoSuchKey는 S3 "NoSuchKey" 오류 여부를 판별한다.
// 타입 어설션 대신 코드 문자열 비교를 사용하여 S3-compatible 서비스와의 호환성을 높인다.
func isNoSuchKey(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey"
}
