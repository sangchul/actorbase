// Package redis provides a Redis Streams-based WALStore implementation.
// Intended for production use where WAL must be shared across multiple PS nodes.
package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/sangchul/actorbase/provider"
)

// WALStore는 Redis Streams 기반 WALStore 구현체.
//
// 파티션당 2개 키를 사용한다:
//
//	{wal:{partitionID}}:lsn    → String  (INCR 카운터, 현재까지 발급된 최대 LSN)
//	{wal:{partitionID}}:stream → Stream  (XADD {lsn} d {binary_data})
//
// LSN을 Stream ID의 ms 파트로 사용하므로 Redis ID = "N-0" 형태가 된다.
// XRANGE/XTRIM에 정수 N을 그대로 전달하면 Redis가 N-0으로 해석한다.
//
// Redis Cluster 환경에서 두 키가 동일 슬롯에 배치되도록 해시태그를 사용한다.
type WALStore struct {
	client goredis.UniversalClient
	prefix string
}

// NewWALStore는 Redis WALStore를 생성한다.
// prefix는 키 네임스페이스로 사용된다. 비어있으면 "wal"이 기본값이다.
// client는 단일 인스턴스, Sentinel, Cluster 모두 지원하는 UniversalClient를 전달한다.
func NewWALStore(client goredis.UniversalClient, prefix string) *WALStore {
	if prefix == "" {
		prefix = "wal"
	}
	return &WALStore{client: client, prefix: prefix}
}

// AppendBatch는 파티션의 WAL에 여러 엔트리를 추가하고 각 LSN을 반환한다.
//
// 구현: INCR으로 LSN 범위를 예약한 뒤 파이프라인으로 XADD를 일괄 전송한다.
// INCR과 XADD 사이에 연결이 끊기면 LSN 번호 일부가 낭비될 수 있으나 correctness에 무해하다.
// (ReadFrom이 빈 LSN을 건너뜀. actor 연산이 멱등하면 partial write도 무해하다.)
func (s *WALStore) AppendBatch(ctx context.Context, partitionID string, data [][]byte) ([]uint64, error) {
	if len(data) == 0 {
		return nil, nil
	}

	n := int64(len(data))
	endLSN, err := s.client.IncrBy(ctx, s.lsnKey(partitionID), n).Result()
	if err != nil {
		return nil, fmt.Errorf("redis.WALStore: INCR lsn: %w", err)
	}
	startLSN := uint64(endLSN) - uint64(n) + 1

	pipe := s.client.Pipeline()
	for i := range data {
		lsn := startLSN + uint64(i)
		pipe.XAdd(ctx, &goredis.XAddArgs{
			Stream: s.streamKey(partitionID),
			ID:     strconv.FormatUint(lsn, 10),
			Values: []any{"d", data[i]},
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redis.WALStore: XADD pipeline: %w", err)
	}

	lsns := make([]uint64, n)
	for i := range lsns {
		lsns[i] = startLSN + uint64(i)
	}
	return lsns, nil
}

// ReadFrom은 fromLSN 이상의 모든 WAL 엔트리를 순서대로 반환한다.
//
// 구현: XRANGE {stream} {fromLSN} + 단일 명령.
// 파티션이 존재하지 않으면 nil을 반환한다 (에러 없음).
func (s *WALStore) ReadFrom(ctx context.Context, partitionID string, fromLSN uint64) ([]provider.WALEntry, error) {
	msgs, err := s.client.XRange(ctx, s.streamKey(partitionID), strconv.FormatUint(fromLSN, 10), "+").Result()
	if err != nil {
		if err == goredis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("redis.WALStore: XRANGE: %w", err)
	}
	if len(msgs) == 0 {
		return nil, nil
	}

	entries := make([]provider.WALEntry, 0, len(msgs))
	for _, msg := range msgs {
		lsn, err := parseMsgID(msg.ID)
		if err != nil {
			continue
		}
		data, _ := msg.Values["d"].(string)
		entries = append(entries, provider.WALEntry{
			LSN:  lsn,
			Data: []byte(data),
		})
	}
	return entries, nil
}

// TrimBefore는 lsn 미만의 오래된 WAL 엔트리를 삭제한다.
//
// 구현: XTRIM {stream} MINID {lsn} 단일 명령.
// 파티션이 존재하지 않으면 no-op이다 (에러 없음).
func (s *WALStore) TrimBefore(ctx context.Context, partitionID string, lsn uint64) error {
	err := s.client.XTrimMinID(ctx, s.streamKey(partitionID), strconv.FormatUint(lsn, 10)).Err()
	if err != nil && err != goredis.Nil {
		return fmt.Errorf("redis.WALStore: XTRIM MINID: %w", err)
	}
	return nil
}

func (s *WALStore) lsnKey(partitionID string) string {
	return "{" + s.prefix + ":" + partitionID + "}:lsn"
}

func (s *WALStore) streamKey(partitionID string) string {
	return "{" + s.prefix + ":" + partitionID + "}:stream"
}

// parseMsgID는 Redis Stream ID "N-0" 형태에서 N을 uint64로 파싱한다.
func parseMsgID(id string) (uint64, error) {
	ms, _, found := strings.Cut(id, "-")
	if !found {
		ms = id
	}
	return strconv.ParseUint(ms, 10, 64)
}
