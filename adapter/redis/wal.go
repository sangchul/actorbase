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

// WALStore is a Redis Streams-based WALStore implementation.
//
// Uses 2 keys per partition:
//
//	{wal:{partitionID}}:lsn    → String  (INCR counter, maximum LSN issued so far)
//	{wal:{partitionID}}:stream → Stream  (XADD {lsn} d {binary_data})
//
// The LSN is used as the ms part of the Stream ID, so the Redis ID has the form "N-0".
// Passing integer N directly to XRANGE/XTRIM is interpreted by Redis as N-0.
//
// Hash tags are used to ensure the two keys are placed in the same slot in a Redis Cluster environment.
type WALStore struct {
	client goredis.UniversalClient
	prefix string
}

// NewWALStore creates a Redis WALStore.
// prefix is used as the key namespace; defaults to "wal" if empty.
// client accepts a UniversalClient that supports single instance, Sentinel, and Cluster modes.
func NewWALStore(client goredis.UniversalClient, prefix string) *WALStore {
	if prefix == "" {
		prefix = "wal"
	}
	return &WALStore{client: client, prefix: prefix}
}

// AppendBatch appends multiple entries to the partition's WAL and returns each LSN.
//
// Implementation: reserves an LSN range via INCR, then sends all XADDs in a pipeline.
// If the connection is lost between INCR and XADD, some LSN numbers may be wasted, but this is harmless to correctness.
// (ReadFrom skips empty LSNs. Partial writes are also harmless when actor operations are idempotent.)
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

// ReadFrom returns all WAL entries with LSN >= fromLSN in order.
//
// Implementation: single XRANGE {stream} {fromLSN} + command.
// Returns nil if the partition does not exist (no error).
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

// TrimBefore removes stale WAL entries with LSN less than lsn.
//
// Implementation: single XTRIM {stream} MINID {lsn} command.
// No-op if the partition does not exist (no error).
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

// parseMsgID parses N as a uint64 from a Redis Stream ID in the form "N-0".
func parseMsgID(id string) (uint64, error) {
	ms, _, found := strings.Cut(id, "-")
	if !found {
		ms = id
	}
	return strconv.ParseUint(ms, 10, 64)
}
