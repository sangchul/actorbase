package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/sangchul/actorbase/internal/domain"
)

const redisRoutingKey = "actorbase:routing"

// NewRedisRoutingTableStore creates a Redis-backed RoutingTableStore.
//
// The PM is the sole writer of the routing table, so external pub/sub is not
// needed. Watch() is implemented with in-process subscriber channels:
// Save() delivers the new table to all active Watch() callers immediately,
// without any round-trip to Redis.
func NewRedisRoutingTableStore(cli goredis.UniversalClient) RoutingTableStore {
	return &redisRoutingTableStore{cli: cli}
}

type redisRoutingTableStore struct {
	cli goredis.UniversalClient

	mu   sync.Mutex
	subs []chan *domain.RoutingTable // active Watch() subscribers
}

func (s *redisRoutingTableStore) Save(ctx context.Context, rt *domain.RoutingTable) error {
	data, err := marshalRoutingTable(rt)
	if err != nil {
		return fmt.Errorf("marshal routing table: %w", err)
	}
	if err := s.cli.Set(ctx, redisRoutingKey, string(data), 0).Err(); err != nil {
		return fmt.Errorf("save routing table to redis: %w", err)
	}

	// Notify all in-process Watch() subscribers.
	s.mu.Lock()
	alive := s.subs[:0]
	for _, ch := range s.subs {
		select {
		case ch <- rt:
			alive = append(alive, ch)
		default:
			// Subscriber is full or gone; keep it in the list so it can drain later.
			alive = append(alive, ch)
		}
	}
	s.subs = alive
	s.mu.Unlock()

	return nil
}

func (s *redisRoutingTableStore) Load(ctx context.Context) (*domain.RoutingTable, error) {
	val, err := s.cli.Get(ctx, redisRoutingKey).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load routing table from redis: %w", err)
	}
	return unmarshalRoutingTable([]byte(val))
}

// Watch returns a channel that delivers the current routing table immediately,
// then pushes every subsequent update written via Save().
// The channel is closed when ctx is cancelled.
func (s *redisRoutingTableStore) Watch(ctx context.Context) <-chan *domain.RoutingTable {
	ch := make(chan *domain.RoutingTable, 8)

	// Register before loading so we don't miss a Save() that races with the initial load.
	s.mu.Lock()
	s.subs = append(s.subs, ch)
	s.mu.Unlock()

	go func() {
		defer func() {
			// Unregister and close when the caller's ctx is done.
			s.mu.Lock()
			for i, sub := range s.subs {
				if sub == ch {
					s.subs = append(s.subs[:i], s.subs[i+1:]...)
					break
				}
			}
			s.mu.Unlock()
			close(ch)
		}()

		// Deliver the current table immediately (initial sync).
		rt, err := s.Load(ctx)
		if err != nil {
			slog.Error("redis routing: initial load failed", "err", err)
			return
		}
		select {
		case ch <- rt: // deliver even if nil
		case <-ctx.Done():
			return
		}

		<-ctx.Done()
	}()

	return ch
}
