package sdk

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	"github.com/sangchul/actorbase/provider"
)

// Client는 actorbase 클러스터에 요청을 전송하는 SDK 진입점.
//
// 내부적으로:
//   - PM WatchRouting 스트림으로 라우팅 테이블을 수신·캐싱한다.
//   - key로 담당 파티션을 찾아 해당 PS에 요청을 전달한다.
//   - 라우팅 오류 시 테이블 갱신을 기다려 재시도한다.
type Client[Req, Resp any] struct {
	cfg      Config[Req, Resp]
	pmClient *transport.PMClient
	connPool *transport.ConnPool
	routing  atomic.Pointer[domain.RoutingTable]
}

// NewClient는 Config를 검증하고 PM 연결을 포함한 Client를 생성한다.
// Start를 호출하기 전까지는 Send를 호출하면 안 된다.
func NewClient[Req, Resp any](cfg Config[Req, Resp]) (*Client[Req, Resp], error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	pmConn, err := grpc.NewClient(cfg.PMAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client[Req, Resp]{
		cfg:      cfg,
		pmClient: transport.NewPMClient(pmConn),
		connPool: transport.NewConnPool(),
	}, nil
}

// Start는 PM WatchRouting 스트림을 시작하고 첫 라우팅 테이블을 수신할 때까지 대기한다.
// 첫 테이블 수신 후 즉시 반환하며, 이후 라우팅 업데이트와 연결 정리는 백그라운드에서 처리된다.
// Start가 nil을 반환한 이후에는 Send를 즉시 호출해도 안전하다.
func (c *Client[Req, Resp]) Start(ctx context.Context) error {
	ch := c.pmClient.WatchRouting(ctx, c.cfg.ClientID)

	// 첫 라우팅 테이블 수신 대기
	select {
	case rt, ok := <-ch:
		if !ok {
			return ctx.Err()
		}
		c.routing.Store(rt)
	case <-ctx.Done():
		return ctx.Err()
	}

	// 이후 업데이트 수신 및 종료 시 연결 정리를 백그라운드에서 처리
	go func() {
		c.consumeRouting(ctx, ch)
		c.connPool.Close() //nolint:errcheck
	}()

	return nil
}

func (c *Client[Req, Resp]) consumeRouting(_ context.Context, ch <-chan *domain.RoutingTable) {
	for rt := range ch {
		c.routing.Store(rt)
	}
}

// Send는 key를 담당하는 Actor에 req를 전달하고 Resp를 반환한다.
// 라우팅 오류(ErrPartitionMoved, ErrPartitionNotOwned, ErrPartitionBusy) 시
// 최대 MaxRetries까지 재시도한다.
func (c *Client[Req, Resp]) Send(ctx context.Context, key string, req Req) (Resp, error) {
	var zero Resp
	var lastErr error

	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		rt := c.routing.Load()
		entry, ok := rt.Lookup(c.cfg.TypeID, key)
		if !ok {
			return zero, provider.ErrPartitionNotOwned
		}

		conn, err := c.connPool.Get(entry.Node.Address)
		if err != nil {
			return zero, err
		}

		psClient := transport.NewPSClient(conn, c.cfg.Codec)
		var resp Resp
		err = psClient.Send(ctx, c.cfg.TypeID, entry.Partition.ID, req, &resp)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		switch {
		case errors.Is(err, provider.ErrNotFound),
			errors.Is(err, provider.ErrTimeout),
			errors.Is(err, provider.ErrActorPanicked):
			return zero, err

		case errors.Is(err, provider.ErrPartitionMoved),
			errors.Is(err, provider.ErrPartitionNotOwned),
			errors.Is(err, provider.ErrPartitionBusy):
			if attempt < c.cfg.MaxRetries {
				select {
				case <-time.After(c.cfg.RetryInterval):
				case <-ctx.Done():
					return zero, ctx.Err()
				}
			}

		default:
			return zero, err
		}
	}

	return zero, lastErr
}

// Scan은 [startKey, endKey) 범위에 걸친 모든 파티션에 req를 fan-out하고
// 파티션 순서대로 정렬된 []Resp를 반환한다.
//
// 보장:
//   - 누락 없음: stale 라우팅 또는 partition split으로 인한 ErrPartitionMoved/Busy 시
//     해당 파티션 범위부터 재시도하여 키 누락을 방지한다.
//   - 순서 보장: 결과는 파티션 KeyRange.Start 기준 오름차순이다.
//
// endKey == ""이면 모든 파티션의 끝까지 scan한다.
func (c *Client[Req, Resp]) Scan(ctx context.Context, startKey, endKey string, req Req) ([]Resp, error) {
	type scanResult struct {
		entry domain.RouteEntry
		resp  Resp
		err   error
	}

	var results []Resp
	currentStart := startKey

	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		rt := c.routing.Load()
		partitions := rt.PartitionsInRange(c.cfg.TypeID, currentStart, endKey)
		if len(partitions) == 0 {
			break
		}

		// 병렬 fan-out
		resCh := make(chan scanResult, len(partitions))
		var wg sync.WaitGroup
		for _, entry := range partitions {
			entry := entry
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := c.connPool.Get(entry.Node.Address)
				if err != nil {
					resCh <- scanResult{entry: entry, err: err}
					return
				}
				var resp Resp
				err = transport.NewPSClient(conn, c.cfg.Codec).Scan(
					ctx, c.cfg.TypeID, entry.Partition.ID, req, &resp,
					entry.Partition.KeyRange.Start, entry.Partition.KeyRange.End,
				)
				resCh <- scanResult{entry: entry, resp: resp, err: err}
			}()
		}
		wg.Wait()
		close(resCh)

		// 결과 수집 후 KeyRange.Start 기준 정렬
		collected := make([]scanResult, 0, len(partitions))
		for r := range resCh {
			collected = append(collected, r)
		}
		sort.Slice(collected, func(i, j int) bool {
			return collected[i].entry.Partition.KeyRange.Start < collected[j].entry.Partition.KeyRange.Start
		})

		// 앞에서부터 성공한 파티션 결과를 순서대로 누적.
		// 실패한 파티션에서 멈추고 currentStart를 해당 파티션의 시작으로 설정.
		allSucceeded := true
		for _, r := range collected {
			if r.err != nil {
				allSucceeded = false
				currentStart = r.entry.Partition.KeyRange.Start
				// 재시도 불필요한 에러는 즉시 반환
				if !errors.Is(r.err, provider.ErrPartitionMoved) &&
					!errors.Is(r.err, provider.ErrPartitionNotOwned) &&
					!errors.Is(r.err, provider.ErrPartitionBusy) {
					return nil, r.err
				}
				break
			}
			results = append(results, r.resp)
		}

		if allSucceeded {
			break
		}

		select {
		case <-time.After(c.cfg.RetryInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return results, nil
}
