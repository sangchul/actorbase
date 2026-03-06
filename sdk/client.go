package sdk

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/transport"
	"github.com/oomymy/actorbase/provider"
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
// 이후 ctx 취소 시까지 백그라운드에서 라우팅 업데이트를 수신한다.
// 종료 시 모든 PS 연결을 닫는다.
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

	// 이후 업데이트를 백그라운드에서 처리
	go c.consumeRouting(ctx, ch)

	<-ctx.Done()
	c.connPool.Close() //nolint:errcheck
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
		entry, ok := rt.Lookup(key)
		if !ok {
			return zero, provider.ErrPartitionNotOwned
		}

		conn, err := c.connPool.Get(entry.Node.Address)
		if err != nil {
			return zero, err
		}

		psClient := transport.NewPSClient(conn, c.cfg.Codec)
		var resp Resp
		err = psClient.Send(ctx, entry.Partition.ID, req, &resp)
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
