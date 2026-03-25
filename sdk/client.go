package sdk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	"github.com/sangchul/actorbase/provider"
)

// Client is the SDK entry point for sending requests to an actorbase cluster.
//
// Internally:
//   - Receives and caches the routing table via the PM WatchRouting stream.
//   - Finds the responsible partition by key and forwards the request to the corresponding PS.
//   - On routing errors, waits for the table to be refreshed and retries.
type Client[Req, Resp any] struct {
	cfg      Config[Req, Resp]
	pmClient *transport.PMClient
	connPool *transport.ConnPool
	routing  atomic.Pointer[domain.RoutingTable]
}

// NewClient validates the Config and creates a Client including the PM connection.
// If EtcdEndpoints is configured, the current PM leader address is resolved from etcd.
// Send must not be called before Start returns.
func NewClient[Req, Resp any](cfg Config[Req, Resp]) (*Client[Req, Resp], error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	pmAddr := cfg.PMAddr
	if cfg.haMode() {
		addr, err := discoverLeaderAddr(cfg.EtcdEndpoints)
		if err != nil {
			return nil, fmt.Errorf("sdk: discover PM leader: %w", err)
		}
		pmAddr = addr
	}

	pmConn, err := grpc.NewClient(pmAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client[Req, Resp]{
		cfg:      cfg,
		pmClient: transport.NewPMClient(pmConn),
		connPool: transport.NewConnPool(),
	}, nil
}

// discoverLeaderAddr resolves the current PM leader address from etcd.
// Waits up to 30 seconds for a leader to be elected.
func discoverLeaderAddr(endpoints []string) (string, error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return "", fmt.Errorf("create etcd client: %w", err)
	}
	defer etcdCli.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	addr, err := cluster.WaitForLeader(ctx, etcdCli)
	if err != nil {
		return "", err
	}
	return addr, nil
}

// Start initiates the PM WatchRouting stream and waits until the first routing table is received.
// Returns immediately after the first table is received; subsequent routing updates and connection
// cleanup are handled in the background.
// It is safe to call Send immediately after Start returns nil.
func (c *Client[Req, Resp]) Start(ctx context.Context) error {
	ch := c.pmClient.WatchRouting(ctx, c.cfg.ClientID)

	// wait for the first routing table
	select {
	case rt, ok := <-ch:
		if !ok {
			return ctx.Err()
		}
		c.routing.Store(rt)
	case <-ctx.Done():
		return ctx.Err()
	}

	// handle subsequent updates and connection cleanup on shutdown in the background
	go func() {
		c.consumeRouting(ctx, ch)
		c.connPool.Close() //nolint:errcheck
	}()

	return nil
}

// consumeRouting receives routing tables from the WatchRouting channel.
// In HA mode, when the channel is closed (PM failure), it discovers a new leader from etcd and reconnects.
func (c *Client[Req, Resp]) consumeRouting(ctx context.Context, ch <-chan *domain.RoutingTable) {
	for {
		for rt := range ch {
			c.routing.Store(rt)
		}

		// channel closed — exit if not in HA mode or if ctx is cancelled
		if !c.cfg.haMode() || ctx.Err() != nil {
			return
		}

		// HA mode: discover a new PM leader and reconnect
		slog.Warn("sdk: PM connection lost, re-discovering leader...")
		newAddr, err := c.rediscoverLeader(ctx)
		if err != nil {
			slog.Error("sdk: leader re-discovery failed, stopping", "err", err)
			return
		}

		slog.Info("sdk: reconnecting to new PM leader", "addr", newAddr)
		newConn, err := grpc.NewClient(newAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("sdk: failed to create PM connection", "addr", newAddr, "err", err)
			return
		}
		c.pmClient = transport.NewPMClient(newConn)
		ch = c.pmClient.WatchRouting(ctx, c.cfg.ClientID)
	}
}

// rediscoverLeader retries until a new PM leader is discovered from etcd.
func (c *Client[Req, Resp]) rediscoverLeader(ctx context.Context) (string, error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   c.cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return "", fmt.Errorf("create etcd client: %w", err)
	}
	defer etcdCli.Close() //nolint:errcheck

	// wait until a new leader is elected (up to 30 seconds)
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	addr, err := cluster.WaitForLeader(waitCtx, etcdCli)
	if err != nil {
		return "", err
	}
	return addr, nil
}

// Send forwards req to the Actor responsible for the given key and returns a Resp.
// Retries up to MaxRetries on routing errors (ErrPartitionMoved, ErrPartitionNotOwned, ErrPartitionBusy).
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

// Scan fans out req to all partitions covering the [startKey, endKey) range
// and returns []Resp sorted in partition order.
//
// Guarantees:
//   - No omissions: on ErrPartitionMoved/Busy due to stale routing or partition split,
//     retries from the affected partition range to prevent missing keys.
//   - Order guarantee: results are in ascending order by partition KeyRange.Start.
//
// If endKey == "", the scan covers through the end of all partitions.
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

		// parallel fan-out
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

		// collect results then sort by KeyRange.Start
		collected := make([]scanResult, 0, len(partitions))
		for r := range resCh {
			collected = append(collected, r)
		}
		sort.Slice(collected, func(i, j int) bool {
			return collected[i].entry.Partition.KeyRange.Start < collected[j].entry.Partition.KeyRange.Start
		})

		// accumulate successful partition results in order from the front.
		// stop at the first failed partition and set currentStart to that partition's start.
		allSucceeded := true
		for _, r := range collected {
			if r.err != nil {
				allSucceeded = false
				currentStart = r.entry.Partition.KeyRange.Start
				// return immediately for errors that do not warrant a retry
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
