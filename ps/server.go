package ps

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/engine"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/provider"
)

// ── actorDispatcher ───────────────────────────────────────────────────────────

// actorDispatcher is an interface that wraps engine.ActorHost in a type-erased manner.
// Send/Evict/EvictAll/Activate/Split operate at the byte-slice level,
// allowing a single PS to handle multiple actor types.
type actorDispatcher interface {
	Send(ctx context.Context, partitionID string, payload []byte) ([]byte, error)
	Evict(ctx context.Context, partitionID string) error
	EvictAll(ctx context.Context) error
	Activate(ctx context.Context, partitionID string) error
	Split(ctx context.Context, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID string) (string, error)
	Merge(ctx context.Context, lowerPartitionID, upperPartitionID string) error
	StartSchedulers(ctx context.Context, idleTimeout, evictInterval, checkpointInterval time.Duration)
	GetStats() []engine.PartitionStats
	TypeID() string
}

// typedDispatcher wraps engine.ActorHost[Req, Resp] as an actorDispatcher.
type typedDispatcher[Req, Resp any] struct {
	typeID string
	host   *engine.ActorHost[Req, Resp]
	codec  provider.Codec
}

func newTypedDispatcher[Req, Resp any](cfg TypeConfig[Req, Resp]) *typedDispatcher[Req, Resp] {
	host := engine.NewActorHost[Req, Resp](engine.Config[Req, Resp]{
		Factory:                cfg.Factory,
		WALStore:               cfg.WALStore,
		CheckpointStore:        cfg.CheckpointStore,
		MailboxSize:            cfg.MailboxSize,
		FlushSize:              cfg.FlushSize,
		FlushInterval:          cfg.FlushInterval,
		CheckpointWALThreshold: cfg.CheckpointWALThreshold,
	})
	return &typedDispatcher[Req, Resp]{typeID: cfg.TypeID, host: host, codec: cfg.Codec}
}

func (d *typedDispatcher[Req, Resp]) Send(ctx context.Context, partitionID string, payload []byte) ([]byte, error) {
	var req Req
	if err := d.codec.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	resp, err := d.host.Send(ctx, partitionID, req)
	if err != nil {
		return nil, err
	}
	return d.codec.Marshal(resp)
}

func (d *typedDispatcher[Req, Resp]) Evict(ctx context.Context, partitionID string) error {
	return d.host.Evict(ctx, partitionID)
}

func (d *typedDispatcher[Req, Resp]) EvictAll(ctx context.Context) error {
	return d.host.EvictAll(ctx)
}

func (d *typedDispatcher[Req, Resp]) Activate(ctx context.Context, partitionID string) error {
	return d.host.Activate(ctx, partitionID)
}

func (d *typedDispatcher[Req, Resp]) Split(ctx context.Context, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID string) (string, error) {
	return d.host.Split(ctx, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID)
}

func (d *typedDispatcher[Req, Resp]) Merge(ctx context.Context, lowerPartitionID, upperPartitionID string) error {
	return d.host.Merge(ctx, lowerPartitionID, upperPartitionID)
}

func (d *typedDispatcher[Req, Resp]) StartSchedulers(
	ctx context.Context,
	idleTimeout, evictInterval, checkpointInterval time.Duration,
) {
	evictSched := engine.NewEvictionScheduler[Req, Resp](d.host, idleTimeout, evictInterval)
	go evictSched.Start(ctx)
	cpSched := engine.NewCheckpointScheduler[Req, Resp](d.host, checkpointInterval)
	go cpSched.Start(ctx)
}

func (d *typedDispatcher[Req, Resp]) GetStats() []engine.PartitionStats {
	return d.host.GetStats()
}

func (d *typedDispatcher[Req, Resp]) TypeID() string {
	return d.typeID
}

// ── ServerBuilder ─────────────────────────────────────────────────────────────

// ServerBuilder assembles a PS server.
// Create one with NewServerBuilder, register actor types with Register, then call Build to produce a Server.
//
// Example:
//
//	builder := ps.NewServerBuilder(baseConfig)
//	ps.Register(builder, ps.TypeConfig[KVReq, KVResp]{TypeID: "kv", ...})
//	srv, err := builder.Build()
type ServerBuilder struct {
	base        BaseConfig
	dispatchers map[string]actorDispatcher
}

// NewServerBuilder creates a ServerBuilder from the given BaseConfig.
func NewServerBuilder(cfg BaseConfig) *ServerBuilder {
	return &ServerBuilder{
		base:        cfg,
		dispatchers: make(map[string]actorDispatcher),
	}
}

// Register registers an actor type with the PS.
// Provided as a package-level function rather than a method due to Go generics constraints.
func Register[Req, Resp any](b *ServerBuilder, cfg TypeConfig[Req, Resp]) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if _, dup := b.dispatchers[cfg.TypeID]; dup {
		return errorf("actor type %q already registered", cfg.TypeID)
	}
	b.dispatchers[cfg.TypeID] = newTypedDispatcher(cfg)
	return nil
}

// Build creates a Server.
func (b *ServerBuilder) Build() (*Server, error) {
	b.base.setDefaults()
	if err := b.base.validate(); err != nil {
		return nil, err
	}
	if len(b.dispatchers) == 0 {
		return nil, errorf("at least one actor type must be registered via Register()")
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   b.base.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	rtStore := cluster.NewRoutingTableStore(etcdCli)

	grpcSrv := transport.NewGRPCServer(transport.ServerConfig{
		ListenAddr: b.base.Addr,
		Metrics:    b.base.Metrics,
	})

	s := &Server{
		cfg:         b.base,
		dispatchers: b.dispatchers,
		rtStore:     rtStore,
		grpcSrv:     grpcSrv,
		etcdCli:     etcdCli,
	}

	pb.RegisterPartitionServiceServer(grpcSrv, &partitionHandler{
		dispatchers:           b.dispatchers,
		routing:               &s.routing,
		nodeID:                b.base.NodeID,
		fenced:                &s.fenced,
		lastHeartbeatOK:       &s.lastHeartbeatOK,
		ownershipLeaseTimeout: b.base.OwnershipLeaseTimeout,
	})
	pb.RegisterPartitionControlServiceServer(grpcSrv, &controlHandler{
		dispatchers: b.dispatchers,
		nodeID:      b.base.NodeID,
		routing:     &s.routing,
		fenced:      &s.fenced,
	})

	return s, nil
}

// ── Server ────────────────────────────────────────────────────────────────────

// Server is the entry point for the Partition Server.
// Created via ServerBuilder.Build().
type Server struct {
	cfg         BaseConfig
	dispatchers map[string]actorDispatcher
	rtStore     cluster.RoutingTableStore
	grpcSrv     *grpc.Server
	etcdCli     *clientv3.Client
	routing         atomic.Pointer[domain.RoutingTable]
	fenced          atomic.Bool  // true when the node is isolated and shutting down
	lastHeartbeatOK atomic.Int64 // Unix nanoseconds of the last successful PM heartbeat
}

// Start starts the PS. Returns after graceful shutdown when ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	slog.Info("ps: waiting for PM leader", "node", s.cfg.NodeID)

	// 1. Wait until the PM leader is elected.
	pmAddr, err := cluster.WaitForLeader(ctx, s.etcdCli)
	if err != nil {
		return err
	}
	slog.Info("ps: PM leader found, starting", "node", s.cfg.NodeID, "addr", s.cfg.Addr, "pm", pmAddr)

	// 2. Request cluster admission from PM. PM validates Waiting state and transitions to Active.
	if err := s.requestJoin(ctx, pmAddr); err != nil {
		return fmt.Errorf("ps: RequestJoin rejected by PM: %w", err)
	}
	slog.Info("ps: PM admitted node", "node", s.cfg.NodeID)

	// 3. Start PM heartbeat loop. Uses an inner context so isolation fencing can
	//    cancel without touching the caller's ctx.
	// Initialize lastHeartbeatOK to now so the ownership lease does not expire
	// before the first heartbeat tick fires.
	s.lastHeartbeatOK.Store(time.Now().UnixNano())
	innerCtx, innerCancel := context.WithCancel(ctx)
	defer innerCancel()
	go s.heartbeatLoop(innerCtx, pmAddr, innerCancel)

	// 4. Start watching the routing table and wait for the initial value.
	rtCh := s.rtStore.Watch(innerCtx)
	firstRT, ok := <-rtCh
	if !ok {
		return fmt.Errorf("routing table watch closed before receiving initial value")
	}
	s.routing.Store(firstRT)
	if firstRT != nil {
		slog.Info("ps: initial routing table received", "node", s.cfg.NodeID, "version", firstRT.Version(), "partitions", len(firstRT.Entries()))
	}

	// 5. Handle subsequent routing updates in the background.
	go s.watchRouting(innerCtx, rtCh)

	// 6. Start the gRPC server.
	lis, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.Addr, err)
	}
	grpcErrCh := make(chan error, 1)
	go func() {
		grpcErrCh <- s.grpcSrv.Serve(lis)
	}()
	slog.Info("ps: gRPC server started", "node", s.cfg.NodeID, "addr", s.cfg.Addr)

	// 7. Start schedulers for each actor type.
	for _, d := range s.dispatchers {
		slog.Info("ps: starting schedulers", "node", s.cfg.NodeID, "type", d.TypeID(),
			"idle_timeout", s.cfg.IdleTimeout, "evict_interval", s.cfg.EvictInterval)
		d.StartSchedulers(innerCtx, s.cfg.IdleTimeout, s.cfg.EvictInterval, s.cfg.CheckpointInterval)
	}

	// 8. Wait for inner context cancellation (normal shutdown OR isolation fencing).
	select {
	case <-innerCtx.Done():
	case err := <-grpcErrCh:
		return fmt.Errorf("grpc server error: %w", err)
	}

	return s.shutdown()
}

func (s *Server) watchRouting(ctx context.Context, ch <-chan *domain.RoutingTable) {
	for rt := range ch {
		s.routing.Store(rt)
		if rt != nil {
			slog.Info("ps: routing table updated", "node", s.cfg.NodeID, "version", rt.Version(), "partitions", len(rt.Entries()))
		}
	}
}

// heartbeatLoop sends a Heartbeat RPC to the PM leader every HeartbeatInterval.
// If PM is unreachable, it checks etcd to distinguish between PM failure and node isolation.
//
// Fencing logic:
//   - PM unreachable + etcd unreachable consecutively >= IsolationFenceLimit → isolated → fence + cancel
//   - PM unreachable + etcd OK → PM is down (HA failover in progress), keep serving
//   - PM ok → reset isolation counter
func (s *Server) heartbeatLoop(ctx context.Context, pmAddr string, cancel context.CancelFunc) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck

	isolatedConsecutive := 0

	for {
		select {
		case <-ticker.C:
			conn, connErr := pool.Get(pmAddr)
			var hbErr error
			if connErr != nil {
				hbErr = connErr
			} else {
				hbCtx, hbCancel := context.WithTimeout(ctx, s.cfg.HeartbeatInterval/2)
				hbErr = transport.NewPMClient(conn).Heartbeat(hbCtx, s.cfg.NodeID)
				hbCancel()
			}

			if hbErr == nil {
				// Heartbeat succeeded — node is not isolated.
				isolatedConsecutive = 0
				s.lastHeartbeatOK.Store(time.Now().UnixNano())
				continue
			}

			// PM heartbeat failed — check etcd to determine whether this node is isolated.
			etcdCtx, etcdCancel := context.WithTimeout(ctx, s.cfg.HeartbeatInterval/2)
			newAddr, etcdErr := cluster.GetLeaderAddr(etcdCtx, s.etcdCli)
			etcdCancel()

			if etcdErr != nil {
				// Both PM and etcd are unreachable — possible network isolation.
				isolatedConsecutive++
				slog.Warn("ps: both PM and etcd unreachable (possible isolation)",
					"node", s.cfg.NodeID, "consecutive", isolatedConsecutive,
					"limit", s.cfg.IsolationFenceLimit)
				if isolatedConsecutive >= s.cfg.IsolationFenceLimit {
					slog.Error("ps: node isolated — fencing and initiating shutdown",
						"node", s.cfg.NodeID)
					s.fenced.Store(true)
					cancel()
					return
				}
			} else {
				// etcd is reachable → PM is down (maintenance or HA election), node is not isolated.
				isolatedConsecutive = 0
				if newAddr != "" && newAddr != pmAddr {
					slog.Info("ps: PM leader changed, updating heartbeat target",
						"node", s.cfg.NodeID, "old_pm", pmAddr, "new_pm", newAddr)
					pmAddr = newAddr
				} else {
					slog.Info("ps: PM unavailable (election in progress), continuing to serve",
						"node", s.cfg.NodeID)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// notifyEvictionComplete signals PM that all WAL data has been flushed to shared storage.
// This lets PM skip walFlushMargin and immediately assign partitions to a replacement PS.
// Errors are only logged; the shutdown proceeds regardless.
func (s *Server) notifyEvictionComplete(ctx context.Context) {
	pmAddr, err := cluster.GetLeaderAddr(ctx, s.etcdCli)
	if err != nil {
		slog.Warn("ps: notifyEvictionComplete: cannot get PM address, skipping", "err", err)
		return
	}
	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck
	conn, err := pool.Get(pmAddr)
	if err != nil {
		slog.Warn("ps: notifyEvictionComplete: cannot connect to PM", "pm_addr", pmAddr, "err", err)
		return
	}
	evictCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := transport.NewPMClient(conn).EvictionComplete(evictCtx, s.cfg.NodeID); err != nil {
		slog.Warn("ps: notifyEvictionComplete: failed (non-fatal)", "node", s.cfg.NodeID, "err", err)
	} else {
		slog.Info("ps: notified PM of eviction completion", "node", s.cfg.NodeID)
	}
}

func (s *Server) shutdown() error {
	slog.Info("ps: shutdown initiated", "node", s.cfg.NodeID)

	// 1. Notify PM that we are beginning graceful shutdown (Active → Draining).
	drainCtx, drainCancel := context.WithTimeout(context.Background(), s.cfg.DrainTimeout)
	defer drainCancel()
	s.notifyDraining(drainCtx)

	// 2. Pre-migrate partitions to other nodes.
	s.drainPartitions(drainCtx)
	slog.Info("ps: drain complete", "node", s.cfg.NodeID)

	// 2b. Notify PM that drain is done (Draining → Drained). Non-fatal if PM is down.
	s.notifyDrained(drainCtx)

	// 3. gRPC: wait for in-flight RPCs to complete, then stop.
	s.grpcSrv.GracefulStop()
	slog.Info("ps: gRPC server stopped", "node", s.cfg.NodeID)

	// 4. Save checkpoints for all actors across every actor type.
	slog.Info("ps: evicting all actors", "node", s.cfg.NodeID)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	for _, d := range s.dispatchers {
		if err := d.EvictAll(shutdownCtx); err != nil {
			slog.Warn("ps: evictAll failed", "node", s.cfg.NodeID, "type", d.TypeID(), "err", err)
		}
	}

	// 4b. Signal PM that WAL is fully flushed so it can skip walFlushMargin.
	// Non-fatal: PM will fall back to the margin if this fails.
	s.notifyEvictionComplete(context.Background())

	slog.Info("ps: shutdown complete", "node", s.cfg.NodeID)
	// 5. Close the etcd client.
	return s.etcdCli.Close()
}

// drainPartitions requests migration of all partitions owned by this PS to other PS nodes.
func (s *Server) drainPartitions(ctx context.Context) {
	pmAddr, err := cluster.GetLeaderAddr(ctx, s.etcdCli)
	if err != nil {
		slog.Warn("ps: drain: cannot get PM address, skipping drain", "err", err)
		return
	}

	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck
	pmConn, err := pool.Get(pmAddr)
	if err != nil {
		slog.Warn("ps: drain: cannot connect to PM", "pm_addr", pmAddr, "err", err)
		return
	}
	pmClient := transport.NewPMClient(pmConn)

	members, err := pmClient.ListMembers(ctx)
	if err != nil {
		slog.Warn("ps: drain: cannot list members", "err", err)
		return
	}
	targets := make([]transport.MemberInfo, 0, len(members))
	for _, m := range members {
		if m.NodeID != s.cfg.NodeID && m.Status == domain.NodeStatusActive {
			targets = append(targets, m)
		}
	}
	if len(targets) == 0 {
		slog.Warn("ps: drain: no available target nodes, skipping drain")
		return
	}

	rt := s.routing.Load()
	if rt == nil {
		return
	}
	var i int
	for _, entry := range rt.Entries() {
		if entry.NodeID != s.cfg.NodeID {
			continue
		}
		target := targets[i%len(targets)]
		i++
		if err := pmClient.RequestMigrate(ctx, entry.Partition.ActorType, entry.Partition.ID, target.NodeID); err != nil {
			slog.Error("ps: drain: migrate failed",
				"partition", entry.Partition.ID, "target", target.NodeID, "err", err)
		} else {
			slog.Info("ps: drain: partition migrated",
				"partition", entry.Partition.ID, "target", target.NodeID)
		}
	}
}

// requestJoin calls PM's RequestJoin RPC to gain cluster admission.
// The node must be pre-registered with Waiting status via 'abctl node add'.
// It passes the set of actor types this PS supports so PM can validate them.
func (s *Server) requestJoin(ctx context.Context, pmAddr string) error {
	actorTypes := make([]string, 0, len(s.dispatchers))
	for t := range s.dispatchers {
		actorTypes = append(actorTypes, t)
	}
	sort.Strings(actorTypes)

	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck
	conn, err := pool.Get(pmAddr)
	if err != nil {
		return fmt.Errorf("connect to PM %s: %w", pmAddr, err)
	}
	return transport.NewPMClient(conn).RequestJoin(ctx, s.cfg.NodeID, s.cfg.Addr, actorTypes)
}

// notifyDrained calls PM's SetNodeDrained RPC after drainPartitions completes.
// Transitions the catalog state from Draining → Drained.
// Errors are only logged; the shutdown continues regardless.
func (s *Server) notifyDrained(ctx context.Context) {
	pmAddr, err := cluster.GetLeaderAddr(ctx, s.etcdCli)
	if err != nil {
		slog.Warn("ps: notifyDrained: cannot get PM address, skipping", "err", err)
		return
	}
	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck
	conn, err := pool.Get(pmAddr)
	if err != nil {
		slog.Warn("ps: notifyDrained: cannot connect to PM", "pm_addr", pmAddr, "err", err)
		return
	}
	if err := transport.NewPMClient(conn).SetNodeDrained(ctx, s.cfg.NodeID); err != nil {
		slog.Warn("ps: notifyDrained: SetNodeDrained failed (non-fatal)", "node", s.cfg.NodeID, "err", err)
	} else {
		slog.Info("ps: notified PM of drain completion", "node", s.cfg.NodeID)
	}
}

// notifyDraining calls PM's SetNodeDraining RPC before starting drainPartitions.
// Errors are only logged; the shutdown continues regardless.
func (s *Server) notifyDraining(ctx context.Context) {
	pmAddr, err := cluster.GetLeaderAddr(ctx, s.etcdCli)
	if err != nil {
		slog.Warn("ps: notifyDraining: cannot get PM address, skipping", "err", err)
		return
	}
	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck
	conn, err := pool.Get(pmAddr)
	if err != nil {
		slog.Warn("ps: notifyDraining: cannot connect to PM", "pm_addr", pmAddr, "err", err)
		return
	}
	if err := transport.NewPMClient(conn).SetNodeDraining(ctx, s.cfg.NodeID); err != nil {
		slog.Warn("ps: notifyDraining: SetNodeDraining failed", "node", s.cfg.NodeID, "err", err)
	} else {
		slog.Info("ps: notified PM of draining", "node", s.cfg.NodeID)
	}
}
