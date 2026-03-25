package transport

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sangchul/actorbase/provider"
)

// ServerConfig holds common gRPC server configuration.
type ServerConfig struct {
	ListenAddr string
	Metrics    provider.Metrics
}

// NewGRPCServer returns a *grpc.Server with common interceptors applied.
//
// Applied interceptors (unary + stream):
//   - Panic recovery: goroutine panics are converted to INTERNAL status.
//   - Request logging: method, duration, and status are recorded via slog.
//   - Metrics collection: request count, error count, and latency histogram
//     (only when Metrics is non-nil).
func NewGRPCServer(cfg ServerConfig) *grpc.Server {
	return grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			panicRecoverUnary(),
			loggingUnary(),
			metricsUnary(cfg.Metrics),
		),
		grpc.ChainStreamInterceptor(
			panicRecoverStream(),
			loggingStream(),
			metricsStream(cfg.Metrics),
		),
	)
}

// ── Unary Interceptors ───────────────────────────────────────────────────────

func panicRecoverUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("grpc unary panic", "method", info.FullMethod, "panic", r)
				err = status.Errorf(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

func loggingUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		slog.Debug("grpc unary",
			"method", info.FullMethod,
			"duration", time.Since(start),
			"err", err,
		)
		return resp, err
	}
}

func metricsUnary(m provider.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		if m != nil {
			dur := time.Since(start).Seconds()
			m.Counter("grpc_requests_total", "method").Inc(info.FullMethod)
			if err != nil {
				m.Counter("grpc_errors_total", "method").Inc(info.FullMethod)
			}
			m.Histogram("grpc_duration_seconds", "method").Observe(dur, info.FullMethod)
		}
		return resp, err
	}
}

// ── Stream Interceptors ──────────────────────────────────────────────────────

func panicRecoverStream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("grpc stream panic", "method", info.FullMethod, "panic", r)
				err = status.Errorf(codes.Internal, "internal error")
			}
		}()
		return handler(srv, ss)
	}
}

func loggingStream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		slog.Debug("grpc stream",
			"method", info.FullMethod,
			"duration", time.Since(start),
			"err", err,
		)
		return err
	}
}

func metricsStream(m provider.Metrics) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		if m != nil {
			dur := time.Since(start).Seconds()
			m.Counter("grpc_streams_total", "method").Inc(info.FullMethod)
			if err != nil {
				m.Counter("grpc_stream_errors_total", "method").Inc(info.FullMethod)
			}
			m.Histogram("grpc_stream_duration_seconds", "method").Observe(dur, info.FullMethod)
		}
		return err
	}
}
