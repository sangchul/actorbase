## Build & Test Commands

```bash
 Unit tests
go test ./...

# Integration tests (requires etcd running)
bash test/integration/run.sh               # all 20 scenarios
bash test/integration/run.sh 5             # single scenario
bash test/longrun/run.sh                   # 8-min chaos correctness test
# NOTE: longrun test requires etcd pre-started: bash test/run.etcd.sh
```

## Architecture Overview

This is a **platform/framework** for building distributed actor-based key-value stores. It is NOT an application — actor implementations belong in `examples/`, never in `cmd/` or `internal/`.

```
SDK (client routing) → PM (cluster brain) → PS (partition host) → Engine (actor lifecycle)
                              ↕                     ↕
                        etcd (coordination)    WALStore / CheckpointStore
```

- **`provider/`** — all user-facing interfaces: `Actor[Req,Resp]`, `WALStore`, `CheckpointStore`, `BalancePolicy`, `Codec`, `Metrics`
- **`internal/domain/`** — core data models: `Partition`, `KeyRange`, `RouteEntry`, `NodeInfo`, `NodeStatus` (4 states: Waiting/Active/Draining/Failed)
- **`internal/engine/`** — `ActorHost[Req,Resp]`: single-threaded mailbox per partition, group-commit WAL flusher, checkpoint/recovery, split/merge export-import
- **`internal/cluster/`** — etcd-backed stores: `NodeRegistry` (TTL heartbeat), `NodeCatalog` (authoritative node state), `RoutingStore`, `PolicyStore`
- **`internal/transport/`** — gRPC client/server; `PMClient`, `PSClient` with connection pooling
- **`internal/rebalance/`** — `Splitter`, `Merger`, `Migrator` orchestration logic
- **`ps/`** — `ServerBuilder` + `Register[Req,Resp]()` multi-actor-type host; handles RequestJoin/SetNodeDraining lifecycle
- **`pm/`** — cluster brain: routing table management, node lifecycle (4-state), rebalance scheduling, PM HA via etcd election
- **`sdk/`** — `Client[Req,Resp]`: watches routing table, routes by key, handles failover and fan-out Scan
- **`adapter/`** — pluggable implementations of provider interfaces
- **`policy/`** — `ThresholdPolicy`, `RelativePolicy`, `NoopPolicy`
- **`examples/`** — reference implementations (kv_server, s3_server); the only place for concrete actor code

## Key Design Rules

**Platform neutrality**: `cmd/`, `internal/`, `ps/`, `pm/`, `sdk/` must not contain any domain-specific actor logic. All user-defined behavior goes in `examples/`.

**adapter/fs is test-only**: `adapter/fs` WAL and CheckpointStore are for unit/integration tests only — never production. Use `adapter/redis` (WAL) or `adapter/s3` (checkpoint) for production deployments.
