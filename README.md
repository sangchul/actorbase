# actorbase

> There's an old saying in software: *"Don't reinvent the wheel."*
> In the age of AI-assisted development, that advice has changed:
> **"Build the wheel that fits you perfectly."**

A Go framework for building distributed, actor-based stateful services — where each shard of data is a single-threaded actor that knows how to split itself under load.

---

## The Problem

Scaling a stateful service typically means bolting sharding logic on top of a general-purpose database. The result is operational complexity spread across multiple layers: the database, the sharding middleware, and the application itself.

actorbase takes a different approach. **The application logic and the partition are the same thing.** Each shard is an actor — a plain Go struct — that holds its own state, processes requests single-threadedly, and knows how to serialize itself for replication, migration, and splitting.

The platform handles the hard distributed systems problems:
- WAL and checkpointing for durability
- Partition splitting and migration across nodes
- Automatic failover when a node dies
- Load-based rebalancing via a pluggable policy

You bring the business logic.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│ Client                                                          │
│   SDK Client ──WatchRouting──▶ PM ──etcd── PS-1, PS-2, PS-3   │
│   SDK Client ──Send(key)────▶ PS (routed by key range)         │
└─────────────────────────────────────────────────────────────────┘

PM  (Partition Manager) — cluster brain: routing table, split, migrate, failover
PS  (Partition Server)  — runs Actor instances; your binary, your Actor
SDK                     — client library: watches routing, routes requests, retries
```

Each partition maps to a **key range** and lives on one PS node. When a partition gets too hot, it splits in two. When nodes are imbalanced, partitions migrate. When a node dies, partitions fail over — all automatically, or on demand via `abctl`.

**Key design decisions:**
- Actor = single goroutine. No locks inside your actor code.
- WAL group commit — IO never blocks the actor goroutine.
- Checkpoint + WAL replay = full state recovery after any crash.
- Split key is determined by the actor itself (via `SplitHinter`) or by the platform (key range midpoint). Policy only decides *when* to split.

---

## Features

| Feature | Description |
|---|---|
| **Actor model** | Each partition is a typed `Actor[Req, Resp]`. Single-threaded, no locking needed inside actor code. |
| **WAL group commit** | Write-ahead log with batched IO. Actor goroutine never waits on disk. |
| **Checkpoint & replay** | Periodic snapshots + WAL replay. Full state recovery on restart or failover. |
| **Partition split** | Hot partitions split into two. Split key decided by actor (`SplitHinter`) or midpoint fallback. |
| **Migration** | Partitions move across nodes with zero data loss via shared checkpoint store. |
| **Automatic failover** | PM detects dead nodes via etcd lease expiry and reroutes partitions automatically. |
| **Graceful drain** | On SIGTERM, PS migrates its partitions before shutting down. |
| **Auto balancer** | Pluggable `BalancePolicy` drives split/migrate decisions. Built-in: `ThresholdPolicy`, `RelativePolicy`. |
| **Runtime policy** | Apply, inspect, or clear balance policy at runtime via `abctl policy apply`. |
| **Multi actor type** | A single PS binary can host multiple actor types (e.g. `bucket` + `object`). |
| **SDK client** | Watches routing table live. Retries on `ErrPartitionBusy` / `ErrPartitionMoved`. |

---

## What You Implement

actorbase is a framework. You provide the actor logic; the platform handles everything else.

### Required: the `Actor` interface

```go
type Actor[Req, Resp any] interface {
    // Process a request. Return walEntry=nil for read-only ops (no WAL write).
    Receive(ctx provider.Context, req Req) (resp Resp, walEntry []byte, err error)

    // Apply one WAL entry to actor state. Used during recovery.
    Replay(entry []byte) error

    // Serialize full actor state for checkpointing.
    Snapshot() ([]byte, error)

    // Restore actor state from a Snapshot.
    Restore(data []byte) error

    // Split state at splitKey. Return the upper half; remove it from self.
    Split(splitKey string) (upperHalf []byte, err error)
}
```

### Optional: tuning interfaces

```go
// Countable — lets the auto balancer see how many keys this actor holds.
type Countable interface {
    KeyCount() int64
}

// SplitHinter — actor proposes where to split (e.g. hotspot key).
// If not implemented, the platform uses the key range midpoint.
// Called inside the actor goroutine — no synchronization needed.
type SplitHinter interface {
    SplitHint() string
}
```

### Required: storage backends

```go
// WALStore — append-only log (implement with Redis Streams, Kafka, local files, …)
type WALStore interface {
    AppendBatch(ctx context.Context, partitionID string, data [][]byte) ([]uint64, error)
    ReadFrom(ctx context.Context, partitionID string, fromLSN uint64) ([]WALEntry, error)
    TrimBefore(ctx context.Context, partitionID string, lsn uint64) error
}

// CheckpointStore — snapshot store (implement with S3, GCS, shared filesystem, …)
type CheckpointStore interface {
    Save(ctx context.Context, partitionID string, data []byte) error
    Load(ctx context.Context, partitionID string) ([]byte, error)
    Delete(ctx context.Context, partitionID string) error
}
```

`adapter/fs` ships a local-filesystem implementation of both. Useful for single-machine deployments and testing.

### Optional: balance policy

```go
type BalancePolicy interface {
    Evaluate(ctx context.Context, stats ClusterStats) []BalanceAction
    OnNodeJoined(ctx context.Context, node NodeInfo, stats ClusterStats) []BalanceAction
    OnNodeLeft(ctx context.Context, node NodeInfo, reason NodeLeaveReason, stats ClusterStats) []BalanceAction
}
```

Or use the built-in YAML-driven policies:

```yaml
# threshold: split when RPS or key count exceeds an absolute value
algorithm: threshold
check_interval: 30s
cooldown:
  global: 60s
  partition: 120s
split:
  rps_threshold: 1000
  key_threshold: 10000
balance:
  max_partition_diff: 2
  rps_imbalance_pct: 30
```

```yaml
# relative: split when a partition's RPS exceeds N× the cluster average
algorithm: relative
check_interval: 30s
split:
  rps_multiplier: 3.0
  min_avg_rps: 10.0
balance:
  max_partition_diff: 2
  rps_imbalance_pct: 30
```

Apply at runtime: `abctl policy apply policy.yaml`

---

## Quickstart

```bash
# 1. Build
go build -o bin/pm        ./cmd/pm
go build -o bin/abctl     ./cmd/abctl
go build -o bin/kv_server ./examples/kv_server   # example PS binary
go build -o bin/kv_client ./examples/kv_client

# 2. Start etcd
etcd &

# 3. Start PM
./bin/pm -addr :8000 -etcd localhost:2379 -actor-types kv

# 4. Start PS(s)  — all share the same WAL and checkpoint directories
./bin/kv_server -node-id ps-1 -addr :8001 -etcd localhost:2379 \
  -wal-dir /tmp/actorbase/wal -checkpoint-dir /tmp/actorbase/checkpoint

# 5. Use the client
./bin/kv_client -pm localhost:8000 set hello world
./bin/kv_client -pm localhost:8000 get hello

# 6. Inspect the cluster
./bin/abctl -pm localhost:8000 members
./bin/abctl -pm localhost:8000 routing
```

See `examples/kv_server` for a complete Partition Server, and `examples/s3_server` for a multi-actor-type example (bucket metadata + object metadata as separate actor types on the same PS).

---

## Repository Layout

```
cmd/
  pm/          — Partition Manager binary
  abctl/       — CLI for cluster management
provider/      — Public interfaces: Actor, WALStore, CheckpointStore, BalancePolicy, …
policy/        — Built-in balance policies: ThresholdPolicy, RelativePolicy, NoopPolicy
ps/            — Partition Server assembly (multi-actor-type, gRPC handlers)
pm/            — Partition Manager assembly (routing, balancer, failover)
sdk/           — Client library
internal/
  engine/      — Actor lifecycle: mailbox, WAL group commit, checkpoint, split
  cluster/     — etcd: node registry, routing table store, policy store
  transport/   — gRPC: proto, server/client factories, connection pool
  rebalance/   — Split and migration orchestration
adapter/
  fs/          — Filesystem WALStore + CheckpointStore
  json/        — JSON Codec
examples/
  kv_server/   — Key-value PS: single actor type
  kv_client/   — CLI client
  kv_stress/   — Load generator
  kv_longrun/  — Long-running load + correctness verifier
  s3_server/   — S3 metadata PS: bucket + object actor types
test/
  integration/ — Automated integration scenarios 1–7 (~2.5 min)
  longrun/     — 8-minute chaos test with correctness verification
```

---

## Comparison: actorbase vs HBase

With Redis Streams as WALStore and HDFS as CheckpointStore, actorbase and HBase share the same core recovery path: when a node dies, another node replays the WAL from the shared store and restores state. Neither system replicates data within a partition — both delegate durability to the underlying store.

**Similarities**

| Concept | HBase | actorbase |
|---|---|---|
| Partition | Region | Actor |
| Partition server | RegionServer | PS |
| Cluster brain | HMaster | PM |
| Coordination | ZooKeeper | etcd |
| WAL | HLog on HDFS | Redis Streams / Kafka / filesystem |
| Snapshot | HFile flush to HDFS | HDFS CheckpointStore |
| Failover | WAL replay by another RegionServer | WAL replay by another PS |

**Key differences**

| | actorbase | HBase |
|---|---|---|
| Business logic | Lives inside the Actor (server-side) | Client-side or Coprocessor |
| Data model | Arbitrary Go struct | Fixed: row × column family × version |
| Split key | Decided by the Actor (`SplitHinter`) or midpoint | HMaster or midpoint |
| Range scan | Not supported (future work) | Native |
| Merge / compaction | Not supported | Minor/major compaction, region merge |
| Proven scale | Unverified beyond millions of partitions | Billions of rows, hundreds of nodes |

**When to choose actorbase over HBase**

- Your workload is point-access (key-based), not range-scan-heavy
- Business logic belongs inside the partition — e.g. session state, object metadata, game rooms
- You want a simpler operational stack without HBase expertise
- A moderate partition count (up to low millions) is sufficient

**When HBase is still the right choice**

- Range scan is required
- You need billions of rows at proven production scale
- Compaction and region merge are necessary for long-term storage efficiency
- Your team relies on the Hadoop ecosystem (Phoenix, Spark, Hive)

See [`doc/design/hbase-comparison.md`](doc/design/hbase-comparison.md) for a detailed analysis.

---

## Requirements

- Go 1.21+
- etcd v3.6+
