# StellMap

[English](./README.md) | [Simplified Chinese](./README_CN.md)

`StellMap` is the service registry in the `Stell Hub` ecosystem. Its Chinese name is `星轴` (Xingzhou).

Its responsibility is direct: provide unified service instance registration, discovery, and location capabilities for distributed systems, so callers can find target services at runtime and coordinate routing and governance through consistent metadata.

## Name Origin

### StellMap

The literal meaning of `StellMap` is a "star map" or a coordinate map of stars.

For a service registry, this name maps to its core role in the system:

- Each service instance is like a coordinate point on the star map.
- The registry maintains the current location and state of these coordinate points.
- Callers use this "star map" to complete service discovery, location, and navigation.

In other words, `StellMap` is not a simple address book. It is the unified coordinate system of the whole service ecosystem.

## Repository Positioning

This repository hosts the Go implementation of `StellMap`.

Following the responsibilities of a service registry, this repository will gradually evolve the following capabilities:

- Service registration
- Service discovery
- Instance heartbeat and health state maintenance
- Service metadata management
- Namespace and group isolation
- Collaboration interfaces with governance, configuration, control-plane, and related modules

## Documentation Navigation

The root directory keeps only this `README.md` as the project homepage. Detailed design, module documentation, and operations documents are collected under [`docs/README.md`](docs/README.md).

- Architecture design: [`docs/design/`](docs/design/)
- Module documentation: [`docs/modules/`](docs/modules/)
- Deployment and operations: [`docs/operations/`](docs/operations/)

## Design Goals

`StellMap` is not intended to be a large, all-purpose configuration or coordination system. It is designed as a lightweight, highly available, highly concurrent, and strongly consistent service registry.

- Consistency first: uses a `CP` architecture and prioritizes linearizability and correctness.
- Lightweight: a single Raft group can carry the whole registry data plane.
- High availability: serves traffic as long as a majority is alive.
- High concurrency: keeps read and write paths short and avoids unnecessary abstraction layers.
- Easy recovery: keeps the crash recovery path clear and storage responsibilities well bounded.
- Evolvable: can later extend watch, lease, compaction, layered cache, and related capabilities.

## Overall Architecture

In the current implementation, `stellmapd` consists of three listener surfaces: public `HTTP`, independent `admin HTTP`, and internal `gRPC`. The overall runtime relationship is shown below:

![StellMap architecture diagram](docs/images/architecture-overview.svg)

### Consensus Layer

The `StellMap` registry cluster uses a single `Raft` group and implements the replicated state machine through [`etcd-io/raft`](https://github.com/etcd-io/raft).

The reasons are straightforward:

- The core registry object is the service instance record.
- Metadata scale is usually much smaller than a general-purpose database.
- A single Raft group significantly reduces implementation complexity.
- For a service registry, consistency and maintainability are usually more important than horizontal sharding.

Single Raft does not mean a single point of failure. The actual deployment is still a multi-node replica cluster; the entire data space is simply managed by the same consensus group.

### Data Model

`StellMap` exposes an "instance registry" model instead of a general-purpose `KV` product.

- Logical primary key: `namespace / service / instanceId`
- Instance content: endpoint, labels, metadata, lease TTL, latest heartbeat time, and other registration information
- External semantics: callers use the system through "register instance, query candidates, renew lease, and watch changes"
- Internal implementation: data is still encoded into stable key-value records for replication, recovery, and snapshots

Where:

- `namespace` isolates environment, tenant, or region boundaries.
- `service` is the normalized full service name.
- A full service name supports multi-level organization: `organization.businessDomain.capabilityDomain.application.role`.
- Structured fields and the normalized service name are both retained, making prefix subscription, permission governance, and monitoring aggregation easier.

This design helps:

- Map directly to the instance-change apply flow after Raft commit.
- Simplify log replication, snapshot recovery, and instance expiration cleanup.
- Support registry capabilities such as querying candidate instances by service, filtering by labels, and watching event streams.

## Consistency Model

### CP Architecture

`StellMap` explicitly uses a `CP` architecture.

- When a network partition occurs, minority nodes stop serving linearizable writes.
- The cluster prioritizes preventing data split-brain, rollback, and stale reads.
- The system accepts writes only after a majority is alive and leader election completes.

This means that under extreme failures, the system prefers being temporarily unavailable for writes rather than sacrificing consistency for apparent availability.

### Read Requests Must Be Linearizable

All read requests must be linearizable. There is no default stale read mode.

Implementation path:

- External read requests enter `raftnode.LinearizableRead`.
- `LinearizableRead` internally requests a read barrier through `ReadIndex`.
- The local state machine is read only after `appliedIndex >= readIndex`.

Benefits:

- Does not depend on local time and does not introduce lease clock drift issues.
- Keeps the read path relatively short.
- Strictly satisfies the registry's requirement for the latest instance view.

If a request lands on a follower, `ReadIndex` still confirms leadership through the Raft path and then returns the linearizable result from the local state machine.

## Membership Changes

Node membership changes must support:

- `Learner`
- `Joint Consensus`

Specific strategy:

- A new node first joins as a `Learner`; it only receives logs and does not vote.
- After it catches up and passes required health checks, it is promoted to a voting member.
- Multi-node topology changes use `ConfChangeV2` and `Joint Consensus` to avoid quorum instability caused by direct switching.

This design ensures:

- Scale-out does not immediately affect quorum because of a slow node.
- Scale-in and node replacement have an explicit transition state for consensus configuration changes.
- The implementation stays aligned with the modern membership-change mechanism in `etcd-io/raft`.

## Storage Design

`StellMap` splits persistence responsibilities into three parts:

- `WAL`: stores the Raft log.
- `Pebble`: stores instance registry data and a small amount of local metadata.
- `Snapshot`: stores independent snapshot files.

The directory layout after responsibility separation looks like this:

```text
data/
  wal/
    0000000000000001.wal
    0000000000000002.wal
  pebble/
    MANIFEST-000001
    CURRENT
    *.sst
    *.log
  snapshot/
    snapshot-0000000000001234-0000000000005678.snap
    snapshot-0000000000001234-0000000000005678.meta
```

### 1. WAL: Raft Log

`WAL` is responsible only for the Raft replication log and required persistent consensus metadata, such as:

- `Entry`
- `HardState`
- Snapshot position reference information

This ensures:

- Clear Raft append-only semantics.
- Stable write patterns, which are friendly to batch fsync.
- Decoupling between consensus log and state machine data, making recovery easier to implement.

### 2. Pebble: Instance Registry Data + Small Metadata

`Pebble` does not store the full Raft log. It only stores:

- Applied instance registry data.
- A small amount of local metadata, such as the latest applied index / term, currently active snapshot metadata, and future small control metadata such as lease, compaction point, and watermark.

This avoids mixing Raft logs and registry data into a single engine and reduces coupling between compaction, recovery, and space management.

### 3. Snapshot: Independent Files

Snapshots are independent files instead of being directly mixed into Pebble, because:

- A snapshot is naturally a versioned state cut.
- Independent files make streaming transfer, verification, persistence, and atomic replacement easier.
- They make interrupted download recovery, file-level checksum, and historical cleanup easier.

A snapshot file usually contains:

- Snapshot metadata: term, index, conf state, checksum
- Instance registry data exported by the state machine
- Required extension metadata

## Why Pebble

`Pebble` is an open-source Go-native LSM `key-value` engine from `CockroachDB`.

Official resources:

- GitHub README: <https://github.com/cockroachdb/pebble>
- Go package documentation: <https://pkg.go.dev/github.com/cockroachdb/pebble>

The official documentation explicitly states that:

- Pebble is a `key-value store` inspired by `LevelDB/RocksDB`.
- It focuses on performance and is used in production at scale in `CockroachDB`.
- It provides a faster commit pipeline, better concurrency, and a smaller, more maintainable code baseline.

### Reasons for Using Pebble

For `StellMap`, Pebble is a good fit for the registry scenario:

- Go-native implementation with no `cgo` dependency.
- Strong sequential-write and read-amplification control, suitable for high-frequency registration and heartbeat updates.
- Supports range deletion, snapshots, and batch writes, which helps implement instance cleanup, snapshots, and batch apply.
- Mature engineering quality with long-term production use.
- Works well for structured but not overly complex data such as "instance registry records + small metadata".

### Advantages

- Go-native, friendly to deployment and cross-compilation.
- Strong write concurrency, suitable for high-concurrency metadata updates.
- LSM structure fits registry workloads with both high read and high write volume and clear key prefixes.
- Friendly to scanning instances by service, reading candidate sets before label filtering, and batch apply.
- Mature community and industry practice, with lower maintenance cost than a self-built storage engine.

### Drawbacks

- Still an LSM, so it introduces typical issues such as compaction, write amplification, and space amplification.
- Does not provide full relational query capabilities; complex retrieval requires upper-layer encoding design.
- Not suitable as the sole storage medium for the Raft log, otherwise log and state-machine lifecycles interfere with each other.
- Although it is compatible with parts of RocksDB formats, it is not a full replacement; migration must follow official compatibility boundaries.

### Comparison

| Option | Advantages | Drawbacks | Conclusion |
| --- | --- | --- | --- |
| `Pebble` | Go-native, fast, mature, suitable for registry data | Has compaction cost | Suitable as instance registry storage |
| `bbolt` | Simple implementation, single file, easy to understand | Single-writer model is obvious and unsuitable for high-concurrency writes | Not suitable for a high-concurrency registry |
| `Badger` | Go-native, complete KV capability | Value log and GC add operational complexity | Usable, but less direct and controllable for registry state management than Pebble |
| `RocksDB` | Mature ecosystem, rich capabilities | Depends on `cgo`; heavier operations and build chain | Too heavy for a lightweight Go project |

Therefore, `StellMap` chooses:

- `WAL` for the Raft log
- `Pebble` for instance registry data and small local metadata
- `Snapshot` for state-cut recovery

instead of pushing all three responsibilities into one storage component.

## Read and Write Flow

### Write Requests

1. The client sends a registration, deregistration, or heartbeat-renewal request through the `HTTP API`.
2. A non-leader node redirects or forwards the request to the leader.
3. The leader encodes the instance change command as a proposal and submits it to the single Raft group.
4. The log is appended to the local `WAL`.
5. The log is replicated to a majority and committed.
6. The state machine applies the command to `Pebble` in order.
7. A write success response is returned.

### Read Requests

1. The client sends an instance query request.
2. The request reaches the leader or is forwarded to the leader by a follower.
3. The leader executes `ReadIndex`.
4. The node waits until local `appliedIndex` catches up to `readIndex`.
5. The latest instance registry view is read from `Pebble`.
6. A linearizable result is returned.

## Communication Design

### External API

Only the public `HTTP API` is exposed for third-party business integrations.

The value of the external `HTTP API`:

- Lowers integration cost.
- Makes scripting, sidecar, and multi-language client calls convenient.
- Fits open interfaces such as registration, deregistration, query, and health reporting.

### Management API

Membership changes, leader transfer, and cluster status queries are not mounted on the public HTTP listener. They are mounted on an independent `admin HTTP` listener.

Design constraints:

- Public `HTTP` carries only the business data plane and health checks.
- `admin HTTP` carries only control-plane actions and binds to loopback by default, for example `127.0.0.1:18080`.
- `admin HTTP` currently additionally enforces requests to come only from `127.0.0.1`.
- `admin HTTP` requires fixed token authentication with request header `Authorization: Bearer <token>`.
- `stellmapctl` is the only control-plane entry point.
- This means the control plane currently supports local operations by default. Cross-machine operations require separately relaxing the source restriction and adding more complete authentication later.

### Internal Transport

Cluster-internal replication uses `gRPC`.

Reasons:

- Internal and external communication surfaces have different responsibilities and should not share the same protocol assumptions.
- The internal surface of a single Raft cluster must carry Raft messages, snapshots, leader forwarding, and related protocols.
- `gRPC` is a better fit for machine-to-machine internal communication, with clear protocol definitions, strong typing, and streaming snapshot transfer support.

Internal transport mainly carries:

- Raft message transfer
- Snapshot file/chunk transfer
- Read/write forwarding from follower to leader
- Node management and health probing

Strategy:

- The external surface exposes only the `HTTP API`.
- The internal replication surface uses `gRPC`.
- External HTTP and internal `gRPC` share the same core state machine and permission-checking logic to avoid duplicate implementations.

## Crash Recovery Flow

`StellMap` recovery must strictly follow the `Snapshot -> Pebble -> WAL Replay` order.

### Startup Recovery Steps

1. Read the latest local snapshot metadata.
2. If a valid snapshot exists, restore the snapshot file into the state machine working directory first.
3. Open `Pebble` and load instance registry data and local metadata.
4. Open `WAL` and read `HardState`, `Entry`, and snapshot position.
5. Discard old logs already covered by the snapshot index.
6. Apply the remaining unapplied committed entries to the state machine in order.
7. Rebuild the in-memory Raft node, apply watermarks, and membership view.
8. Enter a serviceable state externally.

### Crash-Point Handling Principles

- If WAL has been persisted but the state machine has not applied it yet, replay the log after restart.
- If a snapshot file has been generated but metadata has not been switched atomically, the old snapshot remains authoritative.
- If the applied index stored in Pebble lags behind the WAL committed index, continue applying.
- If snapshot, WAL, and state-machine indexes are inconsistent, prioritize "never roll back committed logs".

### Post-Recovery Checks

- `HardState.Commit >= appliedIndex`
- The state machine's `appliedIndex` does not exceed the largest committed index in WAL.
- `ConfState` in the snapshot is consistent with the recovered in-memory membership.
- Whether the current node is still in the latest membership.
- Whether it needs to continue pulling missing logs from the leader or install a new snapshot.

## Snapshot and Log Compaction

To prevent Raft log growth without bound, snapshots must be generated periodically and old logs truncated.

Basic strategy:

- Trigger a snapshot when `appliedIndex - snapshotIndex` exceeds the threshold.
- After snapshot generation, atomically write snapshot metadata.
- WAL keeps only necessary logs after the snapshot point.
- New or lagging nodes first try log catch-up and switch to snapshot installation when the gap is too large.

## Consistency and Fault Testing

`StellMap` must treat consistency verification and fault injection as core tests, not as extras.

### Consistency Tests

- Linearizable read test: after concurrent writes, all successful reads must observe the latest value that satisfies real-time ordering.
- Monotonic read test: repeated reads from the same client must not go backward.
- Read-your-writes test: after a write succeeds, later linearizable reads must see it.
- Instance candidate-set consistency test: query results must correspond to some linearizable point in time.
- Leader switch test: no acknowledged write may be lost before or after leader election.

### Membership Tests

- After joining, a `Learner` only synchronizes logs and does not vote.
- A `Learner` is promoted to Voter after catching up.
- Every phase of `Joint Consensus` preserves correct quorum semantics.
- Data and configuration remain consistent after node replacement, scale-in, or scale-out.

### Fault Tests

- Leader crashes and restarts.
- Follower crashes and restarts.
- Recovery-order tests after multi-node restart.
- Recovery strategy tests for WAL corruption, snapshot corruption, and partial file loss.
- Network partition tests: majority continues serving; minority rejects linearizable writes.
- Slow disk write / fsync jitter tests.
- Snapshot transfer interruption and resume/retry tests.

### Stress and Stability Tests

- High-frequency registration/deregistration stress tests.
- High-frequency heartbeat renewal stress tests.
- Large-volume instance query and watch stress tests.
- Long-running soak tests that observe compaction, file descriptors, memory, and tail latency.

## Module Layout

### `raftnode`

`raftnode` is the consensus core of the entire system. It drives `etcd-io/raft` into a runnable replicated state machine.

Main responsibilities:

- Initialize `raft.Config`, `MemoryStorage`, and persistent state.
- Drive tick, campaign, propose, step, and advance lifecycles.
- Process `Entries`, `CommittedEntries`, `Messages`, and `Snapshot` in `Ready` batches.
- Connect with `wal`, `snapshot`, and `storage`.
- Provide the `ReadIndex` capability required by linearizable reads.
- Handle `Learner`, `ConfChangeV2`, and `Joint Consensus`.

### `wal`

`wal` persists the Raft log and does not store registry data.

Main responsibilities:

- Append `Entry` sequentially.
- Persist `HardState`.
- Manage segment rotation, fsync, and truncation.
- Scan and recover usable log segments during startup.
- Provide WAL corruption detection and limited repair capabilities.

Core interface:

```go
type WAL interface {
    Open(ctx context.Context) error
    Append(ctx context.Context, state raftpb.HardState, entries []raftpb.Entry) error
    Load(ctx context.Context) (raftpb.HardState, []raftpb.Entry, error)
    TruncatePrefix(ctx context.Context, index uint64) error
    Sync(ctx context.Context) error
    Close(ctx context.Context) error
}
```

### `snapshot`

`snapshot` handles snapshot export, installation, verification, and switching.

Main responsibilities:

- Export snapshot files from the state machine.
- Maintain snapshot metadata: `term/index/conf_state/checksum`.
- Install remote snapshots and persist them atomically.
- Provide read/write support for snapshot streams.
- Control snapshot retention and cleanup policies.

Core interface:

```go
type SnapshotStore interface {
    Create(ctx context.Context, meta Metadata, exporter Exporter) (Metadata, error)
    OpenLatest(ctx context.Context) (Metadata, io.ReadCloser, error)
    Install(ctx context.Context, meta Metadata, r io.Reader) error
    Cleanup(ctx context.Context, keep int) error
}
```

### `storage`

`storage` uses `Pebble` to implement instance registry data and small metadata storage.

Main responsibilities:

- Maintain instance registration records.
- Maintain state-machine apply progress and local metadata.
- Provide low-level capabilities such as key reads, range scans, batch writes, and range deletion.
- Support snapshot export and snapshot restore.
- Guarantee apply idempotency and ordering.

Core interface:

```go
type StateMachine interface {
    Apply(ctx context.Context, cmd Command) (Result, error)
    Get(ctx context.Context, key []byte) ([]byte, error)
    Scan(ctx context.Context, start, end []byte, limit int) ([]KV, error)
    Snapshot(ctx context.Context, w io.Writer) error
    Restore(ctx context.Context, r io.Reader) error
    AppliedIndex(ctx context.Context) (uint64, error)
}
```

### `transport`

`transport` is split into the external `HTTP API` access surface and the internal `gRPC` replication surface.

Main responsibilities:

- Transfer Raft messages between internal nodes.
- Synchronize snapshots through streams.
- Forward reads and writes from follower to leader.
- Expose registration, discovery, and health interfaces to third-party clients.

Submodules:

- `transport/http`: public HTTP data plane and independent admin HTTP control plane
- `transport/grpc`: internal implementation for inter-node replication, forwarding, and snapshot synchronization

## Module Collaboration

`cmd/stellmapd/main.go` currently assembles three listener surfaces:

- Public `HTTP`: `httptransport.NewPublicServer(registryHandler, health)`, carrying `/api/v1`, `/internal/v1`, `/healthz`, `/readyz`, and `/metrics`
- Independent `admin HTTP`: `httptransport.NewAdminServer(control)`, wrapped by `adminAuthMiddleware`
- Internal `gRPC`: `grpctransport.NewServer(internalService).RegisterHandlers(grpcServer)`

Core call directions:

- `RegistryAPI` handles public registration/discovery APIs, internal replication watch, and Prometheus SD.
- `HealthAPI` handles health checks and metrics exposure.
- `ControlAPI` handles cluster status, replication status, membership changes, and leader transfer.
- `grpctransport.Server` only adapts protobuf servers; actual logic is executed by `runtime.InternalTransportService`.
- `runtime.PeerTransport` consumes `raftnode.Ready()`, sends `RaftMessageBatch` grouped by target node, and performs snapshot chunk transfer for `MsgSnap`.
- `raftnode` is the unified entry point: writes go through `ProposeCommand`, reads go through the linearizable `Get/Scan` path, and membership changes go through `ApplyConfChange`.

Constraints:

- `transport/http` and `transport/grpc` must not directly modify `Pebble`.
- Business writes must go through `raftnode.ProposeCommand`.
- Linearizable reads are implemented through `raftnode.LinearizableRead + storage.Get/Scan`.
- `wal` does not depend on `storage`.
- `snapshot` may call `storage` for export and restore, but must not be depended on by `raftnode` in reverse.

## Internal gRPC Protocol

The internal replication protocol is fixed by `api/proto/stellmap/v1/raft.proto` and `api/proto/stellmap/v1/snapshot.proto`. It is used only for inter-node communication and is not exposed to external business clients.

### Service Definitions

| Service | RPC | Direction | Description |
| --- | --- | --- | --- |
| `RaftTransport` | `Send(RaftMessageBatch) returns (RaftMessageAck)` | Unary | Sends ordinary `Raft` messages in batches |
| `SnapshotService` | `Install(stream InstallSnapshotChunk) returns (InstallSnapshotResponse)` | Client Streaming | Uploads snapshot chunks and installs at EOF |
| `SnapshotService` | `Download(DownloadSnapshotRequest) returns (stream DownloadSnapshotChunk)` | Server Streaming | Downloads a snapshot by `term/index` |

### Message Model

- `RaftEnvelope`: fields are `from`, `to`, and `payload`; `payload` is the serialized `raftpb.Message`.
- `RaftMessageBatch`: a batch of `RaftEnvelope` messages for the same target node.
- `SnapshotMetadata`: contains `term`, `index`, `conf_state`, `checksum`, and `file_size`.
- `SnapshotChunk`: contains `metadata`, `data`, `offset`, and `eof`.

### Implementation Mapping

- [internal/transport/grpc/server.go](internal/transport/grpc/server.go) registers both `RaftTransport` and `SnapshotService`, and converts protobuf types to local `RaftMessageBatch` / `SnapshotChunk`.
- [internal/runtime/transport_service.go](internal/runtime/transport_service.go) contains `InternalTransportService`, which performs the actual message handling: `SendRaftMessages` calls `node.Step`, `InstallSnapshotChunk` aggregates chunks in memory by `term-index`, and `DownloadSnapshot` returns chunks from the local snapshot store.
- [internal/runtime/peer_transport.go](internal/runtime/peer_transport.go) groups `Ready.Messages` by target node. Ordinary messages use `Client.Send`; `MsgSnap` uses `splitSnapshotChunks + Client.InstallSnapshot`.

![StellMap gRPC flow diagram](docs/images/grpc-flow.svg)

## Control Plane Design

Membership changes, leader transfer, and cluster status inspection are all triggered by `stellmapctl`, while the underlying execution path still goes through the independent `admin HTTP` listener of `stellmapd`.

Current control-plane boundary:

- Public business clients access only `HTTPAddr`.
- `stellmapctl` accesses local `AdminAddr` by default.
- `admin` requests must satisfy both conditions: source address is `127.0.0.1` and the request carries `Authorization: Bearer <token>`.
- `PeerAdminAddrs` is used only for leader following and control-plane status display; it does not accept direct remote access.

Control-plane routes:

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/admin/v1/status` | Returns cluster status from the current node's perspective |
| `GET` | `/admin/v1/replication/status` | Returns current replication task status |
| `POST` | `/admin/v1/members/add-learner` | Adds a learner |
| `POST` | `/admin/v1/members/promote` | Promotes a learner |
| `POST` | `/admin/v1/members/remove` | Removes a member |
| `POST` | `/admin/v1/leader/transfer` | Triggers active leader transfer |

Common commands:

- `stellmapctl member add-learner`
- `stellmapctl member promote`
- `stellmapctl member remove`
- `stellmapctl leader transfer`
- `stellmapctl status`

## HTTP API

The `HTTPAddr` listener of `StellMap` carries three classes of routes:

- Public registration and discovery data plane: `/api/v1`
- Internal replication and monitoring helper interfaces: `/internal/v1`
- Health checks and metrics: `/healthz`, `/readyz`, `/metrics`

The independent `AdminAddr` carries only the `/admin/v1` control plane.

![StellMap HTTP flow diagram](docs/images/http-flow.svg)

### Public Registration and Discovery APIs

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/api/v1/registry/register` | Registers an instance; a non-leader returns `503 not_leader` |
| `POST` | `/api/v1/registry/deregister` | Deregisters an instance; a non-leader returns `503 not_leader` |
| `POST` | `/api/v1/registry/heartbeat` | Renews an instance; first performs a linearizable read of the current instance, then submits an update |
| `GET` | `/api/v1/registry/instances` | Queries instance candidates with linearizable semantics |
| `GET` | `/api/v1/registry/watch` | Pushes `snapshot` / `upsert` / `delete` events through SSE |

### Internal HTTP APIs

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/internal/v1/replication/watch` | SSE dedicated to cross-region directory synchronization; requires `Bearer <replication_token>` |
| `GET` | `/internal/v1/prometheus/sd` | Prometheus HTTP SD; requires `Bearer <prometheus_sd_token>` |

Behavior of `/internal/v1/replication/watch`:

- Supports filter parameters such as `namespace`, `service`, `zone`, `endpoint`, `selector`, and `label`.
- Supports `sinceRevision`; if the local replay buffer contains the requested range, events are replayed directly, otherwise a `snapshot` is sent first.
- Returns `text/event-stream`.

Behavior of `/internal/v1/prometheus/sd`:

- Returns Prometheus-compatible target group JSON.
- Supports `namespace`, `service`, `zone`, `endpoint`, `scope`, `includeSelf`, `selector`, and `label`.
- `scope` currently supports `local` and `merged`.
- The default `endpoint` is `metrics`.

Minimal Prometheus integration example:

```yaml
scrape_configs:
  - job_name: stellmap-services
    http_sd_configs:
      - url: http://10.0.0.11:8080/internal/v1/prometheus/sd?endpoint=metrics
        refresh_interval: 30s
        authorization:
          type: Bearer
          credentials: stellmap
```

Example response:

```json
[
  {
    "targets": ["10.0.0.11:9090"],
    "labels": {
      "namespace": "prod",
      "service": "order-service",
      "instance_id": "order-1",
      "region": "cn-sh",
      "zone": "az1",
      "cluster_id": "100",
      "target_kind": "service_instance",
      "__scheme__": "http",
      "__metrics_path__": "/metrics"
    }
  }
]
```

### Health and Debug APIs

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/healthz` | Returns process liveness state and current `leaderId` / `leaderAddr` |
| `GET` | `/readyz` | Returns ready when the node has started, is not stopping, and has a serviceable `Raft` state |
| `GET` | `/metrics` | Exposes Prometheus text metrics |

### Registration Model

The current registry models "one instance" as:

- Stable identity: `namespace`, `service`, `instanceId`
- Structured service identity: `organization`, `businessDomain`, `capabilityDomain`, `application`, `role`
- Instance attributes: `zone`, `labels`, `metadata`
- Protocol entries: `endpoints[]`
- Lease attribute: `leaseTtlSeconds`

Field conventions:

- `namespace`: stable business isolation domain, such as `prod`, `staging`, or `tenant-a`
- `service`: normalized service name, such as `company.trade.order.order-center.api`
- `organization`: organization identifier, such as `company`
- `businessDomain`: business domain, such as `trade`
- `capabilityDomain`: capability domain, such as `order`
- `application`: application name, such as `order-center`
- `role`: application role, such as `api` or `worker`
- `instanceId`: unique instance identifier
- `zone`: availability zone where the instance is located, such as `az1`
- `labels`: low-cardinality governance labels, such as `color=gray` or `version=v2`
- `metadata`: additional descriptive information, such as `build_sha=abc123`
- `endpoints[].name`: endpoint name inside the instance; when empty, the server fills it with `protocol`
- `endpoints[].protocol`: endpoint protocol, such as `http`, `grpc`, or `tcp`
- `endpoints[].host` / `endpoints[].port`: protocol entry address
- `endpoints[].path`: optional path, commonly used for `HTTP` endpoints such as `metrics`
- `endpoints[].weight`: endpoint weight; when omitted, the server defaults it to `100`
- `leaseTtlSeconds`: instance lease TTL; when omitted or set to `0`, the server defaults it to `30`

Multi-level service identity conventions:

- `service` must be consistent with `organization.businessDomain.capabilityDomain.application.role`.
- If the request body does not explicitly provide `service`, the server automatically combines the five structured fields into the normalized service name.
- If only `service` is provided, the server reverse-parses the structured fields.
- All five segments must be complete; skipped levels are not supported, for example only providing `organization` and `application`.

Registration request example:

```json
{
  "namespace": "prod",
  "organization": "company",
  "businessDomain": "trade",
  "capabilityDomain": "order",
  "application": "order-center",
  "role": "api",
  "service": "company.trade.order.order-center.api",
  "instanceId": "order-center-api-10.0.1.23",
  "zone": "az1",
  "labels": {
    "color": "gray",
    "version": "v2"
  },
  "metadata": {
    "build_sha": "abc123",
    "owner": "trade-team"
  },
  "endpoints": [
    {
      "name": "http",
      "protocol": "http",
      "host": "10.0.1.23",
      "port": 8080,
      "weight": 100
    },
    {
      "name": "metrics",
      "protocol": "http",
      "host": "10.0.1.23",
      "port": 8080,
      "path": "/metrics",
      "weight": 100
    },
    {
      "name": "grpc",
      "protocol": "grpc",
      "host": "10.0.1.23",
      "port": 9090,
      "weight": 100
    }
  ],
  "leaseTtlSeconds": 30
}
```

Heartbeat request example:

```json
{
  "namespace": "prod",
  "service": "company.trade.order.order-center.api",
  "instanceId": "order-center-api-10.0.1.23",
  "leaseTtlSeconds": 30
}
```

Instance query examples:

```text
GET /api/v1/registry/instances?namespace=prod&service=company.trade.order.order-center.api&zone=az1&endpoint=http&selector=color=gray,version%20in%20(v2),!deprecated
GET /api/v1/registry/instances?namespace=prod&servicePrefix=company.trade.order&endpoint=http
GET /api/v1/registry/instances?namespace=prod&organization=company&businessDomain=trade&capabilityDomain=order
```

Instance watch examples:

```text
GET /api/v1/registry/watch?namespace=prod&service=company.trade.order.order-center.api&selector=color=gray,version%20in%20(v2)
GET /api/v1/registry/watch?namespace=prod&servicePrefix=company.trade.order&includeSnapshot=true
GET /api/v1/registry/watch?namespace=prod&servicePrefix=company.trade.order&sinceRevision=1024&includeSnapshot=false
```

Watch conventions:

- Returns `text/event-stream`.
- Sends a `snapshot` event with the full current candidate set after the connection is established.
- Sends `upsert` when an instance is added or updated.
- Sends `delete` when an instance is removed or no longer matches the current filters.
- Every event carries `revision`, which corresponds to the underlying committed log index.
- `sinceRevision` is the last directory version successfully processed by the client; the server tries to replay incremental events after this version if they are still in the local buffer window.
- When `includeSnapshot=true`, the server first sends a `snapshot` if `sinceRevision` cannot be recovered or this is the first connection.
- When `includeSnapshot=false` and `sinceRevision` is already outside the local retention window, the server returns `410 revision_expired`.
- For watch, `revision` is an event-stream recovery cursor, not a large-file resume offset.

Query conventions:

- `namespace`: required.
- `service`: optional, represents a normalized full service name; can be repeated for multiple `service` values.
- `servicePrefix`: optional and repeatable; matches prefixes of normalized service names, for example `company.trade.order`.
- `organization`, `businessDomain`, `capabilityDomain`, `application`, `role`: optional. If all five segments are provided, they are equivalent to an exact `service`; if only a continuous prefix of segments is provided, it is automatically converted into a `servicePrefix`.
- `zone`: optional.
- `endpoint`: optional; matches endpoint name or protocol name.
- `selector`: optional. Supports `key`, `!key`, `key=value`, `key!=value`, `key in (v1,v2)`, and `key notin (v1,v2)`.
- `label`: compatible legacy parameter. It can be repeated and has the format `label=key=value`.
- `limit`: optional. It only limits the final returned candidate set size.

Selector integration notes:

- A single `selector` parameter can combine multiple conditions with top-level commas, for example `selector=color=gray,version in (v2),!deprecated`.
- Multiple `selector` parameters can also be repeated. The server merges all expressions with `AND`.
- `in/notin` must use parentheses, for example `version in (v2,v3)` or `env notin (test,dev)`.
- The compatible `label` parameter is suitable for old SDK migration, for example `label=color=gray&label=version=v2`.

Examples:

```text
GET /api/v1/registry/instances?namespace=prod&service=company.trade.order.order-center.api&selector=color=gray,version%20in%20(v2),!deprecated
GET /api/v1/registry/instances?namespace=prod&servicePrefix=company.trade.order&selector=env%20in%20(prod,staging)&selector=tier=core
GET /api/v1/registry/instances?namespace=prod&organization=company&businessDomain=trade&capabilityDomain=order&label=color=gray&label=version=v2
```

Common invalid examples:

```text
selector=version in v2
selector=color=
selector=color=gray,,version=v2
```

For such invalid input, the server returns `400 bad_request` and includes supported syntax and examples in `message`, so SDKs can pass through or map the error directly. Example:

```json
{
  "code": "bad_request",
  "message": "invalid selector \"version in v2\": expected values like (v1,v2); supported selector syntax: key, !key, key=value, key!=value, key in (v1,v2), key notin (v1,v2); valid example: selector=color=gray,version in (v2),!deprecated; invalid example: selector=version in v2"
}
```

Current boundaries:

- The server only filters candidate sets and does not perform final weighted instance selection.
- Weight is returned with endpoint information and is left to the client SDK, gateway, or sidecar for local traffic decisions.
- The query API skips instances whose lease TTL has expired and has not been renewed.
- Expired instances are truly removed from storage by the leader through periodic background scans and `Raft delete proposal`, instead of only being hidden during queries.
- The background scan interval can be adjusted through `--registry-cleanup-interval`.
- The per-scan background deletion limit can be adjusted through `--registry-cleanup-delete-limit`.

### Configuration

`stellmapd` currently starts through a `TOML` configuration file. Command-line arguments only override individual fields in the configuration file.

Precedence:

1. Command-line arguments
2. The `TOML` configuration file specified by `--config`
3. Remaining fields must be explicitly provided, otherwise startup fails

Startup:

```bash
./stellmapd --config=./config/stellmapd.toml
```

The configuration template used by the installation script is:

- [config/stellmapd.toml](config/stellmapd.toml)

Notes:

- [config/stellmapd.toml](config/stellmapd.toml) is currently the placeholder template used by the installation script.
- When starting manually, use the following "actually usable" configuration content as a reference and fill in your own node parameters.

Minimal example:

```toml
[node]
id = 1
cluster_id = 100
region = "default"
data_dir = "data/node-1"

[server]
http_addr = "0.0.0.0:8080"
admin_addr = "127.0.0.1:18080"
grpc_addr = "0.0.0.0:19090"

[auth]
admin_token = "stellmap"
replication_token = "stellmap"
prometheus_sd_token = "stellmap"

[cluster]
peer_ids = "1,2,3"
peer_grpc_addrs = "1=10.0.0.11:19090,2=10.0.0.12:19090,3=10.0.0.13:19090"
peer_http_addrs = "1=10.0.0.11:8080,2=10.0.0.12:8080,3=10.0.0.13:8080"
peer_admin_addrs = "1=127.0.0.1:18080,2=127.0.0.1:18080,3=127.0.0.1:18080"

[runtime]
request_timeout = "5s"
shutdown_timeout = "10s"

[registry]
cleanup_interval = "1s"
cleanup_delete_limit = 128

[replication]
targets_file = "/etc/stellmapd/stellmapd-node-1-replication-targets.json"
```

Notes:

- `admin_token`, `replication_token`, and `prometheus_sd_token` can now all be placed directly in the configuration file.
- Command-line arguments can still override the configuration file, for example:

```bash
./stellmapd --config=./config/stellmapd.toml --http-addr=:28080 --admin-token=new-token
```

### HTTP Response Model

The unified response format is:

```json
{
  "code": "ok",
  "message": "",
  "data": {},
  "requestId": "01HR..."
}
```

Typical response codes include:

- `ok`
- `bad_request`
- `not_found`
- `not_ready`
- `not_leader`
- `unauthorized`
- `forbidden`
- `read_failed`
- `scan_failed`
- `propose_failed`
- `conf_change_failed`

Exceptions:

- `/api/v1/registry/watch` and `/internal/v1/replication/watch` return `text/event-stream`.
- `/internal/v1/prometheus/sd` directly returns the target group JSON required by Prometheus and does not wrap it in the unified response structure.

## Crash Recovery Template

The recovery flow can be further solidified into an implementation checklist. Each later version can use this template for self-checks.

### Main Recovery Flow Template

```text
[Bootstrap]
1. load config
2. lock data dir
3. inspect snapshot dir
4. open pebble
5. open wal
6. rebuild raft state
7. replay committed entries
8. publish local status
9. join cluster service loop
```

### Recovery Checkpoint Template

| Check | Expected | Failure Handling |
| --- | --- | --- |
| Latest snapshot metadata is readable | Can get `term/index/conf_state` | Mark snapshot as corrupted, fall back to an older snapshot, or enter manual repair mode |
| Pebble can be opened | Registry data directory is complete | Abort startup to avoid serving with damaged data |
| WAL can be scanned | At least `HardState` and usable `Entry` can be recovered | Attempt repair; if repair fails, refuse startup |
| Applied index is legal | `appliedIndex <= commitIndex` | Refuse startup and alert |
| ConfState is consistent | Snapshot/WAL/in-memory membership views are consistent | Enter recovery-only mode and do not serve externally |
| Current node role is legal | The node is still in the latest membership | A non-member node becomes read-only or exits |

### Recovery Drill Template

| Scenario ID | Scenario | Initial Condition | Injection | Expected Result |
| --- | --- | --- | --- | --- |
| `REC-001` | WAL persisted but registry change not yet applied | Single cluster running normally | `kill -9` immediately after a registration request is committed | Restart replays logs successfully, with no committed data loss |
| `REC-002` | Snapshot file write interrupted | Snapshot generation triggered | Power failure halfway through snapshot write | Startup detects the bad snapshot and falls back to the previous valid snapshot |
| `REC-003` | Follower lags for a long time | 3-node cluster | Block one follower's network and then recover | Small gap uses log catch-up; large gap uses snapshot installation |
| `REC-004` | Node restarts after being removed from membership | Remove node completed | Restart the removed node | The node must not rejoin as a voter |

## Test Case Checklist Template

Future versions can maintain the test matrix under `tests/` following this template.

### 1. Consistency Test Template

| Case ID | Name | Coverage | Prerequisite | Steps | Expected Result |
| --- | --- | --- | --- | --- | --- |
| `CONS-001` | Linearizable instance registration and query | `ReadIndex` | 3 nodes normal | Register/renew continuously and query immediately | Reads latest committed instance view |
| `CONS-002` | Concurrent write then read | Real-time ordering | 3 nodes normal | Multiple clients write concurrently, then read concurrently | No stale reads |
| `CONS-003` | Read consistency during leader switch | Leader/follower switch | Trigger one leader change | Continue reads and writes before and after the switch | Acknowledged writes are not lost and reads do not go backward |

### 2. Membership Test Template

| Case ID | Name | Coverage | Prerequisite | Steps | Expected Result |
| --- | --- | --- | --- | --- | --- |
| `MEM-001` | Learner join | Learner synchronization | 3 nodes normal | Add learner | Learner does not vote but can catch up logs |
| `MEM-002` | Learner promotion | Promote | Learner has caught up | Trigger promote | New node becomes voter |
| `MEM-003` | Joint consensus scale-in | `ConfChangeV2` | 5 nodes normal | Remove 2 nodes in one change | Both transition and final phases satisfy quorum rules |

### 3. Fault Test Template

| Case ID | Name | Coverage | Prerequisite | Fault Injection | Expected Result |
| --- | --- | --- | --- | --- | --- |
| `FAULT-001` | Leader crash | Election recovery | 3 nodes normal | Kill current leader | New leader is elected and the cluster resumes write service |
| `FAULT-002` | Minority network partition | CP semantics | 5 nodes normal | Disconnect 2 nodes | Majority continues serving, minority rejects linearizable writes |
| `FAULT-003` | WAL corruption | Recovery strategy | Construct corrupted segment | Start node | Node refuses to serve with damaged data or enters repair flow |

### 4. Stress Test Template

| Case ID | Name | Metrics | Load Model | Expected |
| --- | --- | --- | --- | --- |
| `LOAD-001` | Registration write stress test | QPS, P99, fsync latency | High-frequency `Register` | Stable latency, no abnormal error rate |
| `LOAD-002` | Heartbeat renewal stress test | QPS, P99, compaction frequency | High-frequency `Heartbeat` | No obvious write amplification runaway |
| `LOAD-003` | Service discovery query stress test | scan latency, heap | Large volume of `QueryInstances` / `Watch` | Controlled read latency, no obvious memory leak |

### 5. Automated Execution

- `tests/integration`: basic functional regression
- `tests/consistency`: linearizability, leader switch, and read/write ordering verification
- `tests/fault`: kill, partition, disk fault, and snapshot fault tests
- CI runs lightweight integration tests first; nightly jobs run fault and soak tests

## Current Status

The repository is currently in the initialization stage. The following will be added gradually:

- Server implementation
- Client SDK
- API definitions
- Storage and recovery modules
- Consistency tests and fault-injection tests
- Deployment and example projects

## Module

```go
module github.com/stellhub/stellmap
```

## Reference

- <https://thesecretlivesofdata.com/raft>
