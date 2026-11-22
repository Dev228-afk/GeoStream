# GeoStream — System Design Prompts (`SD-GEO-01`..`SD-GEO-06`)

---

## `SD-GEO-01`: Multi-Region `40K` Active / `80K` Events/Sec Geofence Ingestion Pipeline
- **Scope & Constraints**: Ingest `80,000` GPS pings/s (`40,000` active devices pinging every 0.5–1s) with `< 12 ms` `INGEST_INTERNAL` p99 (`TARGET`), per-device sequence ordering, and zero dropped geofence `ENTER`/`EXIT` transitions.
- **High-Level Architecture**:
  - Edge ALB -> stateless ingest gateway -> Kafka (`32–64` partitions on `courier.gps.raw`, keyed strictly by `deviceId` so one device maps deterministically to one partition).
  - Stateless Go worker pods (`geostream-worker`, HPA on consumer lag + CPU `70%`) reading partitions via `franz-go`, maintaining hot in-memory GeoJSON polygon catalog (`internal/geofence/engine.go`), checking Redis cluster (`geo:<runId>:device:<deviceId>`, 24h TTL), and writing transitions + `outbox` to PostgreSQL primary (connection pooled via `pgxpool`/PgBouncer).
- **Back-of-the-Envelope Sizing**:
  - `80K ev/s * ~250 B JSON` ≈ `20 MB/s` raw ingress (`~60 MB/s` RF=3 Kafka replication bandwidth).
  - Transition rate at `~1–5%` of pings = `800–4,000` PG rows/s (instead of `80,000` raw ping writes/s).
  - Redis working set: `40,000 * ~350 B` ≈ `14 MB` active keyspace; sharded easily across 1–3 Redis nodes.
- **Bottleneck & Failure Modes**: Hot partitions (depot bursts), PG connection starvation -> isolated via bounded channel + `PauseFetchTopics`, consumer rebalance duplicates -> mitigated via Step 5 PG unique key + Step 6 Lua CAS.

---

## `SD-GEO-02`: Crash-Safe Dual-Store State Machine (Kafka + PostgreSQL + Redis)
- **Scope & Constraints**: Guarantee zero lost `ENTER`/`EXIT` transitions across crashes anywhere between Step 1 and Step 7, *without* distributed 2PC XA locks.
- **Core Ordering (`internal/pipeline/pipeline.go`)**:
  1. Step 1: Validate payload & bounds; quarantine invalid packets to `pipeline.deadletter` and await ack before advancing offset.
  2. Step 2–3: Read Redis sequence/hash (`Read`); on cold miss hydrate from PG `device_fence_state` (`LoadMembership`). Enforce `STALE`, `DUPLICATE`, and `CONFLICT` (`EqualSeq + MismatchedPayloadHash -> DLQ`).
  3. Step 4–5: Evaluate transitions; if any `ENTER`/`EXIT`, execute atomic PG transaction writing `geofence_transitions`, `device_fence_state`, and `outbox` first (`HasTransitionForSeq` guards cold Step 3 replays, while `PersistTransitions` runs an inline `SELECT ... WHERE transition_id = $1` consistency check on `pgx.ErrNoRows`).
  4. Step 6: Advance Redis via `compareAdvanceLua` only after PG commits.
  5. Step 7: Commit Kafka offset; background outbox relay publishes `geofence.enter` / `geofence.exit` and executes `MarkOutboxPublished`.
- **Every Crash Window Proven (`evidence/reconciliation/chaos-matrix.json`)**:
  - Crash Step 5->6: Replay sees PG already committed; idempotent retry repairs Redis (`5_crash_step5_to_6_replay`).
  - Crash Step 6->7: Replay triggers `DUPLICATE` in Redis Lua CAS (`6_crash_step6_to_7_replay`).
  - Redis eviction: Cold miss falls back to PG `device_fence_state` (`8_redis_eviction_cold_recovery`).

---

## `SD-GEO-03`: Designing Bounded Backpressure for Bursty Downstream Latency
- **Scope & Constraints**: Prevent OOM kills and GC tail collapse when PostgreSQL latency degrades `10x` during peak delivery bursts, while preserving `100%` of valid transitions.
- **Architecture**:
  - Partition fetchers push records into a bounded channel (`make(chan queuedRecord, cap)` — `256` in `bench/compare`, `4096` in `geostream-worker`).
  - Fixed worker pool drains channel into `PersistTransitions` bounded by `pgxpool` max connections (`16`–`32`).
  - High/low watermarks (`80%` / `40%`) invoke `client.PauseFetchTopics` / `client.ResumeFetchTopics` (`RunBoundedWorker`) so unconsumed spikes queue durably on Kafka broker disk instead of Go pod heap.
- **Measured Proof (`run-bp-001`)**: Capping queue depth at `256` held peak heap at `3.99 MB`, `0.00 ms` GC pause, and `20.32 ms` p99 (`13,932.79 ev/s`), whereas unbounded queuing blew up to `2,288` queued items, `9.46 MB` heap, and `8,468.44 ms` p99 (`276.95 ev/s`).

---

## `SD-GEO-04`: Low-Latency Spatial Engine with Dynamic Versioned Geofences
- **Scope & Constraints**: Evaluate `20`–`5,000` polygons with concave cutout holes (`fence-001`), strict boundary determinism, and noisy GPS coordinates inside `< 12 ms` `INGEST_INTERNAL`.
- **Design**:
  - Broadcast/load active `geofences (id, version, geometry, bbox)` per version epoch; index bounding boxes (current `20`-fence grid uses precomputed `orb.Bound` AABB pruning in O(N), scaling directly to R-tree/H3 hex spatial pre-filter at `>500` fences).
  - Exact containment (`polygonContainsWithBoundary`): AABB check -> `pointOnRingBoundary` (`boundaryEpsilon = 1e-10`) -> outer `planar.RingContains` minus inner holes `rings[1:]`.
  - GPS jitter smoothing: `confirmDwell` holds candidate state changes until sustained past `dwellMS`; transitions tag exact `(fenceId, fenceVersion)` in `device_fence_state` so schema evolution rolls cleanly without invalidating active deliveries.

---

## `SD-GEO-05`: High-Cardinality Real-Time Monitoring UI & BFF (`40K` Entities -> `<=100` DOM Pins)
- **Scope & Constraints**: Provide live situational awareness (`Overview`, `Map & Replay`, `Diagnostics`, `Transitions`, `Evidence`) without rendering `40,000` DOM nodes or hammering Prometheus/Postgres from multiple browser tabs.
- **Architecture**:
  - Go BFF (`api/server.go`) pre-aggregates summary stats from `runs` + Redis `ZCARD` (`ActiveDeviceCount`) and caps device queries (`limit <= 100`).
  - `GET /runs/{id}/stream` streams lightweight `<=1 Hz` JSON summary frames (`activeDeviceCount`, timestamp) over SSE—never per-ping telemetry.
  - UI decouples live Prometheus queries (failing honestly to `Metrics temporarily unavailable`) from durable Postgres ledger panels, pairs SVG maps with accessible tables (`MapReplayPage.tsx`), and paginates transitions by compound cursor (`event_time, transition_id`).

---

## `SD-GEO-06`: Partial Outage Reconciliation, Sequence-Gap Detection & Safe Replay
- **Scope & Constraints**: Detect silent producer gaps, broker offset holes, or bad geofence deploys and recover exact downstream state safely.
- **Design**:
  - Worker tracks `lastSeenSeq` per `(runId, deviceId)` and increments `geostream_sequence_gaps_total` (while chaos tests and admin checkpoint callers pass `SequenceGapCount` into `UpsertCheckpoint` on `partition_checkpoints`; the standard runtime worker currently logs `0` to `partition_checkpoints` until periodic gap persistence is wired); Alertmanager triggers `GeoStreamSequenceGapsDetected` (`increase(geostream_sequence_gaps_total[1m]) > 0`) as `critical`.
  - Offline/on-demand `cmd/reconcile` (`AuditRun`) cross-checks generator manifests vs. Kafka topic watermarks (`InspectTopicOffsets`) vs. Postgres transitions & pending outbox vs. Redis active ZSET cardinality (`reconcile --run-id`).
  - `cmd/replay` replays fixed partitions/offsets or quarantines fixed `pipeline.deadletter` records through idempotent `ProcessRecord` (`CONFLICT`/`DUPLICATE` guards guarantee safe double-replay without duplicate downstream alerts).

