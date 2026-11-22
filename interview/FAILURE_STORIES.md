# GeoStream — Failure Stories & Debugging Scenarios (`FS-GEO-01`..`FS-GEO-20`)

Grounded in real chaos suite runs ([`evidence/reconciliation/chaos-matrix.json`](../evidence/reconciliation/chaos-matrix.json), `run_chaos_180024`), isolated backpressure profiling (`run-bp-001`), live alert drills (`run_alert_drill`), and actual implementation/QA defects fixed during development.

---

### `FS-GEO-01`: Concurrent Go Test Migration Race (`SQLSTATE 23505`)
- **Symptom**: Running `go test -race -count=1 ./internal/... ./api/... ./tests/...` intermittently failed inside `PostgresStore.Migrate` with `duplicate key value violates unique constraint "pg_type_typname_nsp_index" (SQLSTATE 23505)`.
- **Root Cause**: Go executes test packages (`api`, `tests/integration`, `tests/chaos`) in parallel processes targeting the same `geostream` database; concurrent `CREATE TABLE IF NOT EXISTS` statements race inside PostgreSQL's system catalog.
- **Fix**: Wrapped migration execution in `SELECT pg_advisory_xact_lock(727001)` inside [`internal/storage/postgres.go`](../internal/storage/postgres.go) (`Migrate`).
- **Verification**: Concurrent `-race` package runs pass consistently (`api`, `tests/integration`, `tests/chaos`).

### `FS-GEO-02`: Shared Kafka Topic History Replaying Old Runs (`run-local-001`)
- **Symptom**: Launching a new `run-local-001` simulator with a fresh consumer group caused initial consumption lag and unexpected stale historical records before reading new events.
- **Root Cause**: Creating a fresh consumer group defaulting to earliest offset (`AtStart`) on a reused `courier.gps.raw` topic forced workers to scan tens of thousands of leftover messages from `run-smoke-001` and chaos suites.
- **Fix**: Added [`SeekGroupToEnd`](../internal/kafka/client.go) (and `KAFKA_SEEK_END_ON_START=true` for worker containers) before starting new benchmark producers while preserving offset resume for crash-recovery consumer groups.
- **Verification**: `run-local-001` consumed `17,723 / 17,723` generated events cleanly with `0` stale counts and `reconcile pass: true`.

### `FS-GEO-03`: Warm-Path PostgreSQL `LoadMembership` Saturation (`500 ms -> 50 ms` p99)
- **Symptom**: Initial `run-local-001` throughput benchmark showed `PIPELINE_TRANSITION p99 = 500 ms` and leftover `outboxPending` rows despite fast in-memory geofence evaluation.
- **Root Cause**: `ProcessRecord` queried PostgreSQL (`PG.LoadMembership`) on *every* incoming ping even when Redis (`p.Redis.Read`) already held warm device fence state (`foundInRedis == true`), saturating the local PG connection pool.
- **Fix**: Restricted `PG.LoadMembership` strictly to `!foundInRedis` cache misses (`internal/pipeline/pipeline.go:136`), reconstructing `FenceMembership` directly from `curState.InsideFenceIDs` on cache hits.
- **Verification**: Re-running `run-local-001` cut `PIPELINE_TRANSITION p99` from `500 ms` to `50 ms` and drained `outboxPending` to `0 / 3,000`.

### `FS-GEO-04`: Bare Coordinate Arrays from PG Crashing `/map` Route in Playwright
- **Symptom**: During the first live Playwright run, `04-map-replay-1440.png` captured a blank screen and the subsequent `Transitions` navigation timed out after 45s.
- **Root Cause**: `UpsertGeofences` stored `f.RawGeo` as raw polygon ring arrays (`[[[lon, lat], ...]]`), which `PostgresStore.ListGeofences` emitted verbatim as `"geometry": [...]`. [`MapReplayPage.tsx`](../web/src/routes/MapReplayPage.tsx) accessed `(f.geometry.coordinates)[0]`, throwing an uncaught `TypeError` as soon as `/geofences` resolved and unmounting the React tree.
- **Fix**: Normalized array JSONB into GeoJSON `{"type":"Polygon","coordinates":...}` inside `ListGeofences` (`jsonb_typeof(geometry) = 'array'`) AND added defensive `Array.isArray` ring parsing in `MapReplayPage.tsx`.
- **Verification**: `04-map-replay-1440.png` renders all 20 geofence polygons (including `fence-001`'s cutout hole) and 100 sampled pins with `0` console errors.

### `FS-GEO-05`: Mobile `390x844` Axe WCAG `scrollable-region-focusable` Violation
- **Symptom**: Playwright/Axe WCAG 2.2 AA check failed on `mobile-map-390` flagging `.gs-table-wrap` (`<div class="gs-table-wrap">`) for lacking keyboard focusability on Safari/keyboard navigation.
- **Root Cause**: Horizontal table scroll wrappers overflowed cleanly inside `max-width: 100%` without `tabIndex={0}`, blocking keyboard users from scrolling clipped table columns.
- **Fix**: Added `tabIndex={0} role="region" aria-label="..."` across all `.gs-table-wrap` containers and the `<pre>` alert JSON box across all 5 route components (`OverviewPage`, `MapReplayPage`, `DiagnosticsPage`, `TransitionsPage`, `EvidencePage`).
- **Verification**: Playwright + Axe WCAG 2.2 AA passed `0` violations across `1440x900`, `1024x768`, `390x844`, and `720x450` (~200% zoom).

### `FS-GEO-06`: `cmd/alertdrill` Non-Idempotent Device Key Across Repeated Runs
- **Symptom**: Running `scripts/run_live_drill_and_qa.ps1` a third time in one session panicked with `No alert reached firing state` in `cmd/alertdrill`.
- **Root Cause**: Reusing fixed `dev := "courier-alert-001"` on `run_alert_drill` collided with Redis `LastSeq = 8` retained from prior drills, classifying fresh `seq=1` and `seq=8` pings as `STALE`/`DUPLICATE`, while immediate produce occasionally raced fresh worker consumer rebalance.
- **Fix**: Updated [`cmd/alertdrill/main.go`](../cmd/alertdrill/main.go) to sleep 3s for group stabilization and synthesize unique `courier-alert-<unix>` device IDs per drill.
- **Verification**: `run_alert_drill` reliably fires (`18:07:28.207Z`) and recovers (`18:08:25.266Z`) across repeated runs.

### `FS-GEO-07`: Step 5 -> Step 6 Worker Crash Recovery (`5_crash_step5_to_6_replay`)
- **Symptom**: Worker crashes cleanly via `FaultHooks.AfterPGCommit` after PostgreSQL commits the transition + outbox row, **before** Redis Lua CAS runs and before Kafka offset commits.
- **Mechanism Tested**: On restart/redelivery, Step 2 reads stale/missing Redis sequence (`pgLastTransitionSeq` hydrated from `LoadMembership`); Step 3 cold-replay safety checks `HasTransitionForSeq`, and Step 5 `PersistTransitions` handles `pgx.ErrNoRows` via explicit `SELECT ... WHERE transition_id = $1` consistency check before Step 6 advances Redis and Step 7 commits Kafka offset.
- **Evidence**: `vertical-slice.json` (`step5To6CrashPGCount: 1`, `redisRecoveredInside: ["fence-004"]`) and `chaos-matrix.json` (`pgRows: 1`, `redisSeq: 1`, `reconcilePass: true`, `PASS`).

### `FS-GEO-08`: Step 6 -> Step 7 Crash Before Kafka Offset Commit (`6_crash_step6_to_7_replay`)
- **Symptom**: Worker crashes via `FaultHooks.AfterRedisAdvance` after both PG and Redis committed, right before Kafka offset commit.
- **Mechanism Tested**: Broker redelivers the identical event; Step 3 sees `ev.Sequence == curState.LastSeq` with matching `LastEventID` and `PayloadHash`, increments `EventsDuplicate`, and safely commits Kafka offset without re-inserting PG transitions.
- **Evidence**: `chaos-matrix.json` scenario `6_crash_step6_to_7_replay` (`pgRows: 1`, `redisSeq: 1`, `status: "PASS"`).

### `FS-GEO-09`: At-Least-Once Exact Duplicate Delivery (`1_duplicate_delivery`)
- **Symptom**: Upstream producer or broker partition leader failover delivers the same `eventId`, `sequence`, and payload twice.
- **Mechanism Tested**: Redis `compareAdvanceLua` returns `ResultDuplicate`; `ProcessRecord` increments `EventsDuplicate` and suppresses transition evaluation.
- **Evidence**: `chaos-matrix.json` (`pgRows: 1`, `redisSeq: 1`, `status: "PASS"`).

### `FS-GEO-10`: Out-of-Order Stale Delivery (`2_out_of_order_stale`)
- **Symptom**: Delayed packet arrives with `sequence < effectiveFloorSeq` (e.g., `seq=1` after `seq=2` is already committed).
- **Mechanism Tested**: Step 3 rejects `ev.Sequence < effectiveFloorSeq` before mutating Redis coordinates or re-evaluating geofence state.
- **Evidence**: `chaos-matrix.json` (`pgRows: 1`, `redisSeq: 2`, `status: "PASS"`).

### `FS-GEO-11`: Sequence Conflict Mutation Quarantined to DLQ (`3_sequence_conflict_dlq`)
- **Symptom**: Malformed/reset device SDK reuses `seq=1` with different GPS coordinates (`PayloadHash` mismatch).
- **Mechanism Tested**: `ProcessRecord` detects `ev.Sequence == curState.LastSeq` with `curState.PayloadHash != payloadHash`, avoids overwriting Redis or PG, and publishes the record with `CONFLICT:equal_seq_different_payload` to `pipeline.deadletter`.
- **Evidence**: `chaos-matrix.json` (`expectedCategory: "CONFLICT->DLQ"`, `pgRows: 0`, `redisSeq: 1`, `status: "PASS"`).

### `FS-GEO-12`: Poison Payload & Coordinate Bounds Quarantine (`4_poison_invalid_payload`)
- **Symptom**: Corrupt JSON or invalid latitude (`95.5 > 90`) enters `courier.gps.raw`.
- **Mechanism Tested**: `ValidateLocationEvent` returns `ErrInvalidEvent`; `ProcessRecord` blocks Kafka offset commit until `publishDLQ` receives synchronous broker ack on `pipeline.deadletter`.
- **Evidence**: `chaos-matrix.json` (`pgRows: 0`, `redisSeq: 0`, `status: "PASS"`).

### `FS-GEO-13`: Outbox Crash Between Kafka Publish and `MarkOutboxPublished` (`7_outbox_crash_before_mark`)
- **Symptom**: Outbox relay publishes `GeofenceTransitionV1` to `geofence.enter` / `geofence.exit`, then crashes via `FaultHooks.AfterOutboxPublish` before executing `MarkOutboxPublished`.
- **Mechanism Tested**: Subsequent `PublishPendingOutbox` poll re-emits the same deterministic `transitionId` (idempotent for downstream consumers) and marks `published_at = NOW()`.
- **Evidence**: `chaos-matrix.json` (`outboxPending: 0`, `status: "PASS"`).

### `FS-GEO-14`: Redis Cache Eviction & Cold Recovery (`8_redis_eviction_cold_recovery`)
- **Symptom**: Redis restarts or evicts `geo:<runId>:device:*` after TTL expiration while a courier is still physically inside a geofence (`fence-004`).
- **Mechanism Tested**: On next ping outside the fence, `!foundInRedis` triggers `PG.LoadMembership` hydration, recovering `inside=true` so the worker emits the accurate `EXIT` transition rather than missing it.
- **Evidence**: `chaos-matrix.json` (`pgRows: 2`, `redisSeq: 3`, `status: "PASS"`).

### `FS-GEO-15`: Transient PostgreSQL Outage Shielding Redis (`9_transient_db_outage_retry`)
- **Symptom**: PG pool returns transient timeout during Step 5 while Kafka keeps pushing events.
- **Mechanism Tested**: Because Step 5 precedes Step 6, `BeforeDBWrite` failure aborts before `Redis.CompareAdvance` runs, leaving Redis unadvanced and Kafka offset uncommitted; when PG recovers, replay succeeds cleanly with `1` transition.
- **Evidence**: `chaos-matrix.json` (`pgRows: 1`, `redisSeq: 1`, `status: "PASS"`).

### `FS-GEO-16`: Monotonic Sequence Gap Detection & Reconciliation Alert (`10_sequence_gap_reconcile`)
- **Symptom**: Network partition drops `seq=2` on disk/producer so worker receives `seq=1` followed directly by `seq=3`.
- **Mechanism Tested**: `ProcessRecord` increments `SequenceGaps` counter and checkpoint gap count; `reconciliation.AuditRun` sets `Pass: false` (`sequence_gap_count = 1`), and `GeoStreamSequenceGapsDetected` fires critically in Prometheus/Alertmanager.
- **Evidence**: `chaos-matrix.json` (`reconcilePass: false` and `status: "PASS"` validating gap detection) + `evidence/alerts/alert-drill.json`.

### `FS-GEO-17`: Unbounded Queue Backpressure Collapse (`run-bp-001`)
- **Symptom**: Unbuffered/unbounded producer loop overwhelms a contention-limited `SimulatedDBSink` pool, building queue depth to `2,288` items and doubling peak Go heap (`9.46 MB` vs `3.99 MB`) with `p99 = 8468.44 ms`.
- **Mitigation**: Fixed-capacity channel (`256` in benchmark, `4096` in worker) + worker pool + `PauseFetchTopics` caps queue depth and reduces `p99` by `>99%` (`20.32 ms`) while persisting identical `2,400 == 2,400` outputs.
- **Evidence**: [`evidence/backpressure/run-bp-001/comparison.json`](../evidence/backpressure/run-bp-001/comparison.json) & `*.pb.gz` pprof profiles.

### `FS-GEO-18`: Prometheus Outage in Observability UI (`03-metrics-unavailable-1440.png`)
- **Symptom**: Prometheus container is stopped or unreachable while an operator inspects a run on `:5301/overview`.
- **Mitigation**: `handleTimeseries` returns `{status: "unavailable", points: []}` and `OverviewPage.tsx` renders an explicit warning banner (`Metrics temporarily unavailable (no fabricated fallback points)`) alongside DB-verified KPI cards instead of flat-lining `0/s`.
- **Evidence**: [`evidence/ui/screenshots/03-metrics-unavailable-1440.png`](../evidence/ui/screenshots/03-metrics-unavailable-1440.png) & `App.test.tsx`.

### `FS-GEO-19`: Graceful Kubernetes `SIGTERM` During Active Ingest Window
- **Symptom**: Rolling deployment or spot-node eviction sends `SIGTERM` while bounded worker goroutines hold uncommitted records in flight.
- **Behavior**: K8s manifest configures `terminationGracePeriodSeconds: 30` and `PodDisruptionBudget` (`minAvailable: 1`); context cancellation stops `kgo` fetching new batches, drains in-flight Step 5->6 DB/Redis transactions, flushes offset commits, and closes cleanly before SIGKILL. (Verified statically via `deploy/k8s/geostream.yaml`; live multi-pod K8s drain `BLOCKED_EXTERNAL` without cluster.)

### `FS-GEO-20`: Hot Kafka Partition Skew & Worker Rebalance Stalls
- **Symptom**: One high-volume depot or dense `deviceId` prefix hashes disproportionately onto a single partition (`partition 1` lagging behind `0..15`), hitting `GeoStreamKafkaLagHigh` (`max(geostream_kafka_lag) > 500`, once per-partition lag updates are wired from `InspectTopicOffsets` into Prometheus) even while average CPU looks low.
- **Diagnosis & Mitigation**: Inspect `/api/v1/runs/{id}/reconciliation` offset breakdown (`DiagnosticsPage.tsx`) alongside `Metrics.KafkaLag` (`geostream_kafka_lag` defined in telemetry/alert rules; currently left `0` at runtime until periodic offset polling wires `InspectTopicOffsets` -> `KafkaLag`) to distinguish single-partition skew from cluster starvation; pause-resume prevents single hot-partition slowdowns from starving peer partitions sharing a worker process once worker goroutines fan out per partition key, while long-term mitigation splits hot logical tenants via higher partition counts (`>16`).

