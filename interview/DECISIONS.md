# GeoStream — Architecture Decision Records (`DEC-GEO-001`..`DEC-GEO-014`)

Every decision below maps directly to verified files and symbols in [`geostream/`](../README.md).

---

### `DEC-GEO-001`: PostgreSQL Transaction Before Redis CAS Before Kafka Commit
- **Context**: Dual-writing Kafka consumer state to Redis (hot cache) and PostgreSQL (durable audit ledger) creates crash windows where Redis reflects a sequence advance while PostgreSQL rolls back or crashes.
- **Decision**: Enforce the strict 8-step ordering inside [`internal/pipeline/pipeline.go`](../internal/pipeline/pipeline.go) (`Processor.ProcessRecord`): Step 5 commits PostgreSQL (`PersistTransitions`: `geofence_transitions`, `device_fence_state`, `outbox`) **first**; Step 6 executes Redis `CompareAdvance` **second**; Step 7 commits Kafka offsets and runs `PublishPendingOutbox` **third**.
- **Alternatives Rejected**: Redis-first with async DB worker (drops transitions if worker crashes after Step 6), distributed XA two-phase commit (unsupported across Redis and Kafka).
- **Evidence**: [`tests/integration/vertical_slice_test.go`](../tests/integration/vertical_slice_test.go) (`TestGate1VerticalSliceAndStep5CrashRecovery` -> [`evidence/reconciliation/vertical-slice.json`](../evidence/reconciliation/vertical-slice.json)) and chaos scenarios `5_crash_step5_to_6_replay` & `6_crash_step6_to_7_replay` in [`evidence/reconciliation/chaos-matrix.json`](../evidence/reconciliation/chaos-matrix.json).

---

### `DEC-GEO-002`: `franz-go` (`kgo`) Over `sarama` / `confluent-kafka-go`
- **Context**: GeoStream needs partition-level backpressure (`PauseFetchTopics`/`ResumeFetchTopics`), clean cooperative rebalance hooks, and native `CGO_ENABLED=0` static Alpine Docker builds without librdkafka C dependencies.
- **Decision**: Standardize on `github.com/twmb/franz-go/pkg/kgo` inside [`internal/kafka/client.go`](../internal/kafka/client.go) and [`internal/pipeline/pipeline.go`](../internal/pipeline/pipeline.go).
- **Alternatives Rejected**: `confluent-kafka-go` (requires CGO + `librdkafka` in Alpine containers), `segmentio/kafka-go` (weaker consumer group pause/resume ergonomics under dynamic partition assignment).
- **Evidence**: Clean `Dockerfile` multi-stage `CGO_ENABLED=0` build and `PauseFetchTopics`/`ResumeFetchTopics` backpressure in `RunBoundedWorker`.

---

### `DEC-GEO-003`: Atomic Redis Lua CAS (`compareAdvanceLua`) Instead of MULTI/EXEC Watch
- **Context**: Concurrent replays or partition handoffs can race `GET` + `SET` on `geo:<runId>:device:<deviceId>`, allowing stale sequences or silent payload mutations to corrupt latest state.
- **Decision**: Implement a single-round-trip Lua script [`compareAdvanceLua`](../internal/redisstate/store.go) returning `"ACCEPTED"`, `"DUPLICATE"`, `"STALE"`, or `"CONFLICT"`, paired with `EXPIRE` (`24h` TTL) and bounded `ZADD` active-device tracking on `ActiveKey` (`geo:<runId>:active`).
- **Alternatives Rejected**: Client-side `WATCH`/`MULTI`/`EXEC` optimistic retry loops (higher RTT variance under contention), PostgreSQL row-locking per ping (saturates DB IOPS).
- **Evidence**: `TestRedisAtomicLuaCompareAdvanceAndConcurrency` passing under `go test -race`.

---

### `DEC-GEO-004`: Warm-Path Redis Cache Skipping PostgreSQL `LoadMembership`
- **Context**: Early benchmarking (`run-local-001`) revealed `PIPELINE_TRANSITION p99 = 500 ms` when calling `PG.LoadMembership` on every warm ping even when Redis already held `InsideFenceIDs`.
- **Decision**: Query `PG.LoadMembership` strictly on `!foundInRedis` cold misses (`internal/pipeline/pipeline.go`); hydrate membership from `curState.InsideFenceIDs` and `curState.FenceVersion` when Redis hits.
- **Alternatives Rejected**: Unconditional PG read-before-write on every ping (turns Redis into write-only overhead).
- **Evidence**: Warm-path optimization cut `run-local-001` `PIPELINE_TRANSITION p99` from `500 ms` -> `50 ms` and dropped `outboxPending` from non-zero backlog to `0`.

---

### `DEC-GEO-005`: Conflict Detection (`Equal Sequence + Different PayloadHash` -> DLQ)
- **Context**: At-least-once Kafka redelivery legitimately replays identical events, whereas buggy producers or clock resets reuse an old `sequence` with altered coordinates (`Lat`/`Lon`).
- **Decision**: Compute canonical SHA-256 [`CanonicalEventPayloadHash`](../internal/schema/validate.go) and compare both `LastEventID` and `PayloadHash` inside `compareAdvanceLua` and `ProcessRecord`; classify mismatches as `ErrConflict` and route to `pipeline.deadletter`.
- **Alternatives Rejected**: Blind last-write-wins overwrite (corrupts audit history), silent drop (hides producer sequence bugs).
- **Evidence**: Chaos scenario `3_sequence_conflict_dlq` (`PASS`) in [`tests/chaos/chaos_test.go`](../tests/chaos/chaos_test.go).

---

### `DEC-GEO-006`: PostgreSQL Transactional Outbox Over Inline Synchronous Kafka Produce
- **Context**: Emitting `geofence.enter` / `geofence.exit` directly inside the consumer handler fails atomically if Kafka broker produce fails after DB commit or DB rolls back after Kafka publish.
- **Decision**: Insert serialized `GeofenceTransitionV1` payloads into `outbox` inside the same `PersistTransitions` PG transaction, then drain via `FetchPendingOutbox` and `::uuid[]` `MarkOutboxPublished`.
- **Alternatives Rejected**: Kafka transactions across heterogenous DB + broker, CDC Debezium connector (excessive operational footprint for a single-repo service).
- **Evidence**: Scenario `7_outbox_crash_before_mark` proves crash-before-mark recovery converges `outboxPending` to `0` (`PASS`).

---

### `DEC-GEO-007`: Boundary-Is-Inside Polygon Winding + Hole Exclusion + Optional Dwell Hysteresis
- **Context**: Standard ray-casting libraries disagree on points lying directly on polygon edges or inside concave cutout holes (`fence-001`).
- **Decision**: Wrap `planar.RingContains` inside [`polygonContainsWithBoundary`](../internal/geofence/engine.go) with explicit segment distance check (`pointOnRingBoundary`, `boundaryEpsilon = 1e-10`), check outer ring plus inner holes (`rings[1:]` excluded), and gate transitions via thread-safe `confirmDwell` when `dwellMS > 0`.
- **Alternatives Rejected**: Pure bounding-box approximation (false positives), non-deterministic floating-point ray casts without edge epsilon.
- **Evidence**: `TestGeofenceGeometryEdgeCases` and `TestDwellHysteresisSuppressesBriefJitter` in `internal/geofence/`.

---

### `DEC-GEO-008`: Advisory Lock (`pg_advisory_xact_lock(727001)`) Around SQL Migrations
- **Context**: Running `go test -race ./internal/... ./api/... ./tests/...` executes `api`, `tests/integration`, and `tests/chaos` concurrently against the same PostgreSQL instance, colliding on `CREATE TABLE IF NOT EXISTS` (`SQLSTATE 23505`).
- **Decision**: Acquire `SELECT pg_advisory_xact_lock(727001)` at the start of `PostgresStore.Migrate`.
- **Alternatives Rejected**: External `migrate` CLI wrapper in scripts (doesn't serialize parallel Go test binaries).
- **Evidence**: Zero `SQLSTATE 23505` errors across concurrent `go test -race ./internal/... ./api/... ./tests/...` runs.

---

### `DEC-GEO-009`: `SeekGroupToEnd` for Clean Multi-Run Benchmarks on Shared Topic
- **Context**: Reusing `courier.gps.raw` across sequential benchmark runs caused new consumer groups initialized with `AtStart` offsets to replay tens of thousands of historical records from earlier runs.
- **Decision**: Implement [`SeekGroupToEnd`](../internal/kafka/client.go) (invoked cleanly in simulator benchmarks and `KAFKA_SEEK_END_ON_START=true` in worker configuration) when joining a new benchmark run on shared dev topics, while preserving standard offset resume for crash-recovery groups.
- **Alternatives Rejected**: Creating dynamic per-run Kafka topics (strands topic cleanup and breaks static Prometheus/alert rules).
- **Evidence**: Clean `17723 == 17723` reconciliation in `run-local-001`.

---

### `DEC-GEO-010`: Bounded Channel + `PauseFetchTopics` vs. Drop-on-Full
- **Context**: In-memory unbounded queues hide DB downstream slowdowns behind growing heap and sudden GC stalls, while dropping events violates zero-loss transition guarantees.
- **Decision**: Bound persistence channel depth (`GEOSTREAM_CHANNEL_CAP=4096` in K8s / configurable in worker) and suspend Kafka broker fetches cleanly until channel utilization drops below hysteresis watermark (`RunBoundedWorker`).
- **Alternatives Rejected**: Unbounded `chan` / slice queue (proven `98.0%` throughput collapse in `run-bp-001`), tail-drop queue (silently loses geofence crossings).
- **Evidence**: `bench/compare` (`run-bp-001`: `2400 == 2400` persisted, `276.95` vs. `13932.79 ev/s`).

---

### `DEC-GEO-011`: Single-Source JSON Schema Codegen (`scripts/gen-contracts.mjs`)
- **Context**: Drift between Go backend structs, Kafka event payloads, and React TypeScript interfaces breaks consumers silently at runtime.
- **Decision**: Author canonical JSON Schemas under [`contracts/`](../contracts/) (`location-event.v1`, `geofence-transition.v1`, `deadletter.v1`) and generate `internal/schema/types_gen.go` + `web/src/api/contracts.gen.ts` enforced in CI via `--check`.
- **Alternatives Rejected**: Hand-maintained dual Go/TS types or Protobuf toolchain complexity for JSON/REST endpoints.
- **Evidence**: `node scripts/gen-contracts.mjs --check` exits `0` in `Makefile` and `.github/workflows/ci.yml`.

---

### `DEC-GEO-012`: GeoJSON Normalization in PG `ListGeofences` + Defensive Map Parser
- **Context**: Storing `f.RawGeo` as raw coordinate ring arrays in `geofences.geometry` caused `/api/v1/runs/{id}/geofences` to emit bare arrays without GeoJSON `{type, coordinates}` wrappers, crashing the React SVG map during live Playwright QA.
- **Decision**: Normalize bare JSON arrays into GeoJSON `Polygon` objects in `PostgresStore.ListGeofences` (`CASE WHEN jsonb_typeof(geometry) = 'array' THEN jsonb_build_object('type', 'Polygon', 'coordinates', geometry) ELSE geometry END`) AND keep defensive array-or-object ring extraction in [`MapReplayPage.tsx`](../web/src/routes/MapReplayPage.tsx).
- **Alternatives Rejected**: UI-only workaround leaving non-compliant GeoJSON on the REST contract.
- **Evidence**: Verified live in `04-map-replay-1440.png` and Playwright E2E (`1 passed (6.4s)`).

---

### `DEC-GEO-013`: Bounded Prometheus Labels (0 `deviceId`) & Honest Latency Boundaries
- **Context**: Adding `deviceId` labels to `40,000` devices explodes Prometheus TSDB cardinality, and blending Kafka ingress latency with end-to-end PG latency hides true bottlenecks.
- **Decision**: Restrict labels to bounded dimensions (`type` `ENTER|EXIT`, `partition` `0..15`) across all 15 metrics in [`internal/telemetry/metrics.go`](../internal/telemetry/metrics.go), and separate `INGEST_INTERNAL` (`ProcessingLatency`) from `PIPELINE_TRANSITION` (`OutputLatency`) with sub-12ms buckets (`[0.0005..1.0]`).
- **Alternatives Rejected**: Single coarse `request_duration_ms` summary or per-device Prometheus gauges.
- **Evidence**: `TestAll15CanonicalMetricsHaveBoundedCardinality` asserts `0` `deviceId` labels.

---

### `DEC-GEO-014`: Server-Aggregated BFF + `<=100` Map Marker Cap + Zero-Fabrication Degraded States
- **Context**: Rendering `1,500` to `40,000` live courier pins directly in SVG/DOM degrades frame rates, while interpolating missing Prometheus time series misleads operators during outages.
- **Decision**: Serve paginated/sampled `<=100` device slices from `GET /api/v1/runs/{id}/devices`, limit SSE ticks to `<=1 Hz` summary counts (`handleStreamSSE`), pair the SVG map with an accessible table + `tabIndex={0}` scrollable regions, and render explicit `Metrics temporarily unavailable (no fabricated fallback points)` when Prometheus is unreachable.
- **Alternatives Rejected**: Raw Kafka WebSocket firehose to browser or synthetic chart smoothing when Prometheus errors.
- **Evidence**: `01`..`08` screenshots, `App.test.tsx`, and `e2e/geostream.spec.ts` passing Axe WCAG 2.2 AA (`0` violations).

