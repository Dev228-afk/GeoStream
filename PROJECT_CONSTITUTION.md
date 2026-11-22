# GeoStream — Antigravity Implementation Specification

**Target interview:** Apple Software Engineer – Early Career (Front-end), Apple Ads Engineering  
**Resume project:** GeoStream: Real-Time Location Event Processor  
**Priority:** P2 — systems/scalability evidence and an optional React monitoring UI  
**Specification revision:** V2, 2026-11-20. This file supersedes the earlier brief plan.
**Deliverable:** a real Go/Kafka/Redis/PostgreSQL event pipeline, controlled load generator, geofence transitions, offset/data-gap reconciliation, Kubernetes dashboards/alerts, observable backpressure experiment and deep interview packet.

> **Privacy and authenticity:** Courier movement is **entirely simulated**. GeoStream must never present synthetic locations as tracking real people or vehicles. It is a distributed processing and monitoring demonstration, not an Apple advertising system. Public maps use fake device IDs and generated GPS routes only.

## V2 implementation contract — precise event model, crash correctness, API and premium UI

**Provenance and authority.** The resume states Go/Kafka/Redis/PostgreSQL, a 40,000-device / 80,000-events-per-second simulator, deduplication, geofence transitions, offset reconciliation, Prometheus/Grafana, Kubernetes P99 latency, and bounded backpressure. The following topic names, schemas, API endpoints, user journeys, visual tokens and consistency protocol are **inferred implementation choices**. They are not secret Apple infrastructure or a claim about actual courier telemetry. This V2 contract is authoritative over earlier schematic examples. When a small model sees a mismatch, it must stop that task, log `BLOCKED_CONTRACT` and seek lead-agent resolution rather than inventing behavior.

**Updated priority:** correct streaming semantics and measured throughput first, excellent human-readable monitoring/replay UI built incrementally with the vertical slice (not postponed until release), observability/CI/cloud afterward. GeoStream's UI is narrower than AdCanvas, but must meet the **same craft bar**. Only fictional couriers, generated coordinates and licensed/self-generated base maps.

### A. Canonical event schema, validation and deterministic generator

Use a versioned JSON schema in `contracts/location-event.v1.schema.json`, generate Go structs and TS types from it. Never manually diverge. All timestamps UTC RFC 3339 with millisecond precision; validate no NaN/Inf, latitude [-90,90], longitude [-180,180], accuracy 0–200m, `sequence >=1`, and event time not more than 5 minutes in the future (configurable). `deviceId` identifies fictional courier; no real phone/user data.

```json
{
  "schemaVersion": 1,
  "runId": "run_demo_001",
  "eventId": "2fd25bc9-b2c3-5d1e-8175-9a838294186e",
  "deviceId": "courier-000042",
  "sequence": 321,
  "routeId": "route-117",
  "eventTime": "2026-11-20T12:00:00.500Z",
  "producedAt": "2026-11-20T12:00:00.503Z",
  "latitude": 37.33,
  "longitude": -121.89,
  "accuracyM": 8.0
}
```

For a given `runId,deviceId,sequence,seed`, `eventId` is **deterministic** (UUIDv5 or stable hash), not randomly regenerated after retries. `sequence` increments per device; in-simulator sequence loss is deliberate fault injection and must be reported. Kafka message key is `deviceId` (same device -> same partition until partition count changes). Do not change topic partitions mid-run. `producedAt` is a sender-side timestamp; event-time for route/geofence semantics is `eventTime`, but the **monotonic accepted sequence** determines latest-state precedence (late lower sequence is ignored, counted, not retroactively applied).

**Geofence GeoJSON:** `FeatureCollection` with `Feature.id`, `properties.name`, `properties.version`, `geometry Polygon/MultiPolygon`; fence ID and version stable. Use a tested geometry library and document longitude/latitude order, inclusion on boundary (choose **boundary considered inside**), hole semantics (hole is outside), overlapping polygons (independent transitions), jitter mitigation (two consecutive observations or explicit `dwellMs`, default **0 for deterministic smoke**, optional 3000ms stress). The same geofence version is immutable during a run; changing versions requires controlled state migration.

**Dataset construction:** `make seed-routes SEED=2026` creates `fixtures/city_grid.geojson`, `fixtures/geofences.geojson`, `fixtures/routes.jsonl`, `fixtures/fault_profiles.yaml`. The fictional route grid needs ~20 fences and 1000 route templates; build a deterministic geographic projection around a plausible fictional coordinate center, no real live tracking. Ensure routes include outside/inside/outside crossings, boundary touch, hole, overlaps, static device, jitter, teleport and invalid GPS. Track expected transitions for the small smoke fixture in `fixtures/expected_transitions.json` produced independently of processing logic.

Load profiles: smoke 100 devices/200 events/s; dev 1000/2000; scale 10K/20K; resume-stress 40K/80K target >=3-minute steady. Record **generated, broker-acknowledged, consumed, accepted unique, invalid, late, duplicate and terminal processed rates separately**. Never present a target rate as achieved. Do not send simulation traffic to external maps services.

### B. Durable domain model (PostgreSQL) and materialized/cache state (Redis)

PostgreSQL is authoritative for **geofence membership transitions and outbox**, while Redis is a fast *latest-location cache* and hot-path sequence guard. **Do not claim cross-system atomic transactions.** Store GPS raw events primarily in Kafka (optionally archive benchmark samples), not every ping in PostgreSQL. Use migrations and indexes.

| Table | Fields | Invariants |
|---|---|---|
| `runs` | `id text PK`, `seed bigint`, `profile`, `started_at`, `ended_at`, `status enum(CREATED,RUNNING,COMPLETED,FAILED,ABORTED)`, `generated_count`, `broker_acked_count`, `config_sha256` | Distinguishes scenarios; immutable config hash after start. |
| `geofences` | `id text`, `version int`, `name`, `geometry jsonb`, `bbox jsonb`, `enabled`, `created_at`, composite PK `(id,version)` | Version immutable during run. Geometry validated before activation. |
| `device_fence_state` | `run_id`, `device_id`, `fence_id`, `fence_version`, `inside bool`, `last_transition_seq bigint`, `last_transition_at`, `updated_at`, composite PK `(run_id,device_id,fence_id,fence_version)` | **Durable membership at last emitted transition**, not a claim of durable position for every ping. |
| `geofence_transitions` | `transition_id text PK`, `run_id`, `device_id`, `fence_id`, `fence_version`, `transition_type enum(ENTER,EXIT)`, `source_event_id`, `source_sequence`, `event_time`, `created_at`, `source_partition`, `source_offset` | Stable ID hash of `(runId,deviceId,fenceId,fenceVersion,sourceSequence,type)`; idempotent INSERT ON CONFLICT DO NOTHING. |
| `outbox` | `id uuid PK`, `transition_id UNIQUE FK`, `destination_topic`, `payload jsonb`, `published_at nullable`, `publish_attempts`, `last_error nullable` | Created in same PG transaction as transitions + membership change. Publish may retry; downstream dedupes transitionId. |
| `partition_checkpoints` | `run_id`, `consumer_group`, `topic`, `partition`, `last_seen_offset`, `last_committed_offset`, `last_seen_at`, `sequence_gap_count`, PK `(run_id,consumer_group,topic,partition)` | Observability/audit, never mistaken for Kafka's actual commit authority. |
| `incidents` | `id`, `run_id`, `kind`, `severity`, `first_at`, `last_at`, `evidence_ref` | Optional persisted alert outcomes, not fake live metrics. |

Redis record key `geo:{runId}:device:{deviceId}` with TTL 24h during active demo (configurable) and hash fields `lastSeq`, `lastEventId`, `lat`, `lon`, `eventTime`, `observedAt`, `fenceVersion`, `insideFenceIds`, `membershipDigest`. Use a **single atomic Lua compare-and-set** for sequence and location. Return `ACCEPTED | DUPLICATE | STALE`, never split GET/SET across processes. Keep a per-run active-device set **bounded by TTL**; cardinality metric need not scan all keys on each API request. Recovery from Redis eviction is from durable membership in PostgreSQL + new/replayed Kafka events, **not a claim that PostgreSQL stores all last coordinates**. The UI must reflect `positionFreshness` when cache state is lost.

Kafka topics and payloads:

```text
courier.gps.raw       key=deviceId; LocationEvent v1
geofence.enter       key=deviceId; GeofenceTransition v1
geofence.exit        key=deviceId; GeofenceTransition v1
pipeline.deadletter  key=deviceId; {originalTopic,partition,offset,eventId,reason,redactedPayloadHash}
```

Transition JSON:

```json
{
  "schemaVersion":1,
  "transitionId":"tr_3cd94f",
  "runId":"run_demo_001",
  "deviceId":"courier-000042",
  "fenceId":"fence-003",
  "fenceVersion":1,
  "type":"ENTER",
  "sourceEventId":"2fd25bc9-b2c3-5d1e-8175-9a838294186e",
  "sourceSequence":321,
  "eventTime":"2026-11-20T12:00:00.500Z"
}
```

### C. **Required crash-safe processing sequence** (do not reorder)

An earlier sketch that updates Redis first and persists a transition later can drop business transitions after a worker crash: Redis might reject the replay as stale while Postgres has no transition. The implementation **must use** the sequence below and test the failure window.

1. Consumer receives a keyed location event with `(topic,partition,offset)`. Validate schema; poison payload goes to DLQ with original provenance (and offset committed only after DLQ publish is acknowledged).
2. Load last Redis state. If cold/missing, initialize membership from durable `device_fence_state` and last-sequence lower bound from last transition, then continue. Use per-partition ordered work or explicit per-device serialization; do not process one device concurrently across worker goroutines.
3. If `sequence < last accepted sequence`: classify `STALE`, emit metric/audit sample and commit only after classification. Equal sequence with same event ID is `DUPLICATE`; equal sequence with different payload is `CONFLICT`, routed to DLQ, not overwritten.
4. For each affected fence, compute membership change using deterministic boundary/hysteresis contract. If **no transition**, atomically advance Redis latest seq/location, then commit Kafka offset after Redis acknowledges. This path avoids writing every raw ping to PostgreSQL.
5. If **one or more transitions**, **BEFORE advancing Redis sequence**, open a **short PostgreSQL transaction**: insert each unique transition, update durable `device_fence_state` membership/lastTransitionSeq, insert matching `outbox` records. On conflict, verify existing transition consistency rather than blindly overwriting. Commit database.
6. **ONLY AFTER DB COMMIT** atomically update Redis location/sequence/membership snapshot for this event. If Redis fails, do not commit offset; replay will re-check unique transition IDs/PG membership and can safely repair Redis state.
7. Commit input Kafka offset **after required PG/Redis effects are done or safely deduplicable on replay**. Document commit granularity and rebalance graceful shutdown. Outbox publisher sends transition to Kafka using stable key/ID and marks `published_at`. A crash between publish and mark may duplicate publication: downstream contract dedupes `transitionId`. Do not claim one atomic transaction spans Kafka, Redis and PG.
8. If a crash occurs **after DB commit but before Redis write/offset commit**, replay must reconstruct Redis from durable membership and verify the same transition is not duplicated. If a crash occurs **after Redis write but before offset commit**, replay may be skipped as duplicate, but the transition was already durable because DB came first. This precise ordering is essential.

**Important edge case:** Redis state may have moved forward from earlier nontransition pings. On replay of an already committed transition, query durable PG for the recorded stable transition ID before applying stale-sequence suppression if the source offset is uncommitted. Preserve the dedupe/audit reason; do not skip a missing durable transition based solely on Redis. Use fault hooks after each numbered step. If a PG transaction fails, do not mutate Redis sequence/membership; pause/retry appropriately. A redis-cold rebuild recovers transition membership, while last position remains stale until new/replayed pings; UI labels stale location. Designated recovery test **kills worker at each step** and checks PG+outbox+consumer replay counts.

For geofences with dwell/hysteresis, per-device pending candidate state must also be bounded and replay-safe or recomputed from a bounded window; default smoke test uses dwell=0. Do not announce durable dwell guarantees unless implemented and tested.

### D. Go module/interface boundaries and concurrency contract

Use Go module `github.com/example/geostream` (replace with actual repository URL once known; no internal company names). Separation:

```go
type LocationEvent struct { /* generated from JSON schema */ }
type GeofenceTransition struct { /* generated from JSON schema */ }
type EventConsumer interface { Poll(ctx context.Context) (Record, error); Commit(ctx context.Context, r Record) error }
type LatestStateStore interface { Read(ctx context.Context, runID, deviceID string) (LatestState, bool, error); CompareAdvance(ctx context.Context, next LatestState) (AdvanceResult, error) }
type TransitionStore interface { PersistTransitions(ctx context.Context, changes []GeofenceTransition) error; LoadMembership(ctx context.Context, runID, deviceID string) ([]FenceMembership, error) }
type OutboxPublisher interface { PublishPending(ctx context.Context, limit int) error }
type GeofenceEngine interface { Evaluate(p LocationEvent, previous []FenceMembership) ([]TransitionIntent, error) }
```

Error classification `INVALID_EVENT`, `DUPLICATE`, `STALE`, `CONFLICT`, `TRANSIENT_REDIS`, `TRANSIENT_DB`, `TRANSIENT_KAFKA`, `FENCE_GEOMETRY_ERROR`. Transient dependencies use retry + backpressure, deterministic payload failures go DLQ. Producer and consumer use one bounded worker pool per partition/partition-assignment; do not spawn a goroutine per ping. Bounded channel cap fixed (e.g. 4096) and worker pool 4–16 from config; apply pause/controlled polling when full instead of silently dropping. DB connection max and batch settings in config; outbox publisher separate. Graceful SIGTERM: stop intake, drain within configured deadline, commit only completed offsets, flush outbox best effort and leave the rest for replay. Health/readiness endpoints distinguish alive worker from dependency readiness; never fake ready after Kafka disconnect.

### E. Read-only UI API contract (actual data, no server-faked success)

The dashboard backend aggregates Redis/PG/Prometheus; the UI does **not** scrape Redis directly or query 40K devices every second. All endpoints `/api/v1`; all times UTC, durations milliseconds, rates events/second, `p99Ms` numeric with boundary named. Demo and evidence modes clearly distinguished.

| Endpoint | Input | 200 response | Edge cases |
|---|---|---|---|
| `GET /runs` | cursor/limit max 50 | `{items:RunSummary[],nextCursor}` | newest first; no credentials in output. |
| `GET /runs/:id/summary` | run ID | `{run,generatedPerSec,brokerAckedPerSec,consumedPerSec,acceptedPerSec,duplicatePerSec,invalidPerSec,kafkaLag,activeDeviceCount,latency:{boundary,p50Ms,p95Ms,p99Ms},dataFreshnessAt,mode}` | stale components have `status:"unavailable"`, not fake zeros; 404 run absent. |
| `GET /runs/:id/devices?cursor=&limit=` | max 500 | `{items:[DeviceSummary],nextCursor}` | stable pagination; no all-device response. |
| `GET /runs/:id/devices/:deviceId` | device ID | `{deviceId,lastSeq,lat,lon,eventTime,positionFreshness,insideFenceIds}` | 404 nonexistent, `positionFreshness:"stale"` after cache loss. |
| `GET /runs/:id/geofences` | none | `{type:"FeatureCollection",features:[...]}` | real valid GeoJSON with versions. |
| `GET /runs/:id/transitions?cursor=&limit=&type=` | max 100 | `{items:[GeofenceTransition],nextCursor}` | order eventTime+ID, tenantless synthetic demo only. |
| `GET /runs/:id/timeseries?metric=&window=&resolution=` | metric allowlist, window 5m/15m/1h, resolution 1s/10s/1m | `{metric,unit,points:[{at,value|null}],source,status}` | null gaps remain gaps; no fabricated interpolation. |
| `GET /runs/:id/reconciliation` | none | `{generated,brokerAcked,consumed,acceptedUnique,duplicates,invalid,late,gaps,transitionsPersisted,outboxPending,lastCommittedOffsets,pass,limitations}` | pass false for unexplained missing events. |
| `GET /runs/:id/incidents` | cursor | `{items:[Incident],nextCursor}` | timestamps and evidence URLs; not fake alerts. |
| `GET /runs/:id/stream` | SSE GET (optional) | aggregate summary event at max 1Hz | client reconnect fetches summary; do not stream 80K events/s to browser. |
| `GET /health` | none | `{status,dependencies,commit}` | unavailable dependency reflected. |

Example summary:

```json
{
 "run":{"id":"run_demo_001","status":"RUNNING","profile":"smoke","startedAt":"2026-11-20T12:00:00Z"},
 "generatedPerSec":200,"brokerAckedPerSec":200,"consumedPerSec":199.8,"acceptedPerSec":199.7,
 "duplicatePerSec":0.1,"invalidPerSec":0,"kafkaLag":3,"activeDeviceCount":100,
 "latency":{"boundary":"INGEST_INTERNAL","p50Ms":1.4,"p95Ms":3.1,"p99Ms":5.8},
 "dataFreshnessAt":"2026-11-20T12:01:10Z","mode":"LIVE"
}
```

**Example numbers are illustrative contract data only**, never benchmark claims. Use numeric values only from actual Prometheus/evidence queries at runtime. Standard errors:

```json
{"error":{"code":"RUN_NOT_FOUND","message":"This run is unavailable.","requestId":"req_01"}}
```

Status matrix: 400 invalid query, 404 absent, 422 unknown metric/resolution, 429 refresh abuse, 503 storage/metrics unavailable. For unavailable Prometheus, UI shows “Metrics temporarily unavailable” with prior timestamp; do not substitute simulated healthy measurements. A `mode:"RECORDED"` run is clearly labeled with capture timestamp and environment.

### F. Observability, correctness and performance measurement contracts

Counters must reconcile at well-defined layers; use Prometheus counters, histograms for sub-12ms boundaries and a bounded-cardinality label set (never deviceId as Prometheus label). Track `eventsGenerated`, `brokerAcked`, `consumed`, `invalid`, `duplicate`, `stale`, `acceptedUnique`, `transitionsPersisted`, `outboxPending`, `outboxPublished`, `offsetCommitted`. Distinguish **internal ingest processing** from Kafka queue wait and end-to-end durability. Histograms in **seconds** with buckets appropriate around 1ms, 2ms, 4ms, 8ms, 12ms, 20ms, 50ms, 100ms, 500ms and beyond. Derive p99 from histogram quantiles and report histogram resolution. Measure sustained 3-minute stable output at actual Kafka broker ack and consumer rates, not simulator target; record CPU, RSS, GC, Kafka lag, Redis throughput, PG write rate and broker partitions.

**Controlled backpressure regression:** `bench/unbounded-baseline` is isolated, dangerous only to its test process; memory cap and max duration. `production/bounded` uses fixed channel cap and worker count, clear signal to pause/resume reading. Both execute same seeded stream and DB latency injection; compare actual accepted unique counts, throughput, lag, RSS and end-to-end transition correctness. A 22% drop is historical claim only unless measured. If bounded version improves throughput by dropping data, benchmark fails. `make bench-backpressure` must output before/after pprof, channel depth and summary; never display a hard-coded “22%” on reviewer UI.

**Chaos matrix:** simulate duplicate source delivery, out-of-order lower sequence, Kafka worker crash after PG commit before Redis update, after Redis before offset commit, outbox publish before published flag, Redis flush/restart, DB outage during transition, broker rebalance, poison schema, gap in sequence, slow DB under burst, slow subscriber, pod SIGTERM. For each, require: expected terminal category, raw Kafka record/offsets, PG row count, Redis lastseq, outbox status, reconciliation result, metrics/alert result. **Important:** a Kafka offset gap does not directly prove a missing business event; compare generator manifest and event identifiers/sequence accounting.

### G. Product/visual specification — a quiet real-time observability workbench

Identity: **GeoStream = calm infrastructure observability**, inspired by clarity and precise operational feedback, not cyberpunk maps. Light graphite and understated ocean-blue accent, sparse charts and carefully designed maps; no neon-green dots on all-black canvas, random glowing gradients, stock courier photos or unsupported geographic precision. The front-end is original; do not copy Apple Maps or Apple Ads visual assets.

```css
:root {
 --canvas:#f5f7f9; --surface:#fff; --surface-muted:#edf2f6;
 --ink:#17222b; --muted:#5c6c7a; --subtle:#84929d; --line:#d9e2e9;
 --accent:#176a92; --accent-hover:#105473; --accent-soft:#e3f1f8;
 --success:#17785c; --warning:#926315; --danger:#b23f42;
 --r-sm:9px; --r-panel:16px; --shadow-soft:0 10px 26px rgba(23,43,61,.05);
}
```

System-font stack only; page title 32–38px, body 14–16px, table labels 13–14px, metrics 28–36px **tabular numerals**. 8px spacing foundation, 24–32px gutters desktop, wide main 12-column grid. Top status bar: left run selector & capture/live label, right data freshness, dependency health and “Download evidence”. Sidebar 220px. Dashboard **not** 8 giant identical KPI cards: show 4 compact metrics across one band, a dominant event-rate/lag composite chart, and an operational incident panel. Charts use explicit axes, units, legend and time windows, not arbitrary colored decorative lines.

**Screen-by-screen composition:**

| Screen | Primary composition | Required states and fidelity |
|---|---|---|
| `/overview` | Run/time header, compact processed/s and p99 summary, wide throughput chart, Kafka lag sparkline, active alerts table | p99 boundary visibly labeled (e.g., “ingest internal”); “recorded” vs live prominent but calm; missing metrics shown as unavailable, not zero. |
| `/map` | 2/3 width route/grid canvas or local GeoJSON view, right replay scrubber + last events panel | Use deterministic faux basemap grid, clean fence boundaries, per-fence color/outline; maximum 100 visible sampled couriers; accessible data table alternative; no 40K DOM pins. |
| `/diagnostics` | Backpressure channel depth, lag, RSS, pod restarts, before/after benchmark with same run config | Controlled baseline labeled intentionally faulty, not production; evidence links beside charts; clear unit notes and caveats. |
| `/transitions` | Cursor-paginated transition table and detail drawer showing source event, fence and output | Filters ENTER/EXIT, event-time vs processing-time, dedupe state; keyboard-friendly row navigation. |
| `/runs/:id/evidence` | Run manifest, metrics sources, raw export links, chaos/failure timeline, reconciliation diff | Distinguish PASS/BLOCKED/FAIL, provide exact produced-vs-consumed ledger; no “100% reliability” without proof. |

Responsive at 1440×900 / 1024×768 / 390×844: collapse sidebar, map above evidence table, no horizontal page overflow, charts retain labeled axes or switch to summary cards/accessible tabular values. At mobile size use compact top summary and “Replay” screen with native controls; do not try to squeeze desktop telemetry grid. Use 44px touch actions; persistent visible focus, keyboard accessible timeline steps, hover is never the only way to reveal incidents, color/contrast accessible and prefers-reduced-motion respected. Animations purposeful and subtle (~160ms). Do not implement animated thousands of individual courier dots; sample <=100 visible markers, throttle map re-render to <=5fps when playing, aggregate backend snapshots at 1Hz. Time plots downsample to <=600 points series and show min/max evidence when downsampling (no misleading smoothing). UI error states must preserve last measurement timestamp and explain whether charts are stale.

**Craft verification:** pinned Playwright Chromium screenshots at three viewports for (i) clean overview, (ii) live status, (iii) recorded run, (iv) metric unavailable, (v) selected fence/transition, (vi) gap alert, (vii) backpressure experiment and (viii) mobile replay. Visually review alignment, typography, spacing, legends, axes, zero/empty versus missing, chart tooltips, map contrast, and hover/click hit areas. axe WCAG 2.2 AA smoke, keyboard/200% zoom, reduced motion. Never let the agent claim a “premium” UI merely because a CSS theme exists—actual browser walkthrough/screenshots and independent review are required.

### H. Very small agent: exact task sequence and gates

1. Scaffold Go module and Docker Compose (Kafka, Redis, PostgreSQL). Create shared JSON schema + generated Go/TS types, migrations, error enums; verify compose health.
2. Generate one polygon and 4-event expected fixture; build simulator producing outside→inside→inside→outside; assert expected one ENTER/one EXIT.
3. Implement Kafka producer/consumer keying, input validation and DLQ; save source topic fixture and offset trace.
4. Implement geofence point-in-polygon with boundary, hole and overlap tests, and deterministic `transitionId`.
5. Add PostgreSQL transition/member/outbox transaction; prove failure-injection between DB commit and Redis write does not lose/duplicate effect.
6. Implement Redis atomic latest-state, duplicate/stale detection and cold recovery; test replay after Redis restart and Kafka offset rewind.
7. Add bounded batch/outbox worker and offsets; verify source reconciliation and idempotent downstream publication.
8. Build one real `/runs/:id/summary` API and a carefully designed **functional** React overview screen using canonical tokens; screenshot desktop/mobile. This is the first full UI integration gate.
9. Add high-fidelity map/replay + diagnostics + transitions with real sampled data; never start browser from fabricated charts.
10. Add stress simulator, 80K target bench, p99 instrumentation and unbounded/bounded isolated experiment; record achieved numbers and exact latency boundary.
11. Add Prometheus/Grafana alerts, Kubernetes kind profile, Helm and CI; run injected alert drill and verify firing/resolution.
12. Optional approved AWS (EKS/ECR/S3/CloudWatch/Terraform), tagged resources and destroy; generate full interview packet from actual code and evidence.

At each task handoff require: `changed files`, `exact commands and exit codes`, `schema/API example`, `unit/integration results`, `failure injection outcome`, `screenshots where UI`, `raw evidence`, `open blockers`. Maintain status enum `NOT_STARTED / IMPLEMENTED_UNVERIFIED / VERIFIED / BLOCKED_EXTERNAL / BLOCKED_CONTRACT` in `IMPLEMENTATION_STATUS.md`. Worktrees may modify disjoint files only; lead agent owns schemas/migrations and final integrative merge. No completion claims based on passing mocked browser chart fixtures alone.

### I. Interview completeness gate

Generate interview Q&A only after real code and evidence. For at least 50 primary questions + 100 follow-ups include 30/90/300-second answers, exact Go/TS modules, Kafka/PG/Redis effects, raw metrics or `NOT_MEASURED`, failure story, alternative, counterexample and what changes at 10x scale. Required deep probes: Redis->PG lost-transition failure window; what PG transaction covers; Kafka offset commit timing; outbox duplicate publication; difference between consumer processing p99 and end-to-end durable p99; histogram p99 accuracy; 40K logical couriers vs device connections; 80K target vs measured consumption; QD? (not relevant: do not accidentally import NVMe-specific questions); 22% regression under identical load; backpressure propagation without drops; map scalability in React; error/empty/recorded UX; Kubernetes CPU limits/pod termination; when Redis restart leaves stale position; what happens with late or re-ordered event; how the idempotency key survives a replay. Do not provide fictional “we proved” answers with absent benchmarks.


### Universal Antigravity execution protocol — mandatory for less capable agents

**This is not a one-shot generation prompt.** The lead agent must create `IMPLEMENTATION_STATUS.md`, `TASKS.md`, `docs/architecture-decisions.md`, `docs/contracts/`, and `evidence/` before work starts. Break delivery into tasks small enough to finish with tests in one coding session; never ask a subagent to "build the whole app". The **latest V2 data/API/UX contract in this file is the single source of truth**. Existing abbreviated examples below are educational context, not permission to invent alternate routes or fields.

Each subagent task must be issued with this exact format:

```text
TASK ID: <e.g. ADC-006>
GOAL: <one independently testable behavior>
ALLOWED FILES: <explicit paths/directories>
READ FIRST: <relevant sections of this specification and existing source>
DO NOT TOUCH: <other agents' files, database contracts unless approved>
INPUT CONTRACT: <exact types, routes, sample payloads>
OUTPUT CONTRACT: <exact types, routes, sample payloads>
STEPS: <small ordered implementation steps>
TESTS TO RUN: <exact commands + expected assertions>
BROWSER STATES TO CHECK: <URLs, viewport sizes, interactions, if UI>
EVIDENCE TO SAVE: <raw output/log/screenshot/trace paths>
DONE WHEN: <all listed assertions pass>
BLOCKER POLICY: stop, write reason in IMPLEMENTATION_STATUS.md; never fabricate a stub.
```

The lead agent works in this loop: **read existing code → select one task → confirm contract → implement smallest working slice → run typecheck/lint/tests → run browser check if visible → inspect diff → record evidence → mark VERIFIED → merge**. If the environment lacks Docker, Kafka, cloud permissions, browser, or LLM credentials, mark that specific test `BLOCKED_EXTERNAL` with a reproduction command and preserve what genuinely worked. Do not treat a green mocked test as evidence for a live integration.

**Hard prohibitions:** never silently change an API name, never add fields just to make frontend types compile, never use `any`/`@ts-ignore`/disabled ESLint in the frontend, never hardcode a business success response, never use a JSON fixture in a route described as live, never add fabricated KPI cards or synthetic "real" screenshots, never mark a performance target achieved without raw runs, never suppress an error in a catch block, never bypass a failing test by deleting it. No giant `App.tsx` or `views.py`/`main.go` monolith: split by feature and contract; refuse circular imports. Use stable IDs and deterministic seeds in tests, and always test the negative/failure branch.

**UI review is mandatory at every visible feature.** Implement a shared token sheet and base primitives first; never import an unreviewed component library/theme that overrides the specified look. Take actual browser screenshots of the user journey at desktop/tablet/mobile; inspect them yourself and through an independent reviewer/browser agent. Score each page **pass/fail** on hierarchy, spacing, typography, alignment, affordances, accessible keyboard/focus, responsive behavior, empty/loading/error/partial states and whether the content is real. If any critical dimension fails, return the task to the UI agent with the screenshot and one precise issue; visual QA is **not** satisfied by a build passing. External inspiration should emphasize clarity, consistency and meaningful feedback, not copy Apple brand assets or proprietary screens. Reference: https://developer.apple.com/design/human-interface-guidelines/design-principles/ ; WCAG 2.2 AA https://www.w3.org/TR/WCAG22/ ; Playwright screenshots https://playwright.dev/docs/test-snapshots .

**Test strategy:** unit → contract/component → real DB/broker integration → real user/browser E2E → performance/fault injection → visual regression. Pin browser/OS in screenshot CI, freeze seed/time where tests require repeatability, capture console/network warnings, and test 200% zoom. Report `NOT_MEASURED` or `NOT_EXECUTED` for benchmarks/cloud providers without evidence. Never copy historical resume numbers into metrics storage or dashboards.

**Acceptance is by evidence, not confidence:** a task is VERIFIED only with `commit ID`, `commands+exit codes`, `raw test output`, `affected API sample`, `screenshot/trace if UI`, `known limitations`. Lead reviewer may veto a task even with passing tests if it violates the contracts or visual instructions. At the end, an **independent interview agent** reads the final source, tests, architecture decisions and raw benchmark outputs and writes accurate resume-aligned answers; a fictional explanation must be flagged as hypothetical, not phrased as something already implemented.



---

## Original scope, delivery, tests, cloud and interview requirements retained below

## 0. Agent mission and strict scope

Implement exactly the storage/streaming and infrastructure claims from this resume project. Order of priority:

1. **Correct real event processing, idempotency, geofence logic and observable recovery.**
2. **Realistic performance measurements, including 40K device/80K event/sec stress profile and p99 latency definitions.**
3. **Reproduce a throughput regression caused by an unbounded database buffer; fix it with bounded channel and backpressure.**
4. **Kubernetes, Prometheus/Grafana, CI and optional real AWS deployment.**
5. **Small, functional React/TypeScript map/monitoring UI and reviewer story.**

The Apple Ads JD is frontend-focused; do not let this backend project displace completion of AdCanvas. The GeoStream UI should be polished enough to demonstrate performance-minded React integration but must not become an elaborate maps application.

**Historical resume numbers are not hard targets to fake.** Report actually observed results and make any environment limitations explicit.

## 1. Resume-to-evidence matrix

| Resume claim | Implementation requirement | Proof artifact |
|---|---|---|
| Go/Kafka ingestion engine | Actual producer/topics/consumer workers with schemas and partitioned keys | End-to-end test from producer to Redis/PG/topic |
| 40,000 active devices | Simulator holds state for 40,000 logical couriers and emits valid, deterministic events | Simulator manifest, device cardinality, raw metrics |
| Sustained **80,000 events/second** | Dedicated stress profile targeting 2 pings/sec × 40K devices | Multi-minute achieved rate, processed rate, Kafka lag, errors and hardware profile |
| Duplicate ping suppression | Stable event ID + per-device sequence / atomic state update | Duplicate replay tests, distinct accepted IDs vs submitted |
| Latest locations in Redis | Atomic latest-position logic, TTL, monotonic sequence guard | Redis assertions, race/out-of-order tests |
| Geofence entry/exit Kafka topics | Point-in-polygon transitions, stateful dedupe and downstream topics | Fixture transitions and output reconciliation |
| Automated offset reconciliation | Count/offset/sequence audits and replay after crash | Offset snapshots, gap detection, fault tests |
| Prometheus/Grafana data-gap alerts | Metrics for lag, stalled partitions, missing sequence, error and drop rates | Dashboard export + seeded alert drill |
| **Sub-12 ms p99 on Kubernetes at full load** | Define precise latency boundary, capture histograms, resource profile at full measured load | Histogram export + measured p99; distinguish internal from end-to-end |
| **22% throughput drop** during spike from unbounded DB buffer | Before/after controlled faulty vs fixed implementations | Raw before/after sustained throughput, channel depth, RSS, traces |
| Bounded channel and consumer backpressure fix | Hard-cap queue, bounded DB worker pool, pause/resume producer/consumer behavior | Buffer never exceeds cap; correct replay, no silent drops |

## 3. Architecture

```text
Deterministic Go courier simulator
                 |
        key=device_id
                 v
     Kafka courier.gps.raw (partitioned)
                 |
             Go ingest workers
       /         |           \
  validate    dedupe     event-time/order
       \         |           /
                 v
           Redis latest state
                 |
          Geofence state machine
                 |
                 +-- Kafka geofence.enter
                 +-- Kafka geofence.exit
                 +-- PostgreSQL transitions/audit (batched/outbox)
                 |
      Prometheus metrics / Grafana + alerts
                 |
     Small React/TypeScript dashboard
```

### Correctness boundary

Kafka consumers are effectively at-least-once in failure scenarios. Never claim that committing consumer offsets alone guarantees exactly-once writes to Redis, PostgreSQL and Kafka outputs. Protect business effects via stable IDs, monotonic sequence checks, deduplicating transition IDs, idempotent DB constraints and a carefully documented offset/commit policy. If an output is not transactional with the offset, tests must demonstrate safe replay and identify any remaining failure gap.

## 4. Implementation structure

```text
geostream/
├── README.md
├── AGENTS.md
├── PROJECT_CONSTITUTION.md
├── .agents/rules/project-focus.md
├── .agents/agents/{simulator,ingest,geo,state,platform,qa-perf,ui,interview}.md
├── cmd/{simulator,ingest,reconcile,replay}/
├── internal/
│   ├── config/
│   ├── schema/
│   ├── kafka/
│   ├── redisstate/
│   ├── geofence/
│   ├── storage/
│   ├── pipeline/
│   ├── backpressure/
│   ├── telemetry/
│   └── reconciliation/
├── fixtures/{routes.jsonl,city_grid.geojson,geofences.geojson,fault_profiles.yaml}/
├── api/                    # small read-only REST backend for dashboard
├── web/                    # Vite, React, strict TypeScript
├── tests/{unit,integration,chaos,load}/
├── bench/{generator,latency,baseline_bug,after_fix}/
├── deploy/{docker,k8s,helm,terraform/aws}/
├── monitoring/{prometheus,grafana,alerts}/
├── evidence/{throughput,latency,reconciliation,backpressure,cloud,ui,ci}/
├── scripts/
├── .github/workflows/
├── interview/
└── Makefile
```

Use Go modules, Go toolchain compatible with chosen Kafka/Redis/PG libraries, `go test -race` for stateful code, Docker and a locked dependency manifest. PostgreSQL holds durable transition summaries/audits, **not necessarily every raw ping**; raw events already live in Kafka/archived synthetic benchmark outputs. This avoids pretending a single DB can handle unlimited individual inserts.

## 5. First vertical slice (mandatory before delegating)

**One device, one polygon, one Kafka topic, one Redis key, one transition.**

```text
Go generator emits pings outside -> inside -> inside -> outside
                      |
                   Kafka
                      |
                   Go worker
                      |
              Redis last location
                      |
              enter/exit output topic
                      |
               PostgreSQL/audit
```

Assertions: one `enter`, one `exit`, no duplicate transition for repeated inside ping, current location equals newest processed sequence. Then add one duplicate ping and one out-of-order ping, proving monotonic state. Only then scale to multiple devices/workers.

## 6. Kafka topics, partitions and ordering

Create explicit topic manifest:

```text
courier.gps.raw      key=device_id, partitions configurable (e.g. 32 local / 128 stress)
geofence.enter       key=device_id
geofence.exit        key=device_id
pipeline.deadletter  key=device_id
```

Consumer group scales with partition count. The key keeps one courier's pings ordered **within a partition** for arrival order, but event timestamps can still be late/out-of-order. Document the distinction. Handle rebalance, offset commit, poison messages, graceful shutdown and replay. Avoid duplicating Kafka brokers/partition topology in every environment. Don't require a schema registry if a versioned JSON contract and schema tests satisfy the resume, but document the tradeoff.

**Offset policy:** commit after the relevant business effects are durably handled or idempotently replayable. Log partition/offset/sequence checkpoints; on restart use committed offsets and verify output/transition unique keys. Do not commit before work completes. Implement DLQ with original topic/partition/offset/validation failure and a controlled replay CLI. On a crash between downstream publish and offset commit, ensure replay does not emit duplicate business transitions (test explicitly).

## 7. Redis state and deduplication

Redis stores the latest location and per-device last accepted sequence with TTL. Use an atomic Lua script or equivalent transaction that rejects outdated/duplicate sequence numbers and writes a coherent `(lat, lon, seq, timestamp)` bundle. Avoid a naive separate GET then SET race. Tests must include concurrent events, duplicates, late events and missing/expired keys.

Redis is a cache/state accelerator; articulate the tradeoff of losing Redis state. Provide a recovery procedure from Kafka replay and/or durable transition snapshots, and ensure downstream transition IDs remain idempotent when Redis is reconstructed. Unique event IDs + `(device_id, sequence)` enable repeat suppression. Do not claim absolute exactly-once semantics across independent systems without proving a shared transaction or idempotent state contract.

## 8. Geofence engine

Represent fences as GeoJSON polygons and precompute bounding boxes. Start with a tested point-in-polygon library supporting holes; add simple spatial bounding-box filters before exact checks. Require:

- boundary handling contract (define whether on-edge is inside);
- crossing state machine (`OUTSIDE -> INSIDE => ENTER`, `INSIDE -> OUTSIDE => EXIT`);
- duplicate/unchanged ping produces no transition;
- GPS jitter near boundary: configurable hysteresis, dwell-time or minimum movement to avoid flapping;
- overlapping fences, holes, invalid coordinates, dateline and high-latitude limitations documented;
- stable `transition_id` derived from device/fence/transition sequence/version; DB unique constraint and Kafka output reconciliation.

Seed route fixture that hits every state and edge case. Keep algorithm complexity observable: naive O(pings × fences) vs indexed candidate filtering. Benchmark only after correctness.

## 10. Backpressure and the 22% throughput regression

Build two **controlled, explicitly labeled** modes:

```text
experiments/unbounded-baseline  # deliberately faulty, isolation only
production/bounded             # actual safe implementation
```

Faulty mode uses an unbounded producer→DB queue. Under a predictable burst with a DB throughput bottleneck, capture growing memory, queue depth, GC pauses, and a measured throughput regression. Use strict maximum duration/RSS/container limits to prevent OOM of development machine; terminate when safety threshold reached.

Fixed mode uses:

- bounded Go channel with fixed capacity;
- fixed worker count and DB connection pool;
- backpressure propagating to Kafka consumption rather than silent drop;
- consumer pause/controlled poll strategy without exceeding Kafka session timeout;
- timed batch flush and retry for transient DB errors;
- explicit shutdown drain semantics and no unbounded goroutine spawning;
- metrics for channel occupancy, backlog, processing time, dropped/duplicate counts and pause duration.

**Benchmark:** same input seed, rate, duration, hardware, Kafka partitions, Redis/DB settings and output semantics in both modes. Save heap profiles (`pprof`), GC/CPU profiles, queue-depth time series, throughput and p99. Only claim a **22% drop** if observed; otherwise report actual regression and the conditions. The fix must not “improve throughput” by dropping events. Reconcile accepted input to terminal output counts.

## 11. Load and latency benchmarks

A target of 80K events/sec can be more than a laptop can sustain. Create graded profiles; default small local smoke, large stress **only** on approved adequately provisioned local or AWS machines. Producer load and pipeline processing must be independent enough to detect bottlenecks.

**Define latency boundaries separately:**

1. `ingest_processing_ms`: Kafka consumer receives event → Redis state update and in-process geofence decision finishes.
2. `pipeline_transition_ms`: source publish/ingest → transition is durably recorded and published downstream.
3. `user_visible_freshness_ms`: source event timestamp → dashboard latest position visible.

If the historical sub-12ms p99 refers to a different boundary, the recreation must say so. Use histograms (Prometheus bucket design adequate for sub-12 ms), never derive p99 from averages. Capture queueing/lag, CPU, memory, pause time and batch sizes alongside latency.

For **resume-stress**, record at least 30s warmup and 3 minutes of stable measurement where resources allow; achieved produced rate, consumed rate, accepted unique rate, geofence output rate, lag, errors, duplicates, RSS, resource requests/limits and p50/p95/p99. **Do not call 80,000 a measured throughput when it was only a generator target.** Persist full benchmark config and actual hardware/cloud instance type.

## 12. Offset/data-gap reconciliation

Command:

```text
geostream reconcile --run-id <id>
```

Audit per Kafka partition:

- beginning/end offsets and last seen/committed;
- number of produced fixture events (known via manifest), consumed and accepted unique IDs;
- rejected duplicates, invalid/DLQ count, sequence gap count and late/out-of-order count;
- event/outbox unique transition count;
- recovery/replay status.

Kafka offset continuity alone cannot establish no missing *business* events, and unique IDs don't prove end-to-end ordering. Compare generation manifest counts/unique IDs with committed processing and terminal categories. Inject dropped/reordered/duplicate events and ensure each discrepancy appears in a report or is explicitly explained. Never hide data gaps behind successful liveness checks.

## 13. Observability and alert experiments

Expose Prometheus metrics:

```text
geostream_events_received_total
geostream_events_accepted_total
geostream_events_duplicate_total
geostream_events_invalid_total
geostream_sequence_gaps_total
geostream_geofence_transitions_total{type}
geostream_kafka_lag{partition}
geostream_processing_latency_seconds_bucket
geostream_output_latency_seconds_bucket
geostream_db_channel_depth
geostream_db_worker_errors_total
geostream_backpressure_seconds_total
geostream_redis_errors_total
geostream_replay_attempts_total
geostream_deadletter_total
```

Grafana dashboards: throughput, p99, Kafka lag, hot partitions, DB queue, error rate, Redis/PG health, pod restarts. Alert drill tests: persistent lag, no input progress despite producer activity, sequence gap threshold, DLQ spike, missing transitions after replay, p99 exceedance, DB buffer near cap. Alerts must be configured in actual Prometheus/Alertmanager rules, not just painted as red badges in a mock UI. Capture raw firing/recovery notification evidence.

## 14. Kubernetes, CI and cloud

**Local:** Kafka (KRaft or compatible single-node dev broker), Redis, PostgreSQL, Go workers, Prometheus/Grafana using Docker Compose. Kubernetes via kind/minikube; Helm chart or Kustomize for worker/API; readiness/liveness probes, bounded resource requests/limits, config/secret separation, graceful termination. Kubernetes load tests should measure actual pod resources and evictions/restarts.

**AWS opt-in profile** (only after budget/credential permission):

```text
ECR                         -> versioned Go images
EKS                         -> containerized workers / monitoring
EC2 or EKS Kafka            -> cheaper development broker than automatic MSK
S3                          -> fixture/benchmark artifact archive
CloudWatch                  -> metrics/logs (optional alongside Prometheus)
Terraform                   -> provision/teardown
```

Managed MSK, ElastiCache and RDS are valid choices if budget supports them, but **not mandatory**, since they can be expensive. Do not claim managed services were used unless actually deployed. Maintain an EC2-based stress profile if a user-approved machine offers better cost-to-throughput than EKS. Least-privilege IAM, private Redis/Postgres endpoints, resource tags, hard cleanup, cost notes and `make cloud-destroy` are required before provisioning. No silent paid cloud launches.

**CI:** `go vet`, lint, unit, `go test -race`, schema contracts, Docker Compose integration suite, 200 events/sec smoke, deterministically injected duplicate/out-of-order failure, deadletter/replay, Helm validation, benchmark artifact schema, frontend smoke (if UI exists), versioned Docker image build. Large 80K stress test must be a manual/nightly opt-in profile; never gate ordinary PRs on expensive infrastructure.

## 15. Reviewer-facing monitoring UI (secondary but functional)

Build a **small, genuine React + strict TypeScript** monitoring screen, not a second company-scale mapping product. Source data comes from a small read-only REST backend using Redis summary/state + Prometheus query snapshots + recorded GeoJSON. Pages:

- Overview: active simulated devices, processed rate, p99, lag and alerts.
- Map/Replay: sample fake courier traces and geofence entry/exit events with playback speed; no real-time personal data.
- Pipeline Diagnostics: bounded queue depth, before/after backpressure benchmark, partition lag.
- Run Evidence: recorded stress profile with environment/latency definition, replay logs and failure report.

Use controlled React updates (e.g. 1 Hz aggregated snapshots), request cancellation, memoized aggregated series, no rendering 40,000 individual DOM markers. Consider canvas/map clustering only if browser profiling shows a bottleneck. Validate via Playwright and Antigravity browser; include mobile/tablet fallback for the graph and accessible table alternative to map visuals.

## 16. Antigravity execution plan

The lead agent first establishes **one complete source → Kafka → Go worker → Redis → geofence → output topic** test. Once it works, freeze the event/topic schema and delegate isolated branches/worktrees:

| Agent | Ownership | Acceptance evidence |
|---|---|---|
| `simulator-agent` | seeded routes, event generator, profiles | Known device cardinality, controlled rate, source manifest |
| `ingest-agent` | Kafka worker, validation, offset semantics | Replay/idempotency tests and consumer traces |
| `geo-state-agent` | Redis atomic updates, polygon processing | Boundary, duplicate, ordering and transition tests |
| `storage-agent` | PG events/outbox, batch/persistence | Transaction/restart/recovery proofs |
| `perf-agent` | Independent unbounded/bounded experiment and 80K stress | Raw pprof/histogram/throughput logs, environment |
| `platform-agent` | Compose, K8s, monitoring, optional AWS | Actual deploy/logs/alerts or explicit external blocker |
| `ui-agent` | React monitoring/replay | Real browser screenshots and console/network verification |
| `interview-agent` | Read-only source/evidence review at end | Cited question bank with no unverifiable claims |

Use Antigravity custom agents in `.agents/agents/*.md`, project rules, task artifacts, separate worktrees for concurrent changes, and browser subagent for real UI playtesting. Agents modifying schemas/contracts cannot merge without producer/consumer integration tests. Lead agent owns final reconciled evidence, not each worker's optimistic completion note.

### Workspace rule `.agents/rules/project-focus.md`

```md
---
trigger: always_on
---
Primary goal: reproduce GeoStream resume claims with correct Go/Kafka/Redis/Postgres code and measured data.
Do not claim 80K/s, p99 <12ms, or a 22% regression without an actual run with methodology and raw artifacts.
Always separate ingestion latency from end-to-end transition latency and browser freshness.
Never silently drop events to improve throughput.
Treat Kafka replays as possible; downstream business effects must be idempotent.
Do not expose real courier/user locations. Only synthetic routes and fixtures.
Do not run high-volume stress against public hosted mapping/geocoding APIs.
Do not build extra cloud services or complex frontend features absent explicit acceptance value.
One working vertical slice is required before independent subagent fan-out.
Cloud spending requires budget approval, logging and teardown plan.
```

### Initial Antigravity lead-agent prompt

```text
Implement GeoStream from this plan as a real Go/Kafka/Redis/PostgreSQL location-event pipeline, using only synthetic couriers and seeded routes. First make a vertical slice that emits outside->inside->outside pings, proves one enter and one exit, and writes latest position to Redis. Next freeze the schema, add replay-safe deduplication and downstream events, offset reconciliation, bounded Postgres batching, stress simulator, Prometheus/Grafana, Kubernetes, an isolated intentionally unbounded-buffer regression and the bounded-channel fix. Measure generated/processed throughput and p99 for explicitly named boundaries; never invent the resume's 40K/80K/12ms/22% figures. Use optional AWS only with credentials/budget and automatic cleanup. Build a small React monitoring/replay UI after the core pipeline is verified. Generate implementation-specific interview preparation and capture all raw evidence and failure runs.
```

## 17. Mandatory commands, artifacts and gates

```text
make setup
make seed-routes
make dev
make test
make test-race
make smoke
make integration
make reconcile RUN_ID=...
make chaos
make bench-local
make bench-stress       # approval/capacity-gated
make bench-backpressure
make dashboards
make k8s-dev
make cloud-plan        # opt-in
make cloud-destroy     # opt-in
```

**Gates:**

- **0:** reproducible local toolchain, deterministic data, schema, Docker Compose.
- **1:** simple enter/exit roundtrip, Redis latest sequence assertion, real Kafka topic.
- **2:** invalid/duplicate/out-of-order ping handling and automated reconciliation/replay.
- **3:** PostgreSQL persistent transitions/outbox and restart correctness.
- **4:** isolated bounded vs unbounded experiment with unchanged output counts.
- **5:** scaled generator and sustained throughput/p99 measurements, resources and caveats recorded.
- **6:** Prometheus/Grafana dashboards, tested alert, Kubernetes profile, CI.
- **7:** optional AWS actually executed or declared blocked; teardown and cost evidence.
- **8:** browser-verified review screen and implementation-grounded interview packet.

For each run save `evidence/<topic>/<run-id>/{environment.json,commit.txt,config.json,generator.json,pipeline.json,latency-histograms.json,kafka-offsets.json,raw-logs/,summary.md}`. No benchmark summaries without the raw samples and exact scripts. For a failed or incomplete stress run, persist actual rate and reason.

## 18. Interview package — Go systems depth and frontend relevance

Generate `interview/GEOSTREAM_INTERVIEW_GUIDE.md`, `DECISIONS.md`, `FAILURE_STORIES.md`, `LOAD_TEST_REPORT.md` and `FRONTEND_INTEGRATION.md` after code and benchmarks exist. Each question has 30-second, 90-second and deep-dive answers, exact code/config locations, measured evidence, choices/alternatives, failure modes, 2–4 skeptical follow-ups. **Minimum: 50 primary questions, 100 follow-ups, 20 debugging scenarios, 6 design prompts.**

Core areas:

- Kafka partition keys, device ordering, consumer groups, rebalancing, lag, offset commits, at-least-once processing, deduplication and downstream replay semantics.
- Why Redis? TTL, atomic compare/update, stale sequences, crash recovery, cache vs durability, Lua or transactions.
- Geofence point-in-polygon, data structures, spatial index, boundary/hysteresis rules, GPS jitter, transitions, duplicate suppression.
- Go concurrency: goroutines/channels, worker pools, context cancellation, race detection, DB pool, backpressure, buffer growth, profiling with pprof, deadlocks and graceful shutdown.
- PostgreSQL outbox, idempotency constraints, batching vs per-message inserts, dual-write failure window.
- Load generator fidelity, 40K active vs 40K connected, 80K generated vs accepted/processed per second, sustained window, p99 histogram and latency scope.
- Prometheus/Grafana metrics/alerts, pod CPU/memory limits, autoscaling, Kafka partition ceiling, Kubernetes debugging, AWS cost tradeoffs.
- Apple-style frontend connections: efficient React monitoring dashboard, BFF aggregation, map performance and controlled updates, error/loading states, alert UX, why this backend project does or does not translate to Apple Ads frontend work.

**Mandatory skeptical interviewer scenarios:**

1. "What exactly do you mean by 40,000 active devices?"
2. "How did you measure sustained 80,000 events/sec? Did producers and consumers match?"
3. "Is the sub-12ms p99 from Kafka ingestion or end to end including PostgreSQL?"
4. "If Kafka retries after the DB transaction commits, what prevents duplicate geofence events?"
5. "How can you commit offsets safely across Kafka, Redis and Postgres?"
6. "What if Redis restarts and last device sequence disappears?"
7. "How do you handle out-of-order GPS timestamps but monotonically increasing sequence numbers?"
8. "What does your geofence engine do if a device jitters at the boundary?"
9. "Why not write every GPS ping directly to PostgreSQL?"
10. "What evidence proved the queue was unbounded and caused the regression?"
11. "How did bounded channels fix the issue without losing events?"
12. "How do you distinguish broker lag from slow Redis or slow DB?"
13. "If one Kafka partition becomes hot, how can you scale consumers?"
14. "What triggers the data-gap alert and how do you prove alerting works?"
15. "What happens during Kubernetes SIGTERM and a consumer rebalance?"
16. "How would you test a partial partition outage?"
17. "What changes at 10× traffic?"
18. "How do you visualize 40K couriers without 40K React DOM nodes?"
19. "What are the exact conditions of your 22% throughput degradation?"
20. "Why is GeoStream relevant to an Apple Ads frontend position?"

## 19. Explicit non-goals

Do not add real location tracking, personal accounts, payments, delivery dispatch algorithms, mobile clients, route optimization, a rich mapping design system, complex GIS tile serving, dozens of microservices, Spark/Flink, exactly-once claims without proof, or mandatory managed MSK. Preserve performance and correctness depth. Do not let the agent spend days polishing maps before Kafka replay and backpressure experiments work.

## 20. Final definition of done

GeoStream is complete only when real synthetic GPS events flow through Kafka and Go workers into Redis; real enter/exit transitions reach Kafka and are durably/auditably recorded; duplicate/out-of-order replay behavior is tested; offset reconciliation detects injected gaps; bounded backpressure is benchmarked against an isolated unbounded baseline with preserved output correctness; Prometheus/Grafana and Kubernetes actually run; simulated device cardinality, achieved throughput and p99 have honest measurements; reviewer UI displays true recorded/live data; optional AWS work is executed or explicitly blocked; and the interview packet grounds every answer in code or evidence.

## Technical references

- Apache Kafka client semantics: https://kafka.apache.org/documentation/
- Go profiling: https://go.dev/doc/diagnostics
- Prometheus instrumentation: https://prometheus.io/docs/practices/histograms/
- Kubernetes resources: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
- OpenStreetMap licensing: https://www.openstreetmap.org/copyright
- Antigravity rules/subagents: https://antigravity.google/docs/rules/ and https://antigravity.google/docs/subagents
