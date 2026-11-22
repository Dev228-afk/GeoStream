# GeoStream — Contract Index (Canonical V2 Reference)

Authoritative source: `PROJECT_CONSTITUTION.md` Sections A–G.

## 1. JSON Event & Transition Schemas
- `contracts/location-event.v1.schema.json`: `schemaVersion`, `runId`, `eventId` (deterministic UUIDv5/hash from `(runId, deviceId, sequence, seed)`), `deviceId`, `sequence (>=1)`, `routeId`, `eventTime` (RFC3339 ms UTC, <= +5m future), `producedAt`, `latitude [-90,90]`, `longitude [-180,180]`, `accuracyM [0,200]`.
- `contracts/geofence-transition.v1.schema.json`: `schemaVersion`, `transitionId` (`tr_` stable hash), `runId`, `deviceId`, `fenceId`, `fenceVersion`, `type ("ENTER"|"EXIT")`, `sourceEventId`, `sourceSequence`, `eventTime`.

## 2. Kafka Topics (key=`deviceId`)
- `courier.gps.raw` (`LocationEvent v1`, 16 partitions local / configurable)
- `geofence.enter` (`GeofenceTransition v1`)
- `geofence.exit` (`GeofenceTransition v1`)
- `pipeline.deadletter` (`{originalTopic, partition, offset, eventId, reason, redactedPayloadHash}`)

## 3. PostgreSQL Durable Tables (7 canonical tables)
- `runs`, `geofences` (`PK(id,version)`), `device_fence_state` (`PK(run_id,device_id,fence_id,fence_version)`), `geofence_transitions` (`PK(transition_id)`), `outbox` (`transition_id UNIQUE FK`), `partition_checkpoints` (`PK(run_id,consumer_group,topic,partition)`), `incidents`.

## 4. Redis Latest-State Hash
- Key: `geo:{runId}:device:{deviceId}` (TTL 24h); fields `lastSeq`, `lastEventId`, `lat`, `lon`, `eventTime`, `observedAt`, `fenceVersion`, `insideFenceIds`, `membershipDigest`.
- Updated ONLY via single atomic Lua CAS returning `ACCEPTED | DUPLICATE | STALE | CONFLICT`.

## 5. Crash-Safe 8-Step Sequence
1. Receive `(topic, partition, offset)` -> validate -> poison to DLQ -> commit only after DLQ ack.
2. Load Redis state (cold cache initializes membership from `device_fence_state`). Serialize per-partition / per-device.
3. `sequence < lastSeq` -> `STALE`; equal seq + same eventId -> `DUPLICATE` (verify uncommitted transition in PG!); equal seq + different payload -> `CONFLICT` -> DLQ.
4. Evaluate geofences (boundary=inside, hole=outside, overlaps independent). If no transition -> Redis CAS -> commit Kafka offset.
5. If transitions -> short PG tx (`geofence_transitions` + `device_fence_state` + `outbox`) -> **COMMIT PG FIRST**.
6. **ONLY AFTER PG COMMIT** -> Redis CAS. On Redis failure, do NOT commit offset.
7. Commit Kafka offset; separate `OutboxPublisher` emits downstream and sets `published_at`.
8. Crash between Step 5 & 6 repairs Redis on replay without duplicating PG/outbox transitions.

## 6. Read-Only UI REST Endpoints (`/api/v1` on `8200`)
- `GET /api/v1/runs`, `GET /api/v1/runs/:id/summary`, `GET /api/v1/runs/:id/devices`, `GET /api/v1/runs/:id/devices/:deviceId`, `GET /api/v1/runs/:id/geofences`, `GET /api/v1/runs/:id/transitions`, `GET /api/v1/runs/:id/timeseries`, `GET /api/v1/runs/:id/reconciliation`, `GET /api/v1/runs/:id/incidents`, `GET /api/v1/runs/:id/stream` (<=1Hz SSE), `GET /api/v1/health`.

