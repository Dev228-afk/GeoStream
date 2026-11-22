# GeoStream Tasks (`GEO-001`..`GEO-018`)

Allowed statuses: `NOT_STARTED`, `IN_PROGRESS`, `IMPLEMENTED_UNVERIFIED`, `VERIFIED`, `BLOCKED_EXTERNAL`, `BLOCKED_CONTRACT`, `FAILED_VERIFICATION`.

| Task ID | Gate | Scope | Evidence / Verification | Status |
|---|---|---|---|---|
| `GEO-001` | Gate 0 | Docker Compose stack (`kafka:19092`, `postgres:5437`, `redis:6383`, `--profile monitoring`), `.env`, `.gitignore` | `deploy/docker/docker-compose.yml` running under WSL2 `1.9 GiB` cap | `VERIFIED` |
| `GEO-002` | Gate 0 | Canonical JSON Schemas & Go/TS codegen (`scripts/gen-contracts.mjs`) | `node scripts/gen-contracts.mjs --check` -> exit `0` | `VERIFIED` |
| `GEO-003` | Gate 0 | PG 7-table schema, `advisory_xact_lock(727001)` migration, seeded fixtures (`--seed 2026`) | `fixtures/manifest.sha256` (`20` fences, `1,000` routes, `4` smoke events -> `2` expected) | `VERIFIED` |
| `GEO-004` | Gate 1 | Geofence geometry (`orb` + `pointOnRingBoundary`, holes, overlaps, `confirmDwell` hysteresis) | `internal/geofence` `-race` PASS (`engine_test.go`, `dwell_test.go`) | `VERIFIED` |
| `GEO-005` | Gate 1 | Redis atomic Lua CAS (`compareAdvanceLua`), `24h` TTL, active ZSET & concurrency tests | `internal/redisstate` `-race` PASS | `VERIFIED` |
| `GEO-006` | Gate 1 | Strict 8-step crash-safe `Processor.ProcessRecord` + Step 5->6 vertical slice integration | `tests/integration` PASS -> [`evidence/reconciliation/vertical-slice.json`](evidence/reconciliation/vertical-slice.json) | `VERIFIED` |
| `GEO-007` | Gate 2 | Deterministic IDs (`GenerateEventID`, `GenerateTransitionID`), `CanonicalEventPayloadHash`, `CONFLICT`/`INVALID` -> DLQ | `internal/schema` `-race` PASS | `VERIFIED` |
| `GEO-008` | Gate 2 | Reconciliation ledger CLI (`cmd/reconcile` :: `AuditRun`) & safe offset replay CLI (`cmd/replay`) | `reconcile --run-id run-local-001` (`pass: true`) + gap detection chaos scenario | `VERIFIED` |
| `GEO-009` | Gate 3 | 10-scenario Chaos Matrix (dup, stale, conflict, poison, Step 5/6 & 6/7 crashes, outbox, Redis eviction, DB outage, gap) | `tests/chaos` `-race` PASS (`10/10`) -> [`evidence/reconciliation/chaos-matrix.json`](evidence/reconciliation/chaos-matrix.json) | `VERIFIED` |
| `GEO-010` | Gate 3 | 15 bounded-cardinality Prometheus metrics (`0` `deviceId` labels, `[0.0005..1.0]` buckets) | `internal/telemetry` `-race` PASS | `VERIFIED` |
| `GEO-011` | Gate 4 | Unbounded vs. bounded backpressure benchmark + pprof CPU/heap profiles (`2400 == 2400`) | [`evidence/backpressure/run-bp-001/comparison.json`](evidence/backpressure/run-bp-001/comparison.json) (`98.0%` `REPRODUCED` vs `22%` `HISTORICAL_RESUME_CLAIM`) | `VERIFIED` |
| `GEO-012` | Gate 4 | Scaled local synthetic throughput runs (`run-smoke-001` `100` dev, `run-local-001` `1,500` dev) | [`evidence/throughput/run-local-001/summary.md`](evidence/throughput/run-local-001/summary.md) (`LOCAL_SINGLE_HOST_BENCHMARK`; `40K/80K/<12ms` `TARGET`) | `VERIFIED` |
| `GEO-013` | Gate 5 | Live Prometheus (`:9095`) + Alertmanager (`:9096`) firing & recovery drill + Grafana provisioning | [`evidence/alerts/alert-drill.json`](evidence/alerts/alert-drill.json) (`REPRODUCED` firing `18:07:28Z` -> recovered `18:08:25Z`) | `VERIFIED` |
| `GEO-014` | Gate 5 | Kubernetes manifests + Kustomize, Helm chart & AWS Terraform/CLOUD_APPROVAL (`BLOCKED_EXTERNAL`) | `kubectl kustomize deploy/k8s` PASS; [`CLOUD_APPROVAL.md`](CLOUD_APPROVAL.md) | `VERIFIED` |
| `GEO-015` | Gate 5 | Read-only REST BFF API (`:8200`, `<=100` sampled devices, `<=1Hz` SSE, GeoJSON normalization) | `api` `-race` PASS (`TestMonitoringAPIEndpointsAndErrorContracts`) | `VERIFIED` |
| `GEO-016` | Gate 5 | React 19 + TS monitoring console (`:5301`), `tsc`/`eslint`/`vitest`/`vite build`, Playwright + Axe 8-state QA | [`evidence/ui/playwright-report.json`](evidence/ui/playwright-report.json) (`1 passed (6.4s)`, `0` WCAG violations, `01`..`08` PNGs reviewed) | `VERIFIED` |
| `GEO-017` | Gate 6 | CI workflow (`.github/workflows/ci.yml`), `Makefile`, `README.md`, `docs/architecture-decisions.md` | [`README.md`](README.md) & CI/Makefile verified | `VERIFIED` |
| `GEO-018` | Gate 6 | Complete Interview Packet (`50` Qs / `100` follow-ups / `20` failure stories / `6` designs / `14` ADRs) + subagent audit + clean commit & teardown | Audited by `GeoStream Interview Auditor`; [`interview/GEOSTREAM_INTERVIEW_GUIDE.md`](interview/GEOSTREAM_INTERVIEW_GUIDE.md) | `VERIFIED` |
