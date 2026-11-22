# GeoStream — Frontend Integration & Apple Ads Engineering Relevance

This document maps GeoStream's React 19 + TypeScript observability console (`web/`) and BFF (`api/server.go`) to production frontend engineering criteria relevant to Apple Ads.

---

## 1. BFF Aggregation & UI Budget Enforcement

Rather than exposing Kafka, Redis, or PostgreSQL directly to the browser, [`api/server.go`](../api/server.go) exposes a strict read-only JSON + SSE surface:

| Route / Hook | Endpoint | UI Budget / Safeguard | Source File |
|---|---|---|---|
| Overview KPIs & Series | `GET /runs/{id}/summary`, `timeseries`, `incidents` | Separates `INGEST_INTERNAL` vs `PIPELINE_TRANSITION`; flags `LIVE` vs `RECORDED` mode | [`OverviewPage.tsx`](../web/src/routes/OverviewPage.tsx) |
| Real-Time Summary Stream | `GET /runs/{id}/stream` (SSE) | Emits `<= 1 Hz` summary counts (`activeDeviceCount`) instead of raw 80K/s ping firehose | [`server.go`](../api/server.go) |
| Spatial Map & Scrubber | `GET /runs/{id}/geofences`, `devices?limit=100`, `transitions` | Server & client both enforce `<= 100` SVG `<circle>` pins; replay throttled to `<= 5 fps` (`200 ms`) | [`MapReplayPage.tsx`](../web/src/routes/MapReplayPage.tsx) |
| Durable Audit Ledger | `GET /runs/{id}/transitions?cursor=...` | PostgreSQL keyset cursor pagination + lazy source-provenance detail drawer (`partition`/`offset`) | [`TransitionsPage.tsx`](../web/src/routes/TransitionsPage.tsx) |
| Backpressure & Chaos | `GET /evidence/{backpressure,chaos,alerts}` | Displays raw `run-bp-001` pprof stats, `10/10` chaos matrix, and live Alertmanager drill JSON | [`DiagnosticsPage.tsx`](../web/src/routes/DiagnosticsPage.tsx), [`EvidencePage.tsx`](../web/src/routes/EvidencePage.tsx) |

---

## 2. Visual Polish, Accessibility (WCAG 2.2 AA) & QA Verification

- **Tokens & Restraint**: [`web/src/styles/tokens.css`](../web/src/styles/tokens.css) uses high-contrast off-white surfaces (`--canvas: #f5f7f9`, `--surface: #ffffff`, `--ink: #17222b`, `--accent: #176a92`), tabular numerals (`.gs-num`), zero decorative gradients, and max 4 top KPI cards.
- **Accessible Map Alternative & Keyboard Support**: Every SVG geofence visualization is paired with a semantic courier table (`MapReplayPage.tsx:163-199`). Scrollable `.gs-table-wrap` wrappers and `<pre>` JSON inspectors carry `tabIndex={0} role="region" aria-label="..."` so Safari/keyboard users can scroll overflowing columns cleanly (`0` Axe `scrollable-region-focusable` violations).
- **Honest Degraded State (`03-metrics-unavailable-1440.png`)**: When Prometheus is unavailable (`status: "unavailable"`), `OverviewPage.tsx` hides interpolated chart lines and surfaces `"Metrics temporarily unavailable (no fabricated fallback points)"`.
- **Verified Browser & Unit Suites**:
  - `npx tsc --noEmit` & `pnpm run lint` (`eslint src e2e`): `0` errors.
  - `vitest run` (`src/tests/App.test.tsx`): `1/1 passed (499 ms)`.
  - `playwright test` (`e2e/geostream.spec.ts` + `@axe-core/playwright`): `1 passed (6.4 s)`, `0` console/page errors, `0` horizontal overflow, and `0` WCAG 2.2 AA violations across desktop (`1440x900`), tablet (`1024x768`), mobile (`390x844`), `~200%` zoom (`720x450`), and `prefers-reduced-motion: reduce` ([`evidence/ui/playwright-report.json`](../evidence/ui/playwright-report.json)).

---

## 3. Candid Assessment: Why GeoStream Does — and Does Not — Translate to Apple Ads Frontend

### Where It Directly Translates (High Signal)
1. **High-Cardinality Data Summarization**: Ad campaign dashboards summarize millions of ad impressions without locking the browser; GeoStream enforces the exact same architectural boundary—aggregating `1,476.88/s` local (`17,723` total events) to `80,000 events/s` `TARGET` telemetry down to cursor-paginated tables, `<=1 Hz` SSE summaries, and `<=100` SVG markers.
2. **Typed API Contracts & Honest Telemetry UX**: Generating [`contracts.gen.ts`](../web/src/api/contracts.gen.ts) from JSON Schema mirrors OpenAPI/GraphQL schema codegen at Apple scale; surfacing explicit `"unavailable"` states rather than misleading zeroed charts matches ad-reporting accuracy requirements.
3. **WCAG 2.2 AA Discipline**: Visual inspection of `01`..`08` screenshots plus automated Axe auditing mirrors enterprise compliance gates in Apple internal and customer consoles.

### Where It Does Not Translate (Honest Gap)
- GeoStream's primary complexity sits in Go distributed systems (Kafka offsets, Lua CAS, spatial winding, pprof backpressure) rather than complex rich-text editing, multi-step optimistic form mutations, or deep design-system component authoring—which is why **AdCanvas** (`adcanvas/`) and **SoloLedger** (`sololedger/`) carry the heavy frontend/form/BFF weight alongside GeoStream's backend-heavy depth.

