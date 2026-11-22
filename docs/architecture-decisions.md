# GeoStream — Architecture Decisions

1. **PG-First Crash Safety**: Persist durable transitions, fence membership, and outbox rows in a single PostgreSQL transaction (Step 5) *before* advancing Redis state (Step 6) and *before* committing Kafka offsets (Step 7), avoiding the lost-transition window present in naive Redis-first designs.
2. **Go Client Stack**: Use `franz-go` (`kgo`) for Kafka partition-level pause/resume backpressure, `go-redis/v9` with atomic Lua CAS, `pgx/v5` pool, and `paulmach/orb` + explicit boundary-is-inside check.
3. **Single-Source Schema Codegen**: JSON Schemas under `contracts/` compile to `internal/schema/types_gen.go` and `web/src/api/contracts.gen.ts`.
4. **Honest Telemetry Boundaries**: Separate `ingest_processing_ms`, `pipeline_transition_ms`, and `user_visible_freshness_ms` histograms with sub-12ms buckets and bounded Prometheus label cardinality (never `deviceId`).
5. **Single-Node KRaft Kafka**: Apache Kafka `3.9.0` KRaft container in WSL2 Docker (`-Xmx384M`) keeps container footprint within the 2 GB WSL2 memory cap alongside Postgres, Redis, Prometheus, Alertmanager, and Grafana.
