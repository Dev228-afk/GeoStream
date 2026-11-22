# GeoStream — Agent Operating Contract

1. **Source of Truth**: [PROJECT_CONSTITUTION.md](file:///C:/Users/devan/OneDrive/Desktop/apple_ads/geostream/PROJECT_CONSTITUTION.md) (verbatim copy of `03_GeoStream_Antigravity_Implementation_Plan_V2.md`, SHA-256 `118A6DEAB30A6C6F1818F0E9EC1346C3995B43FD24ADB07365B9CD8B27E4EEFA`).
2. **Crash-Safe 8-Step Sequence (NON-NEGOTIABLE)**:
   - NEVER write Redis sequence/state ahead of a durable PostgreSQL geofence transition!
   - When a transition occurs: PostgreSQL transaction (`geofence_transitions` + `device_fence_state` + `outbox`) commits FIRST (Step 5), Redis atomic Lua CAS updates SECOND (Step 6), Kafka input offset commits THIRD (Step 7), and `OutboxPublisher` emits downstream `geofence.enter` / `geofence.exit` idempotently.
3. **Ports & Containers**:
   - Kafka KRaft `19092`, Postgres 16 `5437`, Redis 7 `6383` (NEVER `6379`), Prometheus `9095`, Alertmanager `9096`, Grafana `3100`, Go REST API `8200`, Go Worker metrics/health `8201`, React UI `5301`.
4. **Go & Concurrency Rules**:
   - Go module `github.com/example/geostream`; run `go test -race ./...` for all stateful code.
   - Bounded worker pool + fixed channel cap (4096); never spawn a goroutine per ping; never use `deviceId` as a Prometheus label.
5. **Tracking**: Update `TASKS.md`, `IMPLEMENTATION_STATUS.md`, and `NEXT_AGENT_HANDOFF.md` at every task boundary.
