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
