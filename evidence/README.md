# GeoStream — Evidence Provenance Policy

Each run records `evidence/<topic>/<run-id>/{environment.json,commit.txt,config.json,generator.json,pipeline.json,latency-histograms.json,kafka-offsets.json,raw-logs/,summary.md}`.

## Claim Classification
- `HISTORICAL_RESUME_CLAIM`: `22%` unbounded-buffer throughput drop. Never hardcoded in UI; compare actual measured bounded vs unbounded throughput under identical stream & DB latency.
- `TARGET`: `40,000` active devices / `80,000` events/sec sustained >=3m / `<12ms` p99 on Kubernetes. Only marked `REPRODUCED` if an actual sustained run achieves them; otherwise record achieved local throughput/latency and exact hardware ceiling.
- `REPRODUCED`: Smoke/local throughput, Step 5/6 crash recovery, chaos matrix, backpressure comparison, and live Prometheus alert drills with raw files.
- `BLOCKED_EXTERNAL`: Live EKS/AWS deployment and live k8s cluster rollout when `kind`/`helm`/cloud credentials are absent.

