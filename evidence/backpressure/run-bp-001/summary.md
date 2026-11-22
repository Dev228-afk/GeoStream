# Backpressure Experiment (run-bp-001)

- **Classification**: REPRODUCED (compared against 22% throughput drop under unbounded buffer (HISTORICAL_RESUME_CLAIM); never hardcoded in UI)
- **Identical Persisted Transition Count**: 2400 == 2400 (zero silent drops)

| Metric | Unbounded Baseline (Isolated Faulty) | Bounded After Fix (Production Safe) |
|---|---|---|
| Input Events | 2400 | 2400 |
| Persisted Transitions | 2400 | 2400 |
| Throughput (events/sec) | 277.0 | 13932.8 |
| Max Queue Depth | 2288 | 256 |
| Peak Heap (MB) | 9.46 | 3.99 |
| GC Pause Total (ms) | 0.55 | 0.00 |
| p99 Latency (ms) | 8468.43 | 20.32 |
| Measured Regression vs Bounded | **98.0%** | — |
