# GeoStream Chaos & Failure Recovery Matrix

| Scenario | Expected Category | PG Rows | Redis Seq | Status | Detail |
|---|---|---|---|---|---|
| `1_duplicate_delivery` | `DUPLICATE` | 1 | 1 | `PASS` | Second identical delivery ignored; PG count stayed 1 |
| `2_out_of_order_stale` | `STALE` | 1 | 2 | `PASS` | Lower sequence rejected without mutating Redis seq=2 |
| `3_sequence_conflict_dlq` | `CONFLICT->DLQ` | 0 | 1 | `PASS` | Equal sequence with mutated coordinates emitted to pipeline.deadletter |
| `4_poison_invalid_payload` | `INVALID_EVENT->DLQ` | 0 | 0 | `PASS` | Out-of-bounds GPS & malformed JSON quarantined to pipeline.deadletter |
| `5_crash_step5_to_6_replay` | `RECOVERED_IDEMPOTENT` | 1 | 1 | `PASS` | PG committed first; replay repaired Redis with 0 duplicate transitions |
| `6_crash_step6_to_7_replay` | `DUPLICATE_SAFE` | 1 | 1 | `PASS` | Replay classified duplicate safely while preserving PG durability |
| `7_outbox_crash_before_mark` | `OUTBOX_IDEMPOTENT` | 1 | 1 | `PASS` | Duplicate Kafka publish safe via stable transitionId; retry cleared published_at |
| `8_redis_eviction_cold_recovery` | `COLD_PG_HYDRATION` | 2 | 3 | `PASS` | Stale cache labeled cleanly; durable PG membership drove accurate EXIT |
| `9_transient_db_outage_retry` | `RETRY_RECOVERED` | 1 | 1 | `PASS` | Redis shielded from premature advance during DB outage; retry converged |
| `10_sequence_gap_reconcile` | `GAP_DETECTED_FAIL` | 1 | 3 | `PASS` | Missing seq 2 flagged sequence_gap_count=1 and Pass=false accurately |
