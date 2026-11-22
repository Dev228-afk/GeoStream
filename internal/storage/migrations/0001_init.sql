-- Canonical GeoStream PostgreSQL 16 schema (7 tables per V2 Contract Section B)

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    seed BIGINT NOT NULL,
    profile TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('CREATED', 'RUNNING', 'COMPLETED', 'FAILED', 'ABORTED')),
    generated_count BIGINT NOT NULL DEFAULT 0,
    broker_acked_count BIGINT NOT NULL DEFAULT 0,
    config_sha256 TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS geofences (
    id TEXT NOT NULL,
    version INT NOT NULL,
    name TEXT NOT NULL,
    geometry JSONB NOT NULL,
    bbox JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, version)
);

CREATE TABLE IF NOT EXISTS device_fence_state (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    fence_id TEXT NOT NULL,
    fence_version INT NOT NULL,
    inside BOOLEAN NOT NULL,
    last_transition_seq BIGINT NOT NULL,
    last_transition_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, device_id, fence_id, fence_version)
);
CREATE INDEX IF NOT EXISTS idx_dfs_run_device ON device_fence_state (run_id, device_id);

CREATE TABLE IF NOT EXISTS geofence_transitions (
    transition_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    fence_id TEXT NOT NULL,
    fence_version INT NOT NULL,
    transition_type TEXT NOT NULL CHECK (transition_type IN ('ENTER', 'EXIT')),
    source_event_id TEXT NOT NULL,
    source_sequence BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gt_run_time_id ON geofence_transitions (run_id, event_time DESC, transition_id DESC);
CREATE INDEX IF NOT EXISTS idx_gt_dedupe_lookup ON geofence_transitions (run_id, device_id, source_sequence);

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY,
    transition_id TEXT NOT NULL UNIQUE REFERENCES geofence_transitions(transition_id) ON DELETE RESTRICT,
    destination_topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    publish_attempts INT NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox (published_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS partition_checkpoints (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
    consumer_group TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INT NOT NULL,
    last_seen_offset BIGINT NOT NULL DEFAULT -1,
    last_committed_offset BIGINT NOT NULL DEFAULT -1,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sequence_gap_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, consumer_group, topic, partition)
);

CREATE TABLE IF NOT EXISTS incidents (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    first_at TIMESTAMPTZ NOT NULL,
    last_at TIMESTAMPTZ NOT NULL,
    evidence_ref TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_incidents_run_first ON incidents (run_id, first_at DESC);

