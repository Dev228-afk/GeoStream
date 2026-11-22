package storage

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/example/geostream/internal/geofence"
	"github.com/example/geostream/internal/schema"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type RunRecord struct {
	ID               string     `json:"id"`
	Seed             int64      `json:"seed"`
	Profile          string     `json:"profile"`
	StartedAt        time.Time  `json:"startedAt"`
	EndedAt          *time.Time `json:"endedAt,omitempty"`
	Status           string     `json:"status"`
	GeneratedCount   int64      `json:"generatedCount"`
	BrokerAckedCount int64      `json:"brokerAckedCount"`
	ConfigSHA256     string     `json:"configSha256"`
}

type TransitionRow struct {
	TransitionID    string    `json:"transitionId"`
	RunID           string    `json:"runId"`
	DeviceID        string    `json:"deviceId"`
	FenceID         string    `json:"fenceId"`
	FenceVersion    int       `json:"fenceVersion"`
	Type            string    `json:"type"`
	SourceEventID   string    `json:"sourceEventId"`
	SourceSequence  int64     `json:"sourceSequence"`
	EventTime       time.Time `json:"eventTime"`
	CreatedAt       time.Time `json:"createdAt"`
	SourcePartition int32     `json:"sourcePartition"`
	SourceOffset    int64     `json:"sourceOffset"`
}

type OutboxRow struct {
	ID               uuid.UUID
	TransitionID     string
	DestinationTopic string
	Payload          json.RawMessage
	DeviceID         string
}

type CheckpointRow struct {
	RunID               string `json:"runId"`
	ConsumerGroup       string `json:"consumerGroup"`
	Topic               string `json:"topic"`
	Partition           int32  `json:"partition"`
	LastSeenOffset      int64  `json:"lastSeenOffset"`
	LastCommittedOffset int64  `json:"lastCommittedOffset"`
	SequenceGapCount    int64  `json:"sequenceGapCount"`
}

type IncidentRow struct {
	ID          string    `json:"id"`
	RunID       string    `json:"runId"`
	Kind        string    `json:"kind"`
	Severity    string    `json:"severity"`
	FirstAt     time.Time `json:"firstAt"`
	LastAt      time.Time `json:"lastAt"`
	EvidenceRef string    `json:"evidenceRef"`
}

type TransitionStore interface {
	PersistTransitions(ctx context.Context, runID, deviceID string, changes []geofence.TransitionIntent, eventID string, seq int64, partition int32, offset int64) error
	LoadMembership(ctx context.Context, runID, deviceID string) ([]geofence.FenceMembership, int64, error)
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string, maxConns int32) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: parse pg config: %w", schema.ErrTransientDB, err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	} else {
		cfg.MaxConns = 16
	}
	cfg.MinConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: connect pg: %w", schema.ErrTransientDB, err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	raw, err := migrationFS.ReadFile("migrations/0001_init.sql")
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(727001)"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, string(raw)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) EnsureRun(ctx context.Context, r RunRecord) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO runs (id, seed, profile, started_at, status, generated_count, broker_acked_count, config_sha256)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			generated_count = GREATEST(runs.generated_count, EXCLUDED.generated_count),
			broker_acked_count = GREATEST(runs.broker_acked_count, EXCLUDED.broker_acked_count)
	`, r.ID, r.Seed, r.Profile, r.StartedAt.UTC(), r.Status, r.GeneratedCount, r.BrokerAckedCount, r.ConfigSHA256)
	return err
}

func (s *PostgresStore) FinishRun(ctx context.Context, runID, status string, generated, acked int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE runs SET status = $2, ended_at = NOW(), generated_count = $3, broker_acked_count = $4 WHERE id = $1
	`, runID, status, generated, acked)
	return err
}

func (s *PostgresStore) UpsertGeofences(ctx context.Context, fences []geofence.Fence) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, f := range fences {
		bboxJSON, _ := json.Marshal(map[string]float64{
			"minLon": f.Bound.Min[0], "minLat": f.Bound.Min[1],
			"maxLon": f.Bound.Max[0], "maxLat": f.Bound.Max[1],
		})
		_, err := tx.Exec(ctx, `
			INSERT INTO geofences (id, version, name, geometry, bbox, enabled)
			VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
			ON CONFLICT (id, version) DO NOTHING
		`, f.ID, f.Version, f.Name, string(f.RawGeo), string(bboxJSON), f.Enabled)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) LoadMembership(ctx context.Context, runID, deviceID string) ([]geofence.FenceMembership, int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT fence_id, fence_version, inside, last_transition_seq
		FROM device_fence_state
		WHERE run_id = $1 AND device_id = $2
	`, runID, deviceID)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: load membership: %w", schema.ErrTransientDB, err)
	}
	defer rows.Close()

	var out []geofence.FenceMembership
	var maxSeq int64
	for rows.Next() {
		var m geofence.FenceMembership
		if err := rows.Scan(&m.FenceID, &m.FenceVersion, &m.Inside, &m.LastTransitionSeq); err != nil {
			return nil, 0, err
		}
		if m.LastTransitionSeq > maxSeq {
			maxSeq = m.LastTransitionSeq
		}
		out = append(out, m)
	}
	return out, maxSeq, rows.Err()
}

func (s *PostgresStore) HasTransitionForSeq(ctx context.Context, runID, deviceID string, seq int64) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM geofence_transitions
			WHERE run_id = $1 AND device_id = $2 AND source_sequence = $3
		)
	`, runID, deviceID, seq).Scan(&exists)
	return exists, err
}

// PersistTransitions writes geofence_transitions, device_fence_state, and outbox atomically in Step 5
// BEFORE Redis state advances or Kafka offsets commit.
func (s *PostgresStore) PersistTransitions(
	ctx context.Context,
	runID, deviceID string,
	changes []geofence.TransitionIntent,
	eventID string,
	seq int64,
	partition int32,
	offset int64,
) error {
	if len(changes) == 0 {
		return nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("%s: begin tx: %w", schema.ErrTransientDB, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, ch := range changes {
		evTime, err := time.Parse(time.RFC3339Nano, ch.EventTime)
		if err != nil {
			evTime = time.Now().UTC()
		}
		var insertedID string
		err = tx.QueryRow(ctx, `
			INSERT INTO geofence_transitions (
				transition_id, run_id, device_id, fence_id, fence_version,
				transition_type, source_event_id, source_sequence, event_time,
				source_partition, source_offset
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (transition_id) DO NOTHING
			RETURNING transition_id
		`, ch.TransitionID, runID, deviceID, ch.FenceID, ch.FenceVersion, ch.Type, eventID, ch.SourceSeq, evTime.UTC(), partition, offset).Scan(&insertedID)

		if err == pgx.ErrNoRows {
			// Verify existing row matches transition semantics (consistency check per Section C Step 5)
			var exType string
			var exSeq int64
			var exDev, exFence string
			vErr := tx.QueryRow(ctx, `
				SELECT device_id, fence_id, transition_type, source_sequence
				FROM geofence_transitions WHERE transition_id = $1
			`, ch.TransitionID).Scan(&exDev, &exFence, &exType, &exSeq)
			if vErr != nil {
				return fmt.Errorf("%s: verify conflict transition %s: %w", schema.ErrTransientDB, ch.TransitionID, vErr)
			}
			if exDev != deviceID || exFence != ch.FenceID || exType != ch.Type || exSeq != ch.SourceSeq {
				return fmt.Errorf("%s: transition_id collision mismatch for %s", schema.ErrConflict, ch.TransitionID)
			}
		} else if err != nil {
			return fmt.Errorf("%s: insert transition: %w", schema.ErrTransientDB, err)
		}

		inside := ch.Type == "ENTER"
		_, err = tx.Exec(ctx, `
			INSERT INTO device_fence_state (
				run_id, device_id, fence_id, fence_version, inside, last_transition_seq, last_transition_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
			ON CONFLICT (run_id, device_id, fence_id, fence_version) DO UPDATE SET
				inside = CASE WHEN EXCLUDED.last_transition_seq >= device_fence_state.last_transition_seq THEN EXCLUDED.inside ELSE device_fence_state.inside END,
				last_transition_seq = GREATEST(device_fence_state.last_transition_seq, EXCLUDED.last_transition_seq),
				last_transition_at = CASE WHEN EXCLUDED.last_transition_seq >= device_fence_state.last_transition_seq THEN EXCLUDED.last_transition_at ELSE device_fence_state.last_transition_at END,
				updated_at = NOW()
		`, runID, deviceID, ch.FenceID, ch.FenceVersion, inside, ch.SourceSeq, evTime.UTC())
		if err != nil {
			return fmt.Errorf("%s: upsert membership: %w", schema.ErrTransientDB, err)
		}

		destTopic := "geofence.enter"
		if ch.Type == "EXIT" {
			destTopic = "geofence.exit"
		}
		outPayload, _ := json.Marshal(schema.GeofenceTransitionV1{
			SchemaVersion:  1,
			TransitionId:   ch.TransitionID,
			RunId:          runID,
			DeviceId:       deviceID,
			FenceId:        ch.FenceID,
			FenceVersion:   ch.FenceVersion,
			Type:           ch.Type,
			SourceEventId:  eventID,
			SourceSequence: ch.SourceSeq,
			EventTime:      evTime.UTC().Format(time.RFC3339Nano),
		})
		outID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("outbox:"+ch.TransitionID))
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (id, transition_id, destination_topic, payload)
			VALUES ($1, $2, $3, $4::jsonb)
			ON CONFLICT (transition_id) DO NOTHING
		`, outID, ch.TransitionID, destTopic, string(outPayload))
		if err != nil {
			return fmt.Errorf("%s: insert outbox: %w", schema.ErrTransientDB, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%s: commit tx: %w", schema.ErrTransientDB, err)
	}
	return nil
}

func (s *PostgresStore) FetchPendingOutbox(ctx context.Context, runID string, limit int) ([]OutboxRow, error) {
	if limit <= 0 {
		limit = 200
	}
	var rows pgx.Rows
	var err error
	if runID != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT o.id, o.transition_id, o.destination_topic, o.payload, t.device_id
			FROM outbox o
			JOIN geofence_transitions t ON t.transition_id = o.transition_id
			WHERE o.published_at IS NULL AND t.run_id = $1
			ORDER BY t.event_time ASC, t.transition_id ASC
			LIMIT $2
		`, runID, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT o.id, o.transition_id, o.destination_topic, o.payload, t.device_id
			FROM outbox o
			JOIN geofence_transitions t ON t.transition_id = o.transition_id
			WHERE o.published_at IS NULL
			ORDER BY t.event_time ASC, t.transition_id ASC
			LIMIT $1
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []OutboxRow
	for rows.Next() {
		var r OutboxRow
		if err := rows.Scan(&r.ID, &r.TransitionID, &r.DestinationTopic, &r.Payload, &r.DeviceID); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, rows.Err()
}

func (s *PostgresStore) MarkOutboxPublished(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox SET published_at = NOW(), publish_attempts = publish_attempts + 1, last_error = NULL
		WHERE id = ANY($1::uuid[])
	`, strs)
	return err
}

func (s *PostgresStore) MarkOutboxError(ctx context.Context, id uuid.UUID, errMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox SET publish_attempts = publish_attempts + 1, last_error = $2 WHERE id = $1
	`, id, errMsg)
	return err
}

func (s *PostgresStore) UpsertCheckpoint(ctx context.Context, cp CheckpointRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO partition_checkpoints (
			run_id, consumer_group, topic, partition, last_seen_offset, last_committed_offset, last_seen_at, sequence_gap_count
		) VALUES ($1,$2,$3,$4,$5,$6,NOW(),$7)
		ON CONFLICT (run_id, consumer_group, topic, partition) DO UPDATE SET
			last_seen_offset = GREATEST(partition_checkpoints.last_seen_offset, EXCLUDED.last_seen_offset),
			last_committed_offset = GREATEST(partition_checkpoints.last_committed_offset, EXCLUDED.last_committed_offset),
			last_seen_at = NOW(),
			sequence_gap_count = partition_checkpoints.sequence_gap_count + EXCLUDED.sequence_gap_count
	`, cp.RunID, cp.ConsumerGroup, cp.Topic, cp.Partition, cp.LastSeenOffset, cp.LastCommittedOffset, cp.SequenceGapCount)
	return err
}

func (s *PostgresStore) UpsertIncident(ctx context.Context, inc IncidentRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO incidents (id, run_id, kind, severity, first_at, last_at, evidence_ref)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (id) DO UPDATE SET
			last_at = GREATEST(incidents.last_at, EXCLUDED.last_at),
			severity = EXCLUDED.severity,
			evidence_ref = EXCLUDED.evidence_ref
	`, inc.ID, inc.RunID, inc.Kind, inc.Severity, inc.FirstAt.UTC(), inc.LastAt.UTC(), inc.EvidenceRef)
	return err
}

func (s *PostgresStore) ListRuns(ctx context.Context, limit int) ([]RunRecord, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, seed, profile, started_at, ended_at, status, generated_count, broker_acked_count, config_sha256
		FROM runs ORDER BY started_at DESC, id DESC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunRecord
	for rows.Next() {
		var r RunRecord
		if err := rows.Scan(&r.ID, &r.Seed, &r.Profile, &r.StartedAt, &r.EndedAt, &r.Status, &r.GeneratedCount, &r.BrokerAckedCount, &r.ConfigSHA256); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PostgresStore) GetRun(ctx context.Context, runID string) (RunRecord, bool, error) {
	var r RunRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id, seed, profile, started_at, ended_at, status, generated_count, broker_acked_count, config_sha256
		FROM runs WHERE id = $1
	`, runID).Scan(&r.ID, &r.Seed, &r.Profile, &r.StartedAt, &r.EndedAt, &r.Status, &r.GeneratedCount, &r.BrokerAckedCount, &r.ConfigSHA256)
	if err == pgx.ErrNoRows {
		return r, false, nil
	}
	return r, err == nil, err
}

func (s *PostgresStore) ListGeofences(ctx context.Context) ([]json.RawMessage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT json_build_object(
			'type', 'Feature',
			'id', id,
			'properties', json_build_object('id', id, 'name', name, 'version', version, 'enabled', enabled),
			'geometry', CASE
				WHEN jsonb_typeof(geometry) = 'array' THEN jsonb_build_object('type', 'Polygon', 'coordinates', geometry)
				ELSE geometry
			END
		)
		FROM geofences WHERE enabled = TRUE ORDER BY id ASC, version DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []json.RawMessage
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		out = append(out, json.RawMessage(raw))
	}
	return out, rows.Err()
}

func (s *PostgresStore) ListTransitions(ctx context.Context, runID, typFilter, cursor string, limit int) ([]TransitionRow, string, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	args := []any{runID}
	conds := []string{"run_id = $1"}
	if t := strings.ToUpper(strings.TrimSpace(typFilter)); t == "ENTER" || t == "EXIT" {
		args = append(args, t)
		conds = append(conds, fmt.Sprintf("transition_type = $%d", len(args)))
	}
	if cursor != "" {
		parts := strings.SplitN(cursor, "|", 2)
		if len(parts) == 2 {
			if cTime, err := time.Parse(time.RFC3339Nano, parts[0]); err == nil {
				args = append(args, cTime.UTC(), parts[1])
				conds = append(conds, fmt.Sprintf("(event_time < $%d OR (event_time = $%d AND transition_id < $%d))", len(args)-1, len(args)-1, len(args)))
			}
		}
	}
	args = append(args, limit+1)
	query := fmt.Sprintf(`
		SELECT transition_id, run_id, device_id, fence_id, fence_version, transition_type,
		       source_event_id, source_sequence, event_time, created_at, source_partition, source_offset
		FROM geofence_transitions
		WHERE %s
		ORDER BY event_time DESC, transition_id DESC
		LIMIT $%d
	`, strings.Join(conds, " AND "), len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var items []TransitionRow
	for rows.Next() {
		var row TransitionRow
		if err := rows.Scan(&row.TransitionID, &row.RunID, &row.DeviceID, &row.FenceID, &row.FenceVersion, &row.Type,
			&row.SourceEventID, &row.SourceSequence, &row.EventTime, &row.CreatedAt, &row.SourcePartition, &row.SourceOffset); err != nil {
			return nil, "", err
		}
		items = append(items, row)
	}
	var nextCur string
	if len(items) > limit {
		last := items[limit-1]
		nextCur = fmt.Sprintf("%s|%s", last.EventTime.UTC().Format(time.RFC3339Nano), last.TransitionID)
		items = items[:limit]
	}
	return items, nextCur, rows.Err()
}

func (s *PostgresStore) ListIncidents(ctx context.Context, runID string, limit int) ([]IncidentRow, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, kind, severity, first_at, last_at, evidence_ref
		FROM incidents WHERE run_id = $1 ORDER BY last_at DESC LIMIT $2
	`, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IncidentRow
	for rows.Next() {
		var inc IncidentRow
		if err := rows.Scan(&inc.ID, &inc.RunID, &inc.Kind, &inc.Severity, &inc.FirstAt, &inc.LastAt, &inc.EvidenceRef); err != nil {
			return nil, err
		}
		out = append(out, inc)
	}
	return out, rows.Err()
}

func (s *PostgresStore) GetReconciliationCounts(ctx context.Context, runID string) (transitions int64, outboxPending int64, outboxPublished int64, gaps int64, checkpoints []CheckpointRow, err error) {
	if err = s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM geofence_transitions WHERE run_id = $1`, runID).Scan(&transitions); err != nil {
		return
	}
	if err = s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN o.published_at IS NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN o.published_at IS NOT NULL THEN 1 ELSE 0 END), 0)
		FROM outbox o
		JOIN geofence_transitions t ON t.transition_id = o.transition_id
		WHERE t.run_id = $1
	`, runID).Scan(&outboxPending, &outboxPublished); err != nil {
		return
	}
	rows, qErr := s.pool.Query(ctx, `
		SELECT run_id, consumer_group, topic, partition, last_seen_offset, last_committed_offset, sequence_gap_count
		FROM partition_checkpoints WHERE run_id = $1 ORDER BY partition ASC
	`, runID)
	if qErr != nil {
		err = qErr
		return
	}
	defer rows.Close()
	for rows.Next() {
		var cp CheckpointRow
		if err = rows.Scan(&cp.RunID, &cp.ConsumerGroup, &cp.Topic, &cp.Partition, &cp.LastSeenOffset, &cp.LastCommittedOffset, &cp.SequenceGapCount); err != nil {
			return
		}
		gaps += cp.SequenceGapCount
		checkpoints = append(checkpoints, cp)
	}
	err = rows.Err()
	return
}
