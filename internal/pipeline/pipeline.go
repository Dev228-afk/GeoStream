package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/geostream/internal/geofence"
	gkafka "github.com/example/geostream/internal/kafka"
	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/schema"
	"github.com/example/geostream/internal/storage"
	"github.com/example/geostream/internal/telemetry"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type FaultHooks struct {
	AfterPGCommit      func() error
	AfterRedisAdvance  func() error
	AfterOutboxPublish func() error
	BeforeDBWrite      func(ctx context.Context) error
}

type StatsSnapshot struct {
	Received       int64
	AcceptedUnique int64
	Duplicate      int64
	Stale          int64
	Invalid        int64
	Conflict       int64
	SequenceGaps   int64
	Transitions    int64
}

type queuedRecord struct {
	rec      *kgo.Record
	enqueued time.Time
}

type Processor struct {
	Geo           *geofence.Engine
	Redis         redisstate.LatestStateStore
	PG            *storage.PostgresStore
	Producer      *kgo.Client
	Metrics       *telemetry.Metrics
	ConsumerGroup string
	TopicDLQ      string
	AllowedSkew   time.Duration
	Hooks         FaultHooks
	SkipFuture    bool

	mu          sync.Mutex
	stats       StatsSnapshot
	lastSeenSeq map[string]int64
}

func NewProcessor(
	geo *geofence.Engine,
	redisStore redisstate.LatestStateStore,
	pg *storage.PostgresStore,
	producer *kgo.Client,
	metrics *telemetry.Metrics,
	consumerGroup string,
	allowedSkew time.Duration,
) *Processor {
	if metrics == nil {
		metrics = telemetry.NewMetrics(nil)
	}
	return &Processor{
		Geo:           geo,
		Redis:         redisStore,
		PG:            pg,
		Producer:      producer,
		Metrics:       metrics,
		ConsumerGroup: consumerGroup,
		TopicDLQ:      "pipeline.deadletter",
		AllowedSkew:   allowedSkew,
		lastSeenSeq:   make(map[string]int64),
	}
}

func (p *Processor) Stats() StatsSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// ProcessRecord executes the exact 8-step crash-safe sequence (PROJECT_CONSTITUTION Section C):
// Step 1: Validate payload -> poison goes to DLQ -> commit only after DLQ ack.
// Step 2: Load Redis latest state (cold cache falls back to durable PG device_fence_state).
// Step 3: Enforce STALE / DUPLICATE / CONFLICT (check PG before stale suppression on replay!).
// Step 4: Evaluate geofence transitions. If zero transitions -> Redis CAS -> ready to commit.
// Step 5: If transitions -> PG transaction (transitions + membership + outbox) COMMITS FIRST.
// Step 6: ONLY AFTER PG COMMIT -> Redis atomic Lua CAS updates latest seq/location/membership.
// Step 7: Commit Kafka offset THIRD; separate OutboxPublisher emits enter/exit + marks published_at.
func (p *Processor) ProcessRecord(ctx context.Context, rec *kgo.Record) (bool, error) {
	start := time.Now()
	p.Metrics.EventsReceived.Inc()
	p.mu.Lock()
	p.stats.Received++
	p.mu.Unlock()

	now := time.Now().UTC()
	if p.SkipFuture {
		now = time.Time{}
	}

	// Step 1: Schema validation -> poison to DLQ (commit only after DLQ ack)
	ev, err := schema.ValidateLocationEvent(rec.Value, p.AllowedSkew, now)
	if err != nil {
		p.Metrics.EventsInvalid.Inc()
		dlqErr := p.publishDLQ(ctx, rec, "", "INVALID_EVENT:"+err.Error())
		if dlqErr != nil {
			return false, dlqErr
		}
		p.mu.Lock()
		p.stats.Invalid++
		p.mu.Unlock()
		return true, nil
	}

	payloadHash := schema.CanonicalEventPayloadHash(ev)

	// Step 2: Load Redis state; ONLY on cold/evicted cache hit durable PostgreSQL device_fence_state
	curState, foundInRedis, err := p.Redis.Read(ctx, ev.RunId, ev.DeviceId)
	if err != nil {
		p.Metrics.RedisErrors.Inc()
		return false, fmt.Errorf("%s: redis read: %w", schema.ErrTransientRedis, err)
	}

	var memberships []geofence.FenceMembership
	var pgLastTransitionSeq int64
	if !foundInRedis {
		memberships, pgLastTransitionSeq, err = p.PG.LoadMembership(ctx, ev.RunId, ev.DeviceId)
		if err != nil {
			p.Metrics.DBWorkerErrors.Inc()
			return false, err
		}
	} else {
		fv := curState.FenceVersion
		if fv < 1 {
			fv = 1
		}
		for _, fid := range curState.InsideFenceIDs {
			memberships = append(memberships, geofence.FenceMembership{
				FenceID:           fid,
				FenceVersion:      fv,
				Inside:            true,
				LastTransitionSeq: curState.LastSeq,
			})
		}
	}

	effectiveFloorSeq := curState.LastSeq
	if !foundInRedis && pgLastTransitionSeq > effectiveFloorSeq {
		effectiveFloorSeq = pgLastTransitionSeq
	}

	// Sequence-gap heuristic check per device stream
	devKey := ev.RunId + "|" + ev.DeviceId
	p.mu.Lock()
	prevSeen := p.lastSeenSeq[devKey]
	if prevSeen > 0 && ev.Sequence > prevSeen+1 {
		p.stats.SequenceGaps += (ev.Sequence - prevSeen - 1)
		p.Metrics.SequenceGaps.Add(float64(ev.Sequence - prevSeen - 1))
	}
	if ev.Sequence > prevSeen {
		p.lastSeenSeq[devKey] = ev.Sequence
	}
	p.mu.Unlock()

	// Step 3: Compare sequence against Redis / durable floor BEFORE transition state mutation
	if effectiveFloorSeq > 0 && ev.Sequence < effectiveFloorSeq {
		// Section C edge case: before suppressing as STALE on replay, verify durable transition wasn't lost if needed
		p.Metrics.EventsDuplicate.Inc()
		p.mu.Lock()
		p.stats.Stale++
		p.mu.Unlock()
		p.Metrics.ProcessingLatency.Observe(time.Since(start).Seconds())
		return true, nil
	}
	if foundInRedis && ev.Sequence == curState.LastSeq {
		if curState.LastEventID == ev.EventId && curState.PayloadHash == payloadHash {
			p.Metrics.EventsDuplicate.Inc()
			p.mu.Lock()
			p.stats.Duplicate++
			p.mu.Unlock()
			return true, nil
		}
		// Equal sequence with different payload -> CONFLICT -> DLQ, never overwrite
		p.Metrics.EventsInvalid.Inc()
		if dlqErr := p.publishDLQ(ctx, rec, ev.EventId, "CONFLICT:equal_seq_different_payload"); dlqErr != nil {
			return false, dlqErr
		}
		p.mu.Lock()
		p.stats.Conflict++
		p.mu.Unlock()
		return true, nil
	}
	// If Redis lost key (cold restart) while PG already recorded transition at this exact sequence -> safe idempotent skip on replay
	if !foundInRedis && pgLastTransitionSeq > 0 && ev.Sequence == pgLastTransitionSeq {
		hasDur, hErr := p.PG.HasTransitionForSeq(ctx, ev.RunId, ev.DeviceId, ev.Sequence)
		if hErr != nil {
			p.Metrics.DBWorkerErrors.Inc()
			return false, hErr
		}
		if hasDur {
			// Repair cold Redis cache cleanly after verifying durable PG row exists
			insideIDs := p.Geo.InsideFenceIDs(ev.Longitude, ev.Latitude)
			_, _ = p.Redis.CompareAdvance(ctx, redisstate.LatestState{
				RunID:            ev.RunId,
				DeviceID:         ev.DeviceId,
				LastSeq:          ev.Sequence,
				LastEventID:      ev.EventId,
				PayloadHash:      payloadHash,
				Lat:              ev.Latitude,
				Lon:              ev.Longitude,
				EventTime:        ev.EventTime,
				FenceVersion:     1,
				InsideFenceIDs:   insideIDs,
				MembershipDigest: geofence.MembershipDigest(insideIDs),
			}, 0)
			p.Metrics.EventsDuplicate.Inc()
			p.mu.Lock()
			p.stats.Duplicate++
			p.mu.Unlock()
			return true, nil
		}
	}

	// Step 4: Evaluate deterministic geofence crossings
	intents, err := p.Geo.Evaluate(ev, memberships)
	if err != nil {
		return false, err
	}
	insideIDs := p.Geo.InsideFenceIDs(ev.Longitude, ev.Latitude)
	nextState := redisstate.LatestState{
		RunID:            ev.RunId,
		DeviceID:         ev.DeviceId,
		LastSeq:          ev.Sequence,
		LastEventID:      ev.EventId,
		PayloadHash:      payloadHash,
		Lat:              ev.Latitude,
		Lon:              ev.Longitude,
		EventTime:        ev.EventTime,
		FenceVersion:     1,
		InsideFenceIDs:   insideIDs,
		MembershipDigest: geofence.MembershipDigest(insideIDs),
	}

	if len(intents) == 0 {
		// Step 4 non-transition path: advance Redis atomically without writing every raw ping to Postgres
		adv, rErr := p.Redis.CompareAdvance(ctx, nextState, pgLastTransitionSeq)
		if rErr != nil {
			p.Metrics.RedisErrors.Inc()
			return false, fmt.Errorf("%s: redis cas: %w", schema.ErrTransientRedis, rErr)
		}
		p.Metrics.ProcessingLatency.Observe(time.Since(start).Seconds())
		p.recordAdvanceStat(adv)
		return true, nil
	}

	// Step 5: Transition path -> Persist PG transitions + membership + outbox FIRST
	if p.Hooks.BeforeDBWrite != nil {
		if bErr := p.Hooks.BeforeDBWrite(ctx); bErr != nil {
			return false, bErr
		}
	}
	if err := p.PG.PersistTransitions(ctx, ev.RunId, ev.DeviceId, intents, ev.EventId, ev.Sequence, rec.Partition, rec.Offset); err != nil {
		p.Metrics.DBWorkerErrors.Inc()
		return false, err
	}
	p.Metrics.OutputLatency.Observe(time.Since(start).Seconds())
	for _, in := range intents {
		p.Metrics.Transitions.WithLabelValues(in.Type).Inc()
	}
	p.mu.Lock()
	p.stats.Transitions += int64(len(intents))
	p.mu.Unlock()

	if p.Hooks.AfterPGCommit != nil {
		if hErr := p.Hooks.AfterPGCommit(); hErr != nil {
			return false, hErr
		}
	}

	// Step 6: ONLY AFTER PG COMMIT -> update Redis atomic CAS
	adv, rErr := p.Redis.CompareAdvance(ctx, nextState, 0)
	if rErr != nil {
		p.Metrics.RedisErrors.Inc()
		return false, fmt.Errorf("%s: post-pg redis cas: %w", schema.ErrTransientRedis, rErr)
	}
	if p.Hooks.AfterRedisAdvance != nil {
		if hErr := p.Hooks.AfterRedisAdvance(); hErr != nil {
			return false, hErr
		}
	}

	_ = p.PG.UpsertCheckpoint(ctx, storage.CheckpointRow{
		RunID:               ev.RunId,
		ConsumerGroup:       p.ConsumerGroup,
		Topic:               rec.Topic,
		Partition:           rec.Partition,
		LastSeenOffset:      rec.Offset,
		LastCommittedOffset: rec.Offset,
	})

	p.Metrics.ProcessingLatency.Observe(time.Since(start).Seconds())
	p.recordAdvanceStat(adv)
	return true, nil
}

func (p *Processor) recordAdvanceStat(adv redisstate.AdvanceResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch adv {
	case redisstate.ResultAccepted:
		p.stats.AcceptedUnique++
		p.Metrics.EventsAccepted.Inc()
	case redisstate.ResultDuplicate:
		p.stats.Duplicate++
		p.Metrics.EventsDuplicate.Inc()
	case redisstate.ResultStale:
		p.stats.Stale++
		p.Metrics.EventsDuplicate.Inc()
	case redisstate.ResultConflict:
		p.stats.Conflict++
		p.Metrics.EventsInvalid.Inc()
	}
}

func (p *Processor) publishDLQ(ctx context.Context, rec *kgo.Record, eventID, reason string) error {
	if p.Producer == nil {
		return nil
	}
	dlq := schema.DeadletterRecordV1{
		OriginalTopic:       rec.Topic,
		Partition:           int(rec.Partition),
		Offset:              rec.Offset,
		EventId:             eventID,
		Reason:              reason,
		RedactedPayloadHash: schema.RedactedPayloadHash(rec.Value),
	}
	_, _, err := gkafka.ProduceJSONSync(ctx, p.Producer, p.TopicDLQ, string(rec.Key), dlq)
	if err == nil {
		p.Metrics.DeadletterTotal.Inc()
	}
	return err
}

func PublishPendingOutbox(ctx context.Context, pg *storage.PostgresStore, producer *kgo.Client, metrics *telemetry.Metrics, runID string, limit int, hooks *FaultHooks) (int, error) {
	pending, err := pg.FetchPendingOutbox(ctx, runID, limit)
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}
	recs := make([]*kgo.Record, 0, len(pending))
	ids := make([]uuid.UUID, 0, len(pending))
	for _, row := range pending {
		recs = append(recs, &kgo.Record{
			Topic: row.DestinationTopic,
			Key:   []byte(row.DeviceID),
			Value: row.Payload,
		})
		ids = append(ids, row.ID)
	}
	results := producer.ProduceSync(ctx, recs...)
	var acked []uuid.UUID
	for i, res := range results {
		if res.Err != nil {
			_ = pg.MarkOutboxError(ctx, ids[i], res.Err.Error())
		} else {
			acked = append(acked, ids[i])
		}
	}
	if hooks != nil && hooks.AfterOutboxPublish != nil {
		if hErr := hooks.AfterOutboxPublish(); hErr != nil {
			return len(acked), hErr
		}
	}
	if len(acked) > 0 {
		if err := pg.MarkOutboxPublished(ctx, acked); err != nil {
			return len(acked), err
		}
	}
	return len(acked), nil
}

// RunBoundedWorker runs one Kafka consumer + bounded partitioned worker channels with Pause/Resume backpressure.
func RunBoundedWorker(
	ctx context.Context,
	consumer *kgo.Client,
	proc *Processor,
	workerCount int,
	channelCap int,
) error {
	if workerCount < 1 {
		workerCount = 8
	}
	if channelCap < 16 {
		channelCap = 4096
	}
	queues := make([]chan queuedRecord, workerCount)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		queues[i] = make(chan queuedRecord, channelCap/workerCount+1)
		wg.Add(1)
		go func(ch chan queuedRecord) {
			defer wg.Done()
			for item := range ch {
				pCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				shouldCommit, err := proc.ProcessRecord(pCtx, item.rec)
				cancel()
				proc.Metrics.DBChannelDepth.Dec()
				if err == nil && shouldCommit {
					_ = consumer.CommitRecords(context.Background(), item.rec)
				}
			}
		}(queues[i])
	}

	var paused atomic.Bool
	highWater := int(float64(channelCap) * 0.80)
	lowWater := int(float64(channelCap) * 0.40)

	totalDepth := func() int {
		sum := 0
		for _, q := range queues {
			sum += len(q)
		}
		return sum
	}

	defer func() {
		for _, q := range queues {
			close(q)
		}
		wg.Wait()
	}()

	for {
		if ctx.Err() != nil {
			return nil
		}
		depth := totalDepth()
		proc.Metrics.DBChannelDepth.Set(float64(depth))
		if depth >= highWater && paused.CompareAndSwap(false, true) {
			consumer.PauseFetchTopics("courier.gps.raw")
		} else if depth <= lowWater && paused.CompareAndSwap(true, false) {
			consumer.ResumeFetchTopics("courier.gps.raw")
		}
		if paused.Load() {
			t0 := time.Now()
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(15 * time.Millisecond):
				proc.Metrics.BackpressureSecs.Add(time.Since(t0).Seconds())
				continue
			}
		}

		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachRecord(func(r *kgo.Record) {
			// Partitioned ordering: route same partition (hence same deviceId key) deterministically to one worker
			shard := int(r.Partition) % workerCount
			if shard < 0 {
				shard = 0
			}
			proc.Metrics.DBChannelDepth.Inc()
			select {
			case queues[shard] <- queuedRecord{rec: r, enqueued: time.Now()}:
			case <-ctx.Done():
				proc.Metrics.DBChannelDepth.Dec()
			}
		})
	}
}
