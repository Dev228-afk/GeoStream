package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/geostream/internal/geofence"
	gkafka "github.com/example/geostream/internal/kafka"
	"github.com/example/geostream/internal/pipeline"
	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/schema"
	"github.com/example/geostream/internal/storage"
	"github.com/example/geostream/internal/telemetry"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

func loadFixturesDir() string {
	return filepath.Join("..", "..", "fixtures")
}

func TestGate1VerticalSliceAndStep5CrashRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	brokers := []string{"127.0.0.1:19092"}
	dsn := "postgres://geostream:geostream_local_2026@127.0.0.1:5437/geostream?sslmode=disable"

	pg, err := storage.NewPostgresStore(ctx, dsn, 8)
	if err != nil {
		t.Fatalf("PG connect: %v", err)
	}
	defer pg.Close()
	if err := pg.Migrate(ctx); err != nil {
		t.Fatalf("PG migrate (1): %v", err)
	}
	if err := pg.Migrate(ctx); err != nil {
		t.Fatalf("PG migrate idempotent (2): %v", err)
	}

	rcl := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6383"})
	if err := rcl.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis ping: %v", err)
	}
	defer rcl.Close()
	redisStore := redisstate.NewStore(rcl, time.Hour)

	if err := gkafka.EnsureTopics(ctx, brokers, 16); err != nil {
		t.Fatalf("Kafka EnsureTopics: %v", err)
	}

	gfBytes, err := os.ReadFile(filepath.Join(loadFixturesDir(), "geofences.geojson"))
	if err != nil {
		t.Fatalf("read geofences: %v", err)
	}
	fences, err := geofence.ParseGeoJSON(gfBytes)
	if err != nil {
		t.Fatalf("parse geofences: %v", err)
	}
	if err := pg.UpsertGeofences(ctx, fences); err != nil {
		t.Fatalf("upsert geofences: %v", err)
	}
	eng, err := geofence.NewEngine(fences, 0)
	if err != nil {
		t.Fatalf("geofence engine: %v", err)
	}

	prod, err := gkafka.NewProducer(brokers, 0)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer prod.Close()

	// Part A: Run Gate 1 smoke fixture (outside -> inside -> inside -> outside) + dup + stale
	runID := "run_gate1_" + time.Now().UTC().Format("150405")
	_ = pg.EnsureRun(ctx, storage.RunRecord{
		ID: runID, Seed: 2026, Profile: "smoke", StartedAt: time.Now().UTC(), Status: "RUNNING",
	})
	groupID := "cg_" + runID
	if err := gkafka.SeekGroupToEnd(ctx, brokers, groupID, "courier.gps.raw"); err != nil {
		t.Fatalf("SeekGroupToEnd: %v", err)
	}
	cons, err := gkafka.NewConsumer(brokers, groupID, []string{"courier.gps.raw"})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer cons.Close()

	f, err := os.Open(filepath.Join(loadFixturesDir(), "smoke_4events.jsonl"))
	if err != nil {
		t.Fatalf("open smoke_4events.jsonl: %v", err)
	}
	var smokeEvents []schema.LocationEventV1
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ev schema.LocationEventV1
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			t.Fatalf("unmarshal smoke line: %v", err)
		}
		ev.RunId = runID
		ev.EventId = schema.GenerateEventID(runID, ev.DeviceId, ev.Sequence, 2026)
		nowStr := time.Now().UTC().Format(time.RFC3339Nano)
		ev.EventTime = nowStr
		ev.ProducedAt = nowStr
		smokeEvents = append(smokeEvents, ev)
		_, _, err = gkafka.ProduceJSONSync(ctx, prod, "courier.gps.raw", ev.DeviceId, ev)
		if err != nil {
			t.Fatalf("produce smoke event: %v", err)
		}
	}
	_ = f.Close()

	// Append duplicate seq 2 AND out-of-order stale seq 1 after seq 4
	_, _, _ = gkafka.ProduceJSONSync(ctx, prod, "courier.gps.raw", smokeEvents[1].DeviceId, smokeEvents[1])
	_, _, _ = gkafka.ProduceJSONSync(ctx, prod, "courier.gps.raw", smokeEvents[0].DeviceId, smokeEvents[0])

	proc := pipeline.NewProcessor(eng, redisStore, pg, prod, telemetry.NewMetrics(nil), groupID, 5*time.Minute)
	proc.SkipFuture = true

	processed := 0
	for processed < 6 {
		fetches := cons.PollFetches(ctx)
		if err := fetches.Err(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("poll err: %v", err)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			var probe schema.LocationEventV1
			_ = json.Unmarshal(rec.Value, &probe)
			if probe.RunId != runID {
				_ = cons.CommitRecords(ctx, rec)
				return
			}
			ok, pErr := proc.ProcessRecord(ctx, rec)
			if pErr != nil || !ok {
				t.Fatalf("ProcessRecord err=%v ok=%v", pErr, ok)
			}
			if cErr := cons.CommitRecords(ctx, rec); cErr != nil {
				t.Fatalf("commit record: %v", cErr)
			}
			processed++
		})
	}

	publishedCount, err := pipeline.PublishPendingOutbox(ctx, pg, prod, proc.Metrics, runID, 50, nil)
	if err != nil || publishedCount != 2 {
		t.Fatalf("expected 2 outbox transitions (1 ENTER, 1 EXIT), got published=%d err=%v", publishedCount, err)
	}

	rows, _, err := pg.ListTransitions(ctx, runID, "", "", 20)
	if err != nil || len(rows) != 2 {
		t.Fatalf("expected 2 durable transitions in PG, got len=%d err=%v", len(rows), err)
	}

	state, ok, err := redisStore.Read(ctx, runID, "courier-000001")
	if err != nil || !ok || state.LastSeq != 4 || len(state.InsideFenceIDs) != 0 {
		t.Fatalf("expected Redis final seq=4 & 0 inside fences, got ok=%v err=%v state=%+v", ok, err, state)
	}

	// Verify downstream Kafka topics geofence.enter & geofence.exit received real transitions
	verifyTopicHasRun := func(topic, expectedType string) {
		c2, _ := gkafka.NewConsumer(brokers, "verify_"+runID+"_"+topic, []string{topic})
		defer c2.Close()
		found := false
		dl := time.Now().Add(6 * time.Second)
		for !found && time.Now().Before(dl) {
			fs := c2.PollFetches(ctx)
			fs.EachRecord(func(r *kgo.Record) {
				var tr schema.GeofenceTransitionV1
				if json.Unmarshal(r.Value, &tr) == nil && tr.RunId == runID && tr.Type == expectedType {
					found = true
				}
			})
		}
		if !found {
			t.Fatalf("did not observe %s transition in Kafka topic %s for run %s", expectedType, topic, runID)
		}
	}
	verifyTopicHasRun("geofence.enter", "ENTER")
	verifyTopicHasRun("geofence.exit", "EXIT")

	// Part B: Crash injection BETWEEN Step 5 (PG commit) and Step 6 (Redis CAS) + Kafka replay
	crashRun := runID + "_crash56"
	_ = pg.EnsureRun(ctx, storage.RunRecord{
		ID: crashRun, Seed: 2026, Profile: "chaos-step56", StartedAt: time.Now().UTC(), Status: "RUNNING",
	})
	crashGroup := "cg_" + crashRun
	_ = gkafka.SeekGroupToEnd(ctx, brokers, crashGroup, "courier.gps.raw")
	evCrash := smokeEvents[1] // inside fence-004 -> triggers ENTER
	evCrash.RunId = crashRun
	evCrash.Sequence = 1
	evCrash.EventId = schema.GenerateEventID(crashRun, evCrash.DeviceId, 1, 2026)

	cProd, _ := gkafka.NewConsumer(brokers, crashGroup, []string{"courier.gps.raw"})
	defer cProd.Close()

	_, _, err = gkafka.ProduceJSONSync(ctx, prod, "courier.gps.raw", evCrash.DeviceId, evCrash)
	if err != nil {
		t.Fatalf("produce crash event: %v", err)
	}

	procCrash := pipeline.NewProcessor(eng, redisStore, pg, prod, telemetry.NewMetrics(nil), crashGroup, 5*time.Minute)
	procCrash.SkipFuture = true
	procCrash.Hooks.AfterPGCommit = func() error {
		return errors.New("SIMULATED_CRASH_BETWEEN_STEP5_PG_AND_STEP6_REDIS")
	}

	var crashRec *kgo.Record
	for crashRec == nil {
		fs := cProd.PollFetches(ctx)
		fs.EachRecord(func(r *kgo.Record) {
			var probe schema.LocationEventV1
			if json.Unmarshal(r.Value, &probe) == nil && probe.RunId == crashRun {
				crashRec = r
			}
		})
	}
	// First attempt commits PG (Step 5) then crashes BEFORE Redis CAS (Step 6) -> MUST NOT commit Kafka offset
	_, crashErr := procCrash.ProcessRecord(ctx, crashRec)
	if crashErr == nil {
		t.Fatalf("expected simulated crash error after Step 5 PG commit")
	}
	// Verify PG persisted ENTER transition while Redis does NOT have crashRun state yet!
	if _, foundBeforeRecovery, _ := redisStore.Read(ctx, crashRun, evCrash.DeviceId); foundBeforeRecovery {
		t.Fatalf("Redis must NOT have advanced before crash!")
	}
	preRecCount, _, _, _, _, _ := pg.GetReconciliationCounts(ctx, crashRun)
	if preRecCount != 1 {
		t.Fatalf("expected 1 durable transition in PG right before Step 5 crash, got %d", preRecCount)
	}

	// Worker restarts (no fault hook) and replays exact uncommitted Kafka record -> repairs Redis, keeps PG count == 1!
	procRecover := pipeline.NewProcessor(eng, redisStore, pg, prod, telemetry.NewMetrics(nil), crashGroup, 5*time.Minute)
	procRecover.SkipFuture = true
	okRec, recErr := procRecover.ProcessRecord(ctx, crashRec)
	if recErr != nil || !okRec {
		t.Fatalf("replay after Step 5/6 crash failed: err=%v ok=%v", recErr, okRec)
	}
	_ = cProd.CommitRecords(ctx, crashRec)

	postRecCount, _, _, _, _, _ := pg.GetReconciliationCounts(ctx, crashRun)
	if postRecCount != 1 {
		t.Fatalf("replay duplicated PG transition! expected 1, got %d", postRecCount)
	}
	stRecover, okSt, _ := redisStore.Read(ctx, crashRun, evCrash.DeviceId)
	if !okSt || stRecover.LastSeq != 1 || len(stRecover.InsideFenceIDs) != 1 || stRecover.InsideFenceIDs[0] != "fence-004" {
		t.Fatalf("Redis was not repaired accurately on replay: ok=%v state=%+v", okSt, stRecover)
	}

	_ = pg.FinishRun(ctx, runID, "COMPLETED", 6, 6)
	_ = pg.FinishRun(ctx, crashRun, "COMPLETED", 1, 1)

	// Persist Gate 1 verification JSON evidence
	evDir := filepath.Join("..", "..", "evidence", "reconciliation")
	_ = os.MkdirAll(evDir, 0o755)
	summary := map[string]any{
		"runId":                runID,
		"crashRecoveryRunId":   crashRun,
		"smokeTransitions":     len(rows),
		"outboxPublished":      publishedCount,
		"step5To6CrashPGCount": postRecCount,
		"redisRecoveredInside": stRecover.InsideFenceIDs,
		"status":               "PASS",
	}
	b, _ := json.MarshalIndent(summary, "", "  ")
	_ = os.WriteFile(filepath.Join(evDir, "vertical-slice.json"), append(b, '\n'), 0o644)
}
