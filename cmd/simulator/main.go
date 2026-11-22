package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/geostream/internal/config"
	"github.com/example/geostream/internal/geofence"
	gkafka "github.com/example/geostream/internal/kafka"
	"github.com/example/geostream/internal/pipeline"
	"github.com/example/geostream/internal/reconciliation"
	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/schema"
	"github.com/example/geostream/internal/storage"
	"github.com/example/geostream/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

func histQuantiles(mf *dto.MetricFamily) (p50, p95, p99 float64) {
	if mf == nil || len(mf.Metric) == 0 || mf.Metric[0].Histogram == nil {
		return 0, 0, 0
	}
	h := mf.Metric[0].Histogram
	total := h.GetSampleCount()
	if total == 0 {
		return 0, 0, 0
	}
	q := func(target float64) float64 {
		need := uint64(float64(total) * target)
		if need == 0 {
			need = 1
		}
		for _, b := range h.Bucket {
			if b.GetCumulativeCount() >= need {
				return b.GetUpperBound() * 1000.0
			}
		}
		return 1000.0
	}
	return q(0.50), q(0.95), q(0.99)
}

func main() {
	profile := flag.String("profile", "smoke", "smoke | dev | scale | resume-stress")
	runID := flag.String("run-id", "", "Run identifier")
	durationSec := flag.Int("duration", 12, "Duration in seconds")
	devicesOverride := flag.Int("devices", 0, "Override logical device count")
	rateOverride := flag.Int("rate", 0, "Override target events/sec")
	allowStress := flag.Bool("allow-stress", false, "Explicitly permit high-volume stress profile")
	outDir := flag.String("out", "", "Output evidence folder")
	flag.Parse()

	devices, targetRate := 100, 200
	switch *profile {
	case "smoke":
		devices, targetRate = 100, 200
	case "dev":
		devices, targetRate = 1000, 2000
	case "scale":
		if !*allowStress {
			fmt.Fprintln(os.Stderr, "scale profile requires --allow-stress flag")
			os.Exit(2)
		}
		devices, targetRate = 10000, 20000
	case "resume-stress":
		if !*allowStress {
			fmt.Fprintln(os.Stderr, "resume-stress (40K devices / 80K events/s TARGET) requires --allow-stress flag")
			os.Exit(2)
		}
		devices, targetRate = 40000, 80000
	default:
		fmt.Fprintf(os.Stderr, "unknown profile %q\n", *profile)
		os.Exit(2)
	}
	if *devicesOverride > 0 {
		devices = *devicesOverride
	}
	if *rateOverride > 0 {
		targetRate = *rateOverride
	}

	rid := *runID
	if rid == "" {
		rid = fmt.Sprintf("run_%s_%s", *profile, time.Now().UTC().Format("20060102_150405"))
	}
	dest := *outDir
	if dest == "" {
		dest = filepath.Join("evidence", "throughput", rid)
	}
	_ = os.MkdirAll(filepath.Join(dest, "raw-logs"), 0o755)

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*durationSec+45)*time.Second)
	defer cancel()

	pg, err := storage.NewPostgresStore(ctx, cfg.PostgresURL, 16)
	if err != nil {
		panic(err)
	}
	defer pg.Close()
	if err := pg.Migrate(ctx); err != nil {
		panic(err)
	}

	rcl := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 32})
	defer rcl.Close()
	rds := redisstate.NewStore(rcl, cfg.RedisTTL)

	if err := gkafka.EnsureTopics(ctx, cfg.KafkaBrokers, cfg.RawPartitions); err != nil {
		panic(err)
	}

	gfBytes, err := os.ReadFile(filepath.Join(cfg.FixturesDir, "geofences.geojson"))
	if err != nil {
		panic(err)
	}
	fences, _ := geofence.ParseGeoJSON(gfBytes)
	_ = pg.UpsertGeofences(ctx, fences)
	eng, _ := geofence.NewEngine(fences, cfg.DwellMS)

	prod, err := gkafka.NewProducer(cfg.KafkaBrokers, 1024*1024)
	if err != nil {
		panic(err)
	}
	defer prod.Close()

	groupID := "cg_" + rid
	_ = gkafka.SeekGroupToEnd(ctx, cfg.KafkaBrokers, groupID, cfg.TopicRaw)
	cons, err := gkafka.NewConsumer(cfg.KafkaBrokers, groupID, []string{cfg.TopicRaw})
	if err != nil {
		panic(err)
	}
	defer cons.Close()

	reg := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(reg)
	proc := pipeline.NewProcessor(eng, rds, pg, prod, metrics, groupID, cfg.AllowedFutureSkew)

	_ = pg.EnsureRun(ctx, storage.RunRecord{
		ID: rid, Seed: 2026, Profile: *profile, StartedAt: time.Now().UTC(), Status: "RUNNING",
	})

	// Launch bounded consumer worker + periodic outbox publisher
	workerCtx, workerCancel := context.WithCancel(ctx)
	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		_ = pipeline.RunBoundedWorker(workerCtx, cons, proc, cfg.WorkerPool, cfg.ChannelCap)
	}()
	go func() {
		tk := time.NewTicker(150 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-tk.C:
				_, _ = pipeline.PublishPendingOutbox(context.Background(), pg, prod, metrics, rid, 500, nil)
			}
		}
	}()

	// Seed synthetic logical devices on routes cycling outside/inside fence-004 periodically
	deviceIDs := make([]string, devices)
	for i := 0; i < devices; i++ {
		deviceIDs[i] = fmt.Sprintf("courier-%06d", i+1)
	}

	var generatedCount, ackedCount, produceErrCount int64
	tStart := time.Now()
	endTime := tStart.Add(time.Duration(*durationSec) * time.Second)
	tickInterval := 25 * time.Millisecond
	perTick := int(float64(targetRate) * tickInterval.Seconds())
	if perTick < 1 {
		perTick = 1
	}

	var seqCounters = make([]int64, devices)
	var ackWG sync.WaitGroup
	cursorIdx := 0

	ticker := time.NewTicker(tickInterval)
	for time.Now().Before(endTime) {
		<-ticker.C
		for k := 0; k < perTick; k++ {
			if time.Now().After(endTime) {
				break
			}
			dIdx := cursorIdx % devices
			cursorIdx++
			seqCounters[dIdx]++
			seq := seqCounters[dIdx]
			// Every 10th ping crosses fence-004 so transitions & PG outbox traffic stay real under load
			lon, lat := -121.895, 37.297
			if (seq/5)%2 == 1 {
				lon, lat = -121.880, 37.297
			}
			nowStr := time.Now().UTC().Format(time.RFC3339Nano)
			ev := schema.LocationEventV1{
				SchemaVersion: 1,
				RunId:         rid,
				EventId:       schema.GenerateEventID(rid, deviceIDs[dIdx], seq, 2026),
				DeviceId:      deviceIDs[dIdx],
				Sequence:      seq,
				RouteId:       fmt.Sprintf("route-%04d", (dIdx%1000)+1),
				EventTime:     nowStr,
				ProducedAt:    nowStr,
				Latitude:      lat,
				Longitude:     lon,
				AccuracyM:     6.0,
			}
			b, _ := json.Marshal(ev)
			atomic.AddInt64(&generatedCount, 1)
			ackWG.Add(1)
			prod.Produce(ctx, &kgo.Record{Topic: cfg.TopicRaw, Key: []byte(ev.DeviceId), Value: b}, func(_ *kgo.Record, pErr error) {
				defer ackWG.Done()
				if pErr != nil {
					atomic.AddInt64(&produceErrCount, 1)
				} else {
					atomic.AddInt64(&ackedCount, 1)
				}
			})
		}
	}
	ticker.Stop()
	ackWG.Wait()
	genElapsed := time.Since(tStart)

	// Wait for consumer catch-up up to 25s, stop workers, then drain remaining outbox completely
	drainDeadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(drainDeadline) {
		if proc.Stats().Received >= atomic.LoadInt64(&ackedCount) {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	workerCancel()
	workerWG.Wait()
	for {
		nPub, pErr := pipeline.PublishPendingOutbox(ctx, pg, prod, metrics, rid, 1000, nil)
		if pErr != nil || nPub == 0 {
			break
		}
	}

	stats := proc.Stats()
	gen := atomic.LoadInt64(&generatedCount)
	ack := atomic.LoadInt64(&ackedCount)
	_ = pg.FinishRun(ctx, rid, "COMPLETED", gen, ack)

	mfs, _ := reg.Gather()
	mfMap := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		mfMap[mf.GetName()] = mf
	}
	inP50, inP95, inP99 := histQuantiles(mfMap["geostream_processing_latency_seconds"])
	outP50, outP95, outP99 := histQuantiles(mfMap["geostream_output_latency_seconds"])

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	rep, _ := reconciliation.AuditRun(ctx, pg, rds, cfg.KafkaBrokers, rid, groupID, stats.Received, stats.AcceptedUnique, stats.Duplicate, stats.Invalid, stats.Stale, 0)

	classification := "LOCAL_SINGLE_HOST_BENCHMARK"
	envObj := map[string]any{
		"os":             runtime.GOOS,
		"arch":           runtime.GOARCH,
		"numCpu":         runtime.NumCPU(),
		"goVersion":      runtime.Version(),
		"heapAllocMb":    float64(mem.HeapAlloc) / (1024 * 1024),
		"classification": classification,
	}
	cfgObj := map[string]any{
		"runId":          rid,
		"profile":        *profile,
		"logicalDevices": devices,
		"targetRate":     targetRate,
		"durationSec":    *durationSec,
		"workerPool":     cfg.WorkerPool,
		"channelCap":     cfg.ChannelCap,
		"classification": classification,
	}
	genObj := map[string]any{
		"generated":       gen,
		"brokerAcked":     ack,
		"produceErrors":   atomic.LoadInt64(&produceErrCount),
		"generatedPerSec": float64(gen) / genElapsed.Seconds(),
		"ackedPerSec":     float64(ack) / genElapsed.Seconds(),
	}
	pipeObj := map[string]any{
		"consumed":             stats.Received,
		"acceptedUnique":       stats.AcceptedUnique,
		"duplicates":           stats.Duplicate,
		"stale":                stats.Stale,
		"invalid":              stats.Invalid,
		"transitionsPersisted": rep.TransitionsPersisted,
		"outboxPublished":      rep.OutboxPublished,
		"activeRedisDevices":   rep.ActiveRedisDevices,
		"consumedPerSec":       float64(stats.Received) / time.Since(tStart).Seconds(),
	}
	latObj := map[string]any{
		"ingestInternal":     map[string]any{"boundary": "INGEST_INTERNAL", "p50Ms": inP50, "p95Ms": inP95, "p99Ms": inP99},
		"pipelineTransition": map[string]any{"boundary": "PIPELINE_TRANSITION", "p50Ms": outP50, "p95Ms": outP95, "p99Ms": outP99},
		"productionTarget":   "40000 active devices / 80000 events/s / <12ms p99 (TARGET, requires multi-node cluster)",
	}

	writeJ := func(name string, v any) {
		raw, _ := json.MarshalIndent(v, "", "  ")
		_ = os.WriteFile(filepath.Join(dest, name), append(raw, '\n'), 0o644)
	}
	writeJ("environment.json", envObj)
	writeJ("config.json", cfgObj)
	writeJ("generator.json", genObj)
	writeJ("pipeline.json", pipeObj)
	writeJ("latency-histograms.json", latObj)
	sort.Slice(rep.LastCommittedOffsets, func(i, j int) bool {
		return rep.LastCommittedOffsets[i].Partition < rep.LastCommittedOffsets[j].Partition
	})
	writeJ("kafka-offsets.json", rep.LastCommittedOffsets)
	summary := fmt.Sprintf("# Run %s (`%s`)\n\n- **Classification**: `%s` (`40K / 80K / <12ms p99` = `TARGET`)\n- **Devices**: `%d` | **Generated**: `%d` (`%.1f/s`) | **Acked**: `%d` (`%.1f/s`) | **Consumed**: `%d` (`%.1f/s`)\n- **Transitions**: `%d` | **INGEST_INTERNAL p99**: `%.2f ms` | **PIPELINE_TRANSITION p99**: `%.2f ms`\n",
		rid, *profile, classification, devices, gen, genObj["generatedPerSec"], ack, genObj["ackedPerSec"], stats.Received, pipeObj["consumedPerSec"], rep.TransitionsPersisted, inP99, outP99)
	_ = os.WriteFile(filepath.Join(dest, "summary.md"), []byte(summary), 0o644)
	fmt.Println(summary)
}
