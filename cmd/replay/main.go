package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/example/geostream/internal/config"
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

func main() {
	topic := flag.String("topic", "pipeline.deadletter", "Topic to inspect/replay")
	runFilter := flag.String("run-id", "", "Optional runId filter for raw topic replay")
	maxCount := flag.Int("limit", 100, "Max records to read in bounded replay pass")
	flag.Parse()

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pg, err := storage.NewPostgresStore(ctx, cfg.PostgresURL, 4)
	if err != nil {
		panic(err)
	}
	defer pg.Close()

	rcl := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rcl.Close()
	rds := redisstate.NewStore(rcl, cfg.RedisTTL)

	gfRaw, err := os.ReadFile(filepath.Join(cfg.FixturesDir, "geofences.geojson"))
	if err != nil {
		panic(err)
	}
	fences, err := geofence.ParseGeoJSON(gfRaw)
	if err != nil {
		panic(err)
	}
	eng, err := geofence.NewEngine(fences, cfg.DwellMS)
	if err != nil {
		panic(err)
	}
	prod, err := gkafka.NewProducer(cfg.KafkaBrokers, 0)
	if err != nil {
		panic(err)
	}
	defer prod.Close()

	metrics := telemetry.NewMetrics(nil)
	proc := pipeline.NewProcessor(eng, rds, pg, prod, metrics, cfg.ConsumerGroup+"-replay", cfg.AllowedFutureSkew)

	cons, err := gkafka.NewConsumer(cfg.KafkaBrokers, "replay_"+time.Now().UTC().Format("150405"), []string{*topic})
	if err != nil {
		panic(err)
	}
	defer cons.Close()

	inspected := 0
	replayed := 0
	dlqSample := make([]schema.DeadletterRecordV1, 0)

	readDeadline := time.Now().Add(4 * time.Second)
	for inspected < *maxCount && time.Now().Before(readDeadline) {
		fetches := cons.PollFetches(ctx)
		if fetches.Empty() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if inspected >= *maxCount {
				return
			}
			inspected++
			if *topic == cfg.TopicDLQ {
				var dlq schema.DeadletterRecordV1
				if json.Unmarshal(r.Value, &dlq) == nil {
					dlqSample = append(dlqSample, dlq)
				}
				return
			}
			var ev schema.LocationEventV1
			if json.Unmarshal(r.Value, &ev) == nil {
				if *runFilter != "" && ev.RunId != *runFilter {
					return
				}
				metrics.ReplayAttempts.Inc()
				if ok, err := proc.ProcessRecord(ctx, r); err == nil && ok {
					replayed++
				}
			}
		})
	}
	pub, _ := pipeline.PublishPendingOutbox(ctx, pg, prod, metrics, *runFilter, 100, nil)
	out, _ := json.MarshalIndent(map[string]any{
		"topic":           *topic,
		"inspected":       inspected,
		"replayed":        replayed,
		"outboxPublished": pub,
		"dlqSamples":      dlqSample,
	}, "", "  ")
	fmt.Println(string(out))
}

