package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/example/geostream/internal/config"
	"github.com/example/geostream/internal/geofence"
	gkafka "github.com/example/geostream/internal/kafka"
	"github.com/example/geostream/internal/pipeline"
	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/storage"
	"github.com/example/geostream/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pg, err := storage.NewPostgresStore(ctx, cfg.PostgresURL, 16)
	if err != nil {
		log.Fatalf("pg init: %v", err)
	}
	defer pg.Close()
	if err := pg.Migrate(ctx); err != nil {
		log.Fatalf("pg migrate: %v", err)
	}

	rcl := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rcl.Close()
	if err := rcl.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}
	rds := redisstate.NewStore(rcl, cfg.RedisTTL)

	if err := gkafka.EnsureTopics(ctx, cfg.KafkaBrokers, cfg.RawPartitions); err != nil {
		log.Fatalf("kafka topics: %v", err)
	}

	gfBytes, err := os.ReadFile(filepath.Join(cfg.FixturesDir, "geofences.geojson"))
	if err != nil {
		log.Fatalf("read geofences: %v", err)
	}
	fences, err := geofence.ParseGeoJSON(gfBytes)
	if err != nil {
		log.Fatalf("parse geofences: %v", err)
	}
	if err := pg.UpsertGeofences(ctx, fences); err != nil {
		log.Fatalf("upsert geofences: %v", err)
	}
	eng, err := geofence.NewEngine(fences, cfg.DwellMS)
	if err != nil {
		log.Fatalf("geofence engine: %v", err)
	}

	prod, err := gkafka.NewProducer(cfg.KafkaBrokers, 0)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer prod.Close()

	if os.Getenv("KAFKA_SEEK_END_ON_START") == "true" {
		if sErr := gkafka.SeekGroupToEnd(ctx, cfg.KafkaBrokers, cfg.ConsumerGroup, cfg.TopicRaw); sErr != nil {
			log.Printf("warn SeekGroupToEnd: %v", sErr)
		}
	}

	cons, err := gkafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.TopicRaw})
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer cons.Close()

	metrics := telemetry.NewMetrics(nil)
	proc := pipeline.NewProcessor(eng, rds, pg, prod, metrics, cfg.ConsumerGroup, cfg.AllowedFutureSkew)

	// HTTP /metrics, /healthz, /readyz server on :8201
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"alive"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		rCtx, c := context.WithTimeout(r.Context(), 2*time.Second)
		defer c()
		pgErr := pg.Ping(rCtx)
		rdErr := rds.Ping(rCtx)
		status := http.StatusOK
		if pgErr != nil || rdErr != nil {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ready":    pgErr == nil && rdErr == nil,
			"postgres": pgErr == nil,
			"redis":    rdErr == nil,
		})
	})

	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server error: %v", err)
		}
	}()

	// Periodic OutboxPublisher loop
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = pipeline.PublishPendingOutbox(context.Background(), pg, prod, metrics, "", 200, nil)
			}
		}
	}()

	log.Printf("geostream ingest worker running (pool=%d chanCap=%d metrics=%s)", cfg.WorkerPool, cfg.ChannelCap, cfg.MetricsAddr)
	_ = pipeline.RunBoundedWorker(ctx, cons, proc, cfg.WorkerPool, cfg.ChannelCap)

	// Graceful drain outbox best-effort on SIGTERM
	shCtx, shCancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer shCancel()
	_, _ = pipeline.PublishPendingOutbox(shCtx, pg, prod, metrics, "", 500, nil)
	_ = srv.Shutdown(shCtx)
}

