package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/geostream/api"
	"github.com/example/geostream/internal/config"
	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/storage"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pg, err := storage.NewPostgresStore(ctx, cfg.PostgresURL, 8)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pg.Close()
	if err := pg.Migrate(ctx); err != nil {
		log.Fatalf("pg migrate: %v", err)
	}

	rcl := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rcl.Close()
	rds := redisstate.NewStore(rcl, cfg.RedisTTL)

	srv := api.NewServer(pg, rds, cfg.KafkaBrokers, cfg.PrometheusURL, cfg.EvidenceDir, cfg.FixturesDir, "HEAD")
	httpSrv := &http.Server{
		Addr:    cfg.APIAddr,
		Handler: srv.Router(),
	}

	go func() {
		log.Printf("geostream API listening on http://%s", cfg.APIAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("api error: %v", err)
		}
	}()

	<-ctx.Done()
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shCtx)
}

