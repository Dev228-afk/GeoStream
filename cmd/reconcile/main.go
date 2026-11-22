package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/example/geostream/internal/config"
	"github.com/example/geostream/internal/reconciliation"
	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/storage"
	"github.com/redis/go-redis/v9"
)

func main() {
	runID := flag.String("run-id", "", "Run ID to reconcile")
	group := flag.String("group", "", "Optional consumer group ID")
	minTransitions := flag.Int64("min-transitions", 0, "Expected minimum durable transitions")
	flag.Parse()

	if *runID == "" {
		fmt.Fprintln(os.Stderr, "usage: reconcile --run-id <id>")
		os.Exit(2)
	}

	cfg := config.Load()
	cg := *group
	if cg == "" {
		cg = "cg_" + *runID
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pg, err := storage.NewPostgresStore(ctx, cfg.PostgresURL, 4)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PG error: %v\n", err)
		os.Exit(1)
	}
	defer pg.Close()

	rcl := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rcl.Close()
	rds := redisstate.NewStore(rcl, cfg.RedisTTL)

	rep, err := reconciliation.AuditRun(ctx, pg, rds, cfg.KafkaBrokers, *runID, cg, 0, 0, 0, 0, 0, *minTransitions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Audit error: %v\n", err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if !rep.Pass {
		os.Exit(1)
	}
}

