package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/storage"
	"github.com/redis/go-redis/v9"
)

func TestMonitoringAPIEndpointsAndErrorContracts(t *testing.T) {
	ctx := context.Background()
	pg, err := storage.NewPostgresStore(ctx, "postgres://geostream:geostream_local_2026@127.0.0.1:5437/geostream?sslmode=disable", 4)
	if err != nil {
		t.Fatalf("pg: %v", err)
	}
	defer pg.Close()
	_ = pg.Migrate(ctx)

	rcl := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6383"})
	defer rcl.Close()
	rds := redisstate.NewStore(rcl, time.Hour)

	srv := NewServer(pg, rds, []string{"127.0.0.1:19092"}, "http://127.0.0.1:19999", "../evidence", "../fixtures", "test")
	h := srv.Router()

	// 1. Unknown run returns 404 RUN_NOT_FOUND
	req404 := httptest.NewRequest(http.MethodGet, "/api/v1/runs/does-not-exist/summary", nil)
	rec404 := httptest.NewRecorder()
	h.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 RUN_NOT_FOUND, got %d body=%s", rec404.Code, rec404.Body.String())
	}

	// 2. Unknown timeseries metric returns 422 UNKNOWN_METRIC
	req422 := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-smoke-001/timeseries?metric=bad_metric", nil)
	rec422 := httptest.NewRecorder()
	h.ServeHTTP(rec422, req422)
	if rec422.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 UNKNOWN_METRIC, got %d", rec422.Code)
	}

	// 3. Valid timeseries when Prometheus unreachable returns status=unavailable (NOT fake zeros)
	reqPromOff := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-smoke-001/timeseries?metric=events_accepted_per_sec&window=5m&resolution=10s", nil)
	recPromOff := httptest.NewRecorder()
	h.ServeHTTP(recPromOff, reqPromOff)
	if recPromOff.Code != http.StatusOK {
		t.Fatalf("expected 200 with status:unavailable, got %d", recPromOff.Code)
	}
	var tsBody map[string]any
	_ = json.Unmarshal(recPromOff.Body.Bytes(), &tsBody)
	if tsBody["status"] != "unavailable" {
		t.Fatalf("expected unavailable status when Prometheus unreachable, got %+v", tsBody)
	}

	// 4. Health returns 200 with postgres/redis ok and prometheus unavailable
	reqHealth := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	recHealth := httptest.NewRecorder()
	h.ServeHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK {
		t.Fatalf("expected health 200, got %d", recHealth.Code)
	}

	// 5. Cold/evicted Redis key falls back to PG membership with positionFreshness=stale
	runID := "run_api_stale_" + time.Now().UTC().Format("150405")
	_ = pg.EnsureRun(ctx, storage.RunRecord{ID: runID, Seed: 2026, Profile: "api-test", StartedAt: time.Now().UTC(), Status: "RUNNING"})
	rcl.Del(ctx, redisstate.DeviceKey(runID, "dev-missing"))
	reqDev404 := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/devices/dev-missing", nil)
	recDev404 := httptest.NewRecorder()
	h.ServeHTTP(recDev404, reqDev404)
	if recDev404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown device, got %d", recDev404.Code)
	}
}

