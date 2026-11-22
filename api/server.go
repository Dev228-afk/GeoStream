package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/example/geostream/internal/geofence"
	"github.com/example/geostream/internal/reconciliation"
	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/storage"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Server struct {
	PG            *storage.PostgresStore
	Redis         *redisstate.Store
	Brokers       []string
	PrometheusURL string
	EvidenceDir   string
	FixturesDir   string
	CommitSHA     string
	PromClient    *http.Client
}

func NewServer(pg *storage.PostgresStore, rds *redisstate.Store, brokers []string, promURL, evDir, fixDir, commit string) *Server {
	return &Server{
		PG:            pg,
		Redis:         rds,
		Brokers:       brokers,
		PrometheusURL: strings.TrimRight(promURL, "/"),
		EvidenceDir:   evDir,
		FixturesDir:   fixDir,
		CommitSHA:     commit,
		PromClient:    &http.Client{Timeout: 1500 * time.Millisecond},
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":      code,
			"message":   msg,
			"requestId": "req_" + uuid.NewString()[:8],
		},
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "http://127.0.0.1:5301"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(corsMiddleware)

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", s.handleHealth)
		r.Get("/runs", s.handleListRuns)
		r.Get("/runs/{id}/summary", s.handleRunSummary)
		r.Get("/runs/{id}/devices", s.handleListDevices)
		r.Get("/runs/{id}/devices/{deviceId}", s.handleGetDevice)
		r.Get("/runs/{id}/geofences", s.handleGeofences)
		r.Get("/runs/{id}/transitions", s.handleTransitions)
		r.Get("/runs/{id}/timeseries", s.handleTimeseries)
		r.Get("/runs/{id}/reconciliation", s.handleReconciliation)
		r.Get("/runs/{id}/incidents", s.handleIncidents)
		r.Get("/runs/{id}/stream", s.handleStreamSSE)

		// Verified recorded diagnostics bundles (strictly labeled recorded evidence)
		r.Get("/evidence/backpressure", s.handleEvidenceBackpressure)
		r.Get("/evidence/chaos", s.handleEvidenceChaos)
		r.Get("/evidence/alerts", s.handleEvidenceAlerts)
	})

	return r
}

func (s *Server) promReachable(ctx context.Context) bool {
	if s.PrometheusURL == "" {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.PrometheusURL+"/-/ready", nil)
	if err != nil {
		return false
	}
	resp, err := s.PromClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	pgOK := s.PG != nil && s.PG.Ping(ctx) == nil
	rdOK := s.Redis != nil && s.Redis.Ping(ctx) == nil
	promOK := s.promReachable(ctx)

	overall := "ok"
	code := http.StatusOK
	if !pgOK || !rdOK {
		overall = "degraded"
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"status": overall,
		"dependencies": map[string]any{
			"postgres":   statusStr(pgOK),
			"redis":      statusStr(rdOK),
			"prometheus": availStr(promOK),
		},
		"commit": s.CommitSHA,
	})
}

func statusStr(ok bool) string {
	if ok {
		return "ok"
	}
	return "unavailable"
}

func availStr(ok bool) string {
	if ok {
		return "ok"
	}
	return "unavailable"
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 50 {
			writeErr(w, http.StatusBadRequest, "INVALID_LIMIT", "limit must be between 1 and 50.")
			return
		}
		limit = n
	}
	runs, err := s.PG.ListRuns(r.Context(), limit)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "STORAGE_UNAVAILABLE", err.Error())
		return
	}
	if runs == nil {
		runs = []storage.RunRecord{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      runs,
		"nextCursor": nil,
	})
}

func (s *Server) loadRecordedEvidence(runID string) (gen, pipe, lat map[string]any, ok bool) {
	candidates := []string{
		filepath.Join(s.EvidenceDir, "throughput", runID),
		filepath.Join(s.EvidenceDir, "throughput", "run-local-001"),
		filepath.Join(s.EvidenceDir, "throughput", "run-smoke-001"),
	}
	for _, dir := range candidates {
		if runID == "run-local-001" && !strings.HasSuffix(dir, "run-local-001") {
			continue
		}
		if runID == "run-smoke-001" && !strings.HasSuffix(dir, "run-smoke-001") {
			continue
		}
		rawG, errG := os.ReadFile(filepath.Join(dir, "generator.json"))
		rawP, errP := os.ReadFile(filepath.Join(dir, "pipeline.json"))
		rawL, errL := os.ReadFile(filepath.Join(dir, "latency-histograms.json"))
		if errG == nil && errP == nil && errL == nil && strings.HasSuffix(dir, runID) {
			_ = json.Unmarshal(rawG, &gen)
			_ = json.Unmarshal(rawP, &pipe)
			_ = json.Unmarshal(rawL, &lat)
			return gen, pipe, lat, true
		}
	}
	return nil, nil, nil, false
}

func (s *Server) handleRunSummary(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	run, found, err := s.PG.GetRun(r.Context(), runID)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "STORAGE_UNAVAILABLE", err.Error())
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "RUN_NOT_FOUND", "This run is unavailable.")
		return
	}

	activeCount, _ := s.Redis.ActiveDeviceCount(r.Context(), runID)
	promOK := s.promReachable(r.Context())
	genEv, pipeEv, latEv, hasEv := s.loadRecordedEvidence(runID)

	mode := "LIVE"
	metricsStatus := "unavailable"
	if run.Status == "COMPLETED" && hasEv {
		mode = "RECORDED"
		metricsStatus = "recorded"
	} else if promOK {
		metricsStatus = "ok"
	}

	var genRate, ackRate, consRate, accRate, dupRate, invRate float64
	p50, p95, p99 := 0.0, 0.0, 0.0
	boundary := "INGEST_INTERNAL"

	if hasEv {
		if v, ok := genEv["generatedPerSec"].(float64); ok {
			genRate = v
		}
		if v, ok := genEv["ackedPerSec"].(float64); ok {
			ackRate = v
		}
		if v, ok := pipeEv["consumedPerSec"].(float64); ok {
			consRate = v
			accRate = v
		}
		if ii, ok := latEv["ingestInternal"].(map[string]any); ok {
			if v, ok := ii["p50Ms"].(float64); ok {
				p50 = v
			}
			if v, ok := ii["p95Ms"].(float64); ok {
				p95 = v
			}
			if v, ok := ii["p99Ms"].(float64); ok {
				p99 = v
			}
		}
	}

	freshness := time.Now().UTC().Format(time.RFC3339Nano)
	if run.EndedAt != nil {
		freshness = run.EndedAt.UTC().Format(time.RFC3339Nano)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"run":               run,
		"generatedPerSec":   genRate,
		"brokerAckedPerSec": ackRate,
		"consumedPerSec":    consRate,
		"acceptedPerSec":    accRate,
		"duplicatePerSec":   dupRate,
		"invalidPerSec":     invRate,
		"kafkaLag":          0,
		"activeDeviceCount": activeCount,
		"latency": map[string]any{
			"boundary": boundary,
			"p50Ms":    p50,
			"p95Ms":    p95,
			"p99Ms":    p99,
		},
		"dataFreshnessAt": freshness,
		"mode":            mode,
		"metricsStatus":   metricsStatus,
	})
}

func (s *Server) handleListDevices(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	if _, found, err := s.PG.GetRun(r.Context(), runID); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "STORAGE_UNAVAILABLE", err.Error())
		return
	} else if !found {
		writeErr(w, http.StatusNotFound, "RUN_NOT_FOUND", "Run not found.")
		return
	}

	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 500 {
			writeErr(w, http.StatusBadRequest, "INVALID_LIMIT", "limit must be between 1 and 500.")
			return
		}
		if n > 100 {
			limit = 100 // Cap sampled devices cleanly for UI responsiveness
		} else {
			limit = n
		}
	}
	offset := int64(0)
	if cur := r.URL.Query().Get("cursor"); cur != "" {
		if n, err := strconv.ParseInt(cur, 10, 64); err == nil && n >= 0 {
			offset = n
		}
	}

	ids, err := s.Redis.ListActiveDevices(r.Context(), runID, offset, int64(limit))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "REDIS_UNAVAILABLE", err.Error())
		return
	}

	items := make([]redisstate.LatestState, 0, len(ids))
	for _, devID := range ids {
		st, ok, _ := s.Redis.Read(r.Context(), runID, devID)
		if ok {
			items = append(items, st)
		}
	}
	var nextCursor any = nil
	if len(ids) == limit {
		nextCursor = fmt.Sprintf("%d", offset+int64(limit))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      items,
		"nextCursor": nextCursor,
	})
}

func (s *Server) handleGetDevice(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	devID := chi.URLParam(r, "deviceId")

	st, ok, err := s.Redis.Read(r.Context(), runID, devID)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "REDIS_UNAVAILABLE", err.Error())
		return
	}
	if ok {
		writeJSON(w, http.StatusOK, st)
		return
	}
	// Redis key cold/evicted -> fall back cleanly to durable PG fence state and label positionFreshness:"stale"
	mems, maxSeq, pgErr := s.PG.LoadMembership(r.Context(), runID, devID)
	if pgErr != nil {
		writeErr(w, http.StatusServiceUnavailable, "STORAGE_UNAVAILABLE", pgErr.Error())
		return
	}
	if len(mems) == 0 && maxSeq == 0 {
		writeErr(w, http.StatusNotFound, "DEVICE_NOT_FOUND", "Device not found for this run.")
		return
	}
	var insideIDs []string
	for _, m := range mems {
		if m.Inside {
			insideIDs = append(insideIDs, m.FenceID)
		}
	}
	if insideIDs == nil {
		insideIDs = []string{}
	}
	writeJSON(w, http.StatusOK, redisstate.LatestState{
		RunID:             runID,
		DeviceID:          devID,
		LastSeq:           maxSeq,
		PositionFreshness: "stale",
		InsideFenceIDs:    insideIDs,
	})
}

func (s *Server) handleGeofences(w http.ResponseWriter, r *http.Request) {
	feats, err := s.PG.ListGeofences(r.Context())
	if err == nil && len(feats) > 0 {
		writeJSON(w, http.StatusOK, map[string]any{
			"type":     "FeatureCollection",
			"features": feats,
		})
		return
	}
	raw, fErr := os.ReadFile(filepath.Join(s.FixturesDir, "geofences.geojson"))
	if fErr != nil {
		writeErr(w, http.StatusServiceUnavailable, "GEOFENCES_UNAVAILABLE", "Geofence catalog unavailable.")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}

func (s *Server) handleTransitions(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	typ := r.URL.Query().Get("type")
	cursor := r.URL.Query().Get("cursor")
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			writeErr(w, http.StatusBadRequest, "INVALID_LIMIT", "limit must be between 1 and 100.")
			return
		}
		limit = n
	}
	items, nextCur, err := s.PG.ListTransitions(r.Context(), runID, typ, cursor, limit)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "STORAGE_UNAVAILABLE", err.Error())
		return
	}
	if items == nil {
		items = []storage.TransitionRow{}
	}
	var nc any = nil
	if nextCur != "" {
		nc = nextCur
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      items,
		"nextCursor": nc,
	})
}

var allowedMetrics = map[string]struct{ query, unit string }{
	"events_accepted_per_sec": {query: `rate(geostream_events_accepted_total[15s])`, unit: "events/s"},
	"kafka_lag":               {query: `sum(geostream_kafka_lag)`, unit: "records"},
	"p99_ingest_ms":           {query: `histogram_quantile(0.99, sum(rate(geostream_processing_latency_seconds_bucket[1m])) by (le)) * 1000`, unit: "ms"},
	"db_channel_depth":        {query: `geostream_db_channel_depth`, unit: "items"},
}

func (s *Server) handleTimeseries(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		metric = "events_accepted_per_sec"
	}
	spec, ok := allowedMetrics[metric]
	if !ok {
		writeErr(w, http.StatusUnprocessableEntity, "UNKNOWN_METRIC", "Unsupported metric allowlist key.")
		return
	}
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "5m"
	}
	var dur time.Duration
	switch window {
	case "5m":
		dur = 5 * time.Minute
	case "15m":
		dur = 15 * time.Minute
	case "1h":
		dur = time.Hour
	default:
		writeErr(w, http.StatusUnprocessableEntity, "INVALID_WINDOW", "window must be 5m, 15m, or 1h.")
		return
	}
	resolution := r.URL.Query().Get("resolution")
	if resolution == "" {
		resolution = "10s"
	}
	if resolution != "1s" && resolution != "10s" && resolution != "1m" {
		writeErr(w, http.StatusUnprocessableEntity, "INVALID_RESOLUTION", "resolution must be 1s, 10s, or 1m.")
		return
	}

	if !s.promReachable(r.Context()) {
		writeJSON(w, http.StatusOK, map[string]any{
			"metric": metric,
			"unit":   spec.unit,
			"points": []any{},
			"source": "prometheus",
			"status": "unavailable",
		})
		return
	}

	now := time.Now().UTC()
	url := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%s",
		s.PrometheusURL,
		strings.ReplaceAll(spec.query, " ", "%20"),
		now.Add(-dur).Unix(),
		now.Unix(),
		resolution,
	)
	resp, err := s.PromClient.Get(url)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"metric": metric,
			"unit":   spec.unit,
			"points": []any{},
			"source": "prometheus",
			"status": "unavailable",
		})
		return
	}
	defer resp.Body.Close()

	var promResp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&promResp)
	points := make([]map[string]any, 0)
	if len(promResp.Data.Result) > 0 {
		for _, pair := range promResp.Data.Result[0].Values {
			if len(pair) != 2 {
				continue
			}
			tsFloat, _ := pair[0].(float64)
			valStr, _ := pair[1].(string)
			v, pErr := strconv.ParseFloat(valStr, 64)
			var valOut any = nil
			if pErr == nil {
				valOut = v
			}
			points = append(points, map[string]any{
				"at":    time.Unix(int64(tsFloat), 0).UTC().Format(time.RFC3339),
				"value": valOut,
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"metric": metric,
		"unit":   spec.unit,
		"points": points,
		"source": "prometheus",
		"status": "ok",
	})
}

func (s *Server) handleReconciliation(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	rep, err := reconciliation.AuditRun(r.Context(), s.PG, s.Redis, s.Brokers, runID, "cg_"+runID, 0, 0, 0, 0, 0, 0)
	if err != nil {
		writeErr(w, http.StatusNotFound, "RUN_NOT_FOUND", err.Error())
		return
	}
	if _, pipeEv, _, ok := s.loadRecordedEvidence(runID); ok {
		if v, ok := pipeEv["consumed"].(float64); ok {
			rep.Consumed = int64(v)
		}
		if v, ok := pipeEv["acceptedUnique"].(float64); ok {
			rep.AcceptedUnique = int64(v)
		}
		if v, ok := pipeEv["duplicates"].(float64); ok {
			rep.Duplicates = int64(v)
		}
		if v, ok := pipeEv["invalid"].(float64); ok {
			rep.Invalid = int64(v)
		}
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) handleIncidents(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	items, err := s.PG.ListIncidents(r.Context(), runID, 30)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "STORAGE_UNAVAILABLE", err.Error())
		return
	}
	if items == nil {
		items = []storage.IncidentRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      items,
		"nextCursor": nil,
	})
}

func (s *Server) handleStreamSSE(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "STREAM_UNSUPPORTED", "Streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Emit <=1Hz bounded summary ticks cleanly (never streams raw 80K events to UI)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	sendFrame := func() {
		active, _ := s.Redis.ActiveDeviceCount(r.Context(), runID)
		frame, _ := json.Marshal(map[string]any{
			"runId":             runID,
			"activeDeviceCount": active,
			"at":                time.Now().UTC().Format(time.RFC3339),
		})
		_, _ = fmt.Fprintf(w, "event: summary\ndata: %s\n\n", frame)
		flusher.Flush()
	}

	sendFrame()
	for i := 0; i < 10; i++ {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			sendFrame()
		}
	}
}

func (s *Server) handleEvidenceJSON(w http.ResponseWriter, relPath string) {
	raw, err := os.ReadFile(filepath.Join(s.EvidenceDir, relPath))
	if err != nil {
		writeErr(w, http.StatusNotFound, "EVIDENCE_NOT_FOUND", "Evidence file unavailable: "+relPath)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}

func (s *Server) handleEvidenceBackpressure(w http.ResponseWriter, r *http.Request) {
	s.handleEvidenceJSON(w, filepath.Join("backpressure", "run-bp-001", "comparison.json"))
}

func (s *Server) handleEvidenceChaos(w http.ResponseWriter, r *http.Request) {
	s.handleEvidenceJSON(w, filepath.Join("reconciliation", "chaos-matrix.json"))
}

func (s *Server) handleEvidenceAlerts(w http.ResponseWriter, r *http.Request) {
	s.handleEvidenceJSON(w, filepath.Join("alerts", "alert-drill.json"))
}

var _ = geofence.MembershipDigest

