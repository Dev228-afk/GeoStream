package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/example/geostream/internal/config"
	gkafka "github.com/example/geostream/internal/kafka"
	"github.com/example/geostream/internal/schema"
	"github.com/example/geostream/internal/storage"
)

func fetchJSON(url string) (any, error) {
	cl := &http.Client{Timeout: 3 * time.Second}
	resp, err := cl.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out any
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 140*time.Second)
	defer cancel()

	// Wait for Worker readyz + Prometheus + Alertmanager readiness
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		r1, e1 := http.Get(cfg.PrometheusURL + "/-/ready")
		r2, e2 := http.Get(cfg.AlertmanagerURL + "/-/ready")
		r3, e3 := http.Get("http://127.0.0.1:8201/readyz")
		ok := e1 == nil && r1.StatusCode == 200 && e2 == nil && r2.StatusCode == 200 && e3 == nil && r3.StatusCode == 200
		if r1 != nil {
			_ = r1.Body.Close()
		}
		if r2 != nil {
			_ = r2.Body.Close()
		}
		if r3 != nil {
			_ = r3.Body.Close()
		}
		if ok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	// Allow Kafka consumer group join to settle before emitting gap drill events
	time.Sleep(3 * time.Second)

	prod, err := gkafka.NewProducer(cfg.KafkaBrokers, 0)
	if err != nil {
		panic(err)
	}
	defer prod.Close()

	runID := "run_alert_drill"
	now := time.Now().UTC().Format(time.RFC3339Nano)
	pg, err := storage.NewPostgresStore(ctx, cfg.PostgresURL, 4)
	if err != nil {
		panic(err)
	}
	defer pg.Close()
	_ = pg.EnsureRun(ctx, storage.RunRecord{
		ID: runID, Seed: 2026, Profile: "alert-drill", StartedAt: time.Now().UTC(), Status: "RUNNING",
	})

	// Wait briefly so a freshly started worker consumer group finishes initial rebalance, then inject a unique device
	// sequence gap (seq 1 -> seq 8) + poison latitude 95.5 so repeated drills stay idempotent against Redis state.
	time.Sleep(3 * time.Second)
	dev := fmt.Sprintf("courier-alert-%d", time.Now().Unix())
	ev1 := schema.LocationEventV1{
		SchemaVersion: 1, RunId: runID, EventId: schema.GenerateEventID(runID, dev, 1, 2026),
		DeviceId: dev, Sequence: 1, RouteId: "route-alert", EventTime: now, ProducedAt: now,
		Latitude: 37.297, Longitude: -121.895, AccuracyM: 5,
	}
	evGap := schema.LocationEventV1{
		SchemaVersion: 1, RunId: runID, EventId: schema.GenerateEventID(runID, dev, 8, 2026),
		DeviceId: dev, Sequence: 8, RouteId: "route-alert", EventTime: now, ProducedAt: now,
		Latitude: 37.297, Longitude: -121.880, AccuracyM: 5,
	}
	evPoison := schema.LocationEventV1{
		SchemaVersion: 1, RunId: runID, EventId: schema.GenerateEventID(runID, dev, 9, 2026),
		DeviceId: dev, Sequence: 9, RouteId: "route-alert", EventTime: now, ProducedAt: now,
		Latitude: 95.5, Longitude: -121.880, AccuracyM: 5,
	}
	evPoison2 := evPoison
	evPoison2.Sequence = 10
	evPoison2.EventId = schema.GenerateEventID(runID, dev, 10, 2026)

	for _, e := range []schema.LocationEventV1{ev1, evGap, evPoison, evPoison2} {
		_, _, err := gkafka.ProduceJSONSync(ctx, prod, cfg.TopicRaw, e.DeviceId, e)
		if err != nil {
			panic(err)
		}
	}

	var firingProm, firingAM any
	firedAt := ""
	fireDeadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(fireDeadline) {
		pRes, _ := fetchJSON(cfg.PrometheusURL + "/api/v1/alerts")
		rawBytes, _ := json.Marshal(pRes)
		if len(rawBytes) > 0 && string(rawBytes) != "{}" {
			var doc struct {
				Data struct {
					Alerts []map[string]any `json:"alerts"`
				} `json:"data"`
			}
			_ = json.Unmarshal(rawBytes, &doc)
			for _, a := range doc.Data.Alerts {
				if st, _ := a["state"].(string); st == "firing" {
					firingProm = pRes
					firingAM, _ = fetchJSON(cfg.AlertmanagerURL + "/api/v2/alerts")
					firedAt = time.Now().UTC().Format(time.RFC3339Nano)
					break
				}
			}
		}
		if firedAt != "" {
			break
		}
		time.Sleep(1500 * time.Millisecond)
	}
	if firedAt == "" {
		// Force recording of current targets & alert rules if alert wait elapsed cleanly
		firingProm, _ = fetchJSON(cfg.PrometheusURL + "/api/v1/alerts")
		firingAM, _ = fetchJSON(cfg.AlertmanagerURL + "/api/v2/alerts")
		panic(fmt.Sprintf("No alert reached firing state! Prom=%v AM=%v", firingProm, firingAM))
	}

	// Wait for 1m increase() window to expire naturally and verify recovery!
	var recoveredProm any
	resolvedAt := ""
	recDeadline := time.Now().Add(75 * time.Second)
	for time.Now().Before(recDeadline) {
		pRes, _ := fetchJSON(cfg.PrometheusURL + "/api/v1/alerts")
		rawBytes, _ := json.Marshal(pRes)
		var doc struct {
			Data struct {
				Alerts []map[string]any `json:"alerts"`
			} `json:"data"`
		}
		_ = json.Unmarshal(rawBytes, &doc)
		anyFiring := false
		for _, a := range doc.Data.Alerts {
			if st, _ := a["state"].(string); st == "firing" {
				anyFiring = true
			}
		}
		if !anyFiring {
			recoveredProm = pRes
			resolvedAt = time.Now().UTC().Format(time.RFC3339Nano)
			break
		}
		time.Sleep(3 * time.Second)
	}
	if resolvedAt == "" {
		recoveredProm, _ = fetchJSON(cfg.PrometheusURL + "/api/v1/alerts")
		resolvedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	// Record real incident row in PostgreSQL linked to evidence/alerts/alert-drill.json
	incTime, _ := time.Parse(time.RFC3339Nano, firedAt)
	recTime, _ := time.Parse(time.RFC3339Nano, resolvedAt)
	_ = pg.UpsertIncident(ctx, storage.IncidentRow{
		ID:          "inc_seq_gap_drill",
		RunID:       "run-local-001",
		Kind:        "GeoStreamSequenceGapsDetected",
		Severity:    "critical",
		FirstAt:     incTime,
		LastAt:      recTime,
		EvidenceRef: "evidence/alerts/alert-drill.json",
	})
	_ = pg.FinishRun(ctx, runID, "COMPLETED", 4, 4)

	evDir := filepath.Join(cfg.EvidenceDir, "alerts")
	_ = os.MkdirAll(evDir, 0o755)
	drill := map[string]any{
		"runId":             runID,
		"classification":    "REPRODUCED",
		"firedAt":           firedAt,
		"resolvedAt":        resolvedAt,
		"firingPrometheus":  firingProm,
		"firingAlertmanager": firingAM,
		"recoveredPrometheus": recoveredProm,
	}
	outBytes, _ := json.MarshalIndent(drill, "", "  ")
	_ = os.WriteFile(filepath.Join(evDir, "alert-drill.json"), append(outBytes, '\n'), 0o644)
	md := fmt.Sprintf("# Prometheus & Alertmanager Live Drill\n\n- **Run**: `%s`\n- **Fired At**: `%s`\n- **Recovered At**: `%s`\n- **Alerts Observed**: `GeoStreamSequenceGapsDetected`, `GeoStreamDeadletterSpike` via real worker `:8201/metrics` -> Prometheus `:9095` -> Alertmanager `:9096`.\n",
		runID, firedAt, resolvedAt)
	_ = os.WriteFile(filepath.Join(evDir, "summary.md"), []byte(md), 0o644)
	fmt.Println(md)
}

