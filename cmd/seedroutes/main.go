package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/example/geostream/internal/geofence"
	"github.com/example/geostream/internal/schema"
)

type ExpectedTransition struct {
	DeviceID string `json:"deviceId"`
	FenceID  string `json:"fenceId"`
	Type     string `json:"type"`
	Seq      int64  `json:"sequence"`
}

func makeRectPolygon(lon0, lat0, lon1, lat1 float64) [][][2]float64 {
	return [][][2]float64{{
		{lon0, lat0}, {lon1, lat0}, {lon1, lat1}, {lon0, lat1}, {lon0, lat0},
	}}
}

func main() {
	seed := flag.Int64("seed", 2026, "Deterministic RNG seed")
	outDir := flag.String("out", "fixtures", "Output fixtures directory")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}

	rng := rand.New(rand.NewSource(*seed))
	// Synthetic base coordinate grid around 37.33, -121.89 (purely synthetic corridor matrix)
	baseLon := -121.95
	baseLat := 37.28

	// 1. Build 20 canonical Geofences:
	// fence-001: polygon WITH hole; fence-002 & fence-003: overlapping zones;
	// fence-004: exact boundary polygon (outer bounds -121.890..-121.870, 37.330..37.350);
	// fence-005..fence-020: grid zones
	features := make([]map[string]any, 0, 20)

	// fence-001: Polygon with interior hole (-121.92..-121.90 outer, -121.914..-121.906 hole)
	features = append(features, map[string]any{
		"type": "Feature",
		"id":   "fence-001",
		"properties": map[string]any{"id": "fence-001", "name": "North Depot Hollow", "version": 1, "enabled": true},
		"geometry": map[string]any{
			"type": "Polygon",
			"coordinates": [][][2]float64{
				{{-121.920, 37.300}, {-121.900, 37.300}, {-121.900, 37.320}, {-121.920, 37.320}, {-121.920, 37.300}},
				{{-121.914, 37.306}, {-121.906, 37.306}, {-121.906, 37.314}, {-121.914, 37.314}, {-121.914, 37.306}},
			},
		},
	})
	// fence-002 & fence-003 overlapping
	features = append(features, map[string]any{
		"type": "Feature", "id": "fence-002",
		"properties": map[string]any{"id": "fence-002", "name": "Central Overlap West", "version": 1, "enabled": true},
		"geometry":   map[string]any{"type": "Polygon", "coordinates": makeRectPolygon(-121.895, 37.320, -121.875, 37.340)},
	})
	features = append(features, map[string]any{
		"type": "Feature", "id": "fence-003",
		"properties": map[string]any{"id": "fence-003", "name": "Central Overlap East", "version": 1, "enabled": true},
		"geometry":   map[string]any{"type": "Polygon", "coordinates": makeRectPolygon(-121.885, 37.325, -121.865, 37.345)},
	})
	// fence-004 exact smoke / boundary fence
	features = append(features, map[string]any{
		"type": "Feature", "id": "fence-004",
		"properties": map[string]any{"id": "fence-004", "name": "South Boundary Gate", "version": 1, "enabled": true},
		"geometry":   map[string]any{"type": "Polygon", "coordinates": makeRectPolygon(-121.890, 37.290, -121.870, 37.305)},
	})
	for i := 5; i <= 20; i++ {
		col := (i - 5) % 4
		row := (i - 5) / 4
		lon0 := baseLon + float64(col)*0.025 + 0.002
		lat0 := baseLat + float64(row)*0.020 + 0.002
		fid := fmt.Sprintf("fence-%03d", i)
		features = append(features, map[string]any{
			"type":       "Feature",
			"id":         fid,
			"properties": map[string]any{"id": fid, "name": fmt.Sprintf("Sector-%02d", i), "version": 1, "enabled": true},
			"geometry":   map[string]any{"type": "Polygon", "coordinates": makeRectPolygon(lon0, lat0, lon0+0.018, lat0+0.014)},
		})
	}

	geofencesDoc := map[string]any{"type": "FeatureCollection", "features": features}
	gfBytes, _ := json.MarshalIndent(geofencesDoc, "", "  ")
	gfPath := filepath.Join(*outDir, "geofences.geojson")
	_ = os.WriteFile(gfPath, append(gfBytes, '\n'), 0o644)

	// 2. City grid synthetic GeoJSON lines
	gridFeatures := make([]map[string]any, 0, 12)
	for i := 0; i <= 5; i++ {
		lon := baseLon + float64(i)*0.025
		gridFeatures = append(gridFeatures, map[string]any{
			"type": "Feature",
			"properties": map[string]any{"axis": "meridian", "index": i},
			"geometry": map[string]any{"type": "LineString", "coordinates": [][2]float64{{lon, baseLat}, {lon, baseLat + 0.10}}},
		})
		lat := baseLat + float64(i)*0.020
		gridFeatures = append(gridFeatures, map[string]any{
			"type": "Feature",
			"properties": map[string]any{"axis": "parallel", "index": i},
			"geometry": map[string]any{"type": "LineString", "coordinates": [][2]float64{{baseLon, lat}, {baseLon + 0.125, lat}}},
		})
	}
	cgBytes, _ := json.MarshalIndent(map[string]any{"type": "FeatureCollection", "features": gridFeatures}, "", "  ")
	_ = os.WriteFile(filepath.Join(*outDir, "city_grid.geojson"), append(cgBytes, '\n'), 0o644)

	// 3. Generate 1000 deterministic route templates covering normal + edge scenarios
	routeFile, err := os.Create(filepath.Join(*outDir, "routes.jsonl"))
	if err != nil {
		panic(err)
	}
	defer routeFile.Close()

	edgeTags := []string{"crossing", "boundary_touch", "hole_traverse", "overlap", "static", "jitter", "teleport", "invalid_gps"}
	for r := 1; r <= 1000; r++ {
		tag := edgeTags[(r-1)%len(edgeTags)]
		if r > 64 {
			tag = "crossing"
		}
		var pts [][2]float64
		switch tag {
		case "boundary_touch":
			pts = [][2]float64{{-121.895, 37.297}, {-121.890, 37.297}, {-121.880, 37.297}, {-121.865, 37.297}}
		case "hole_traverse":
			pts = [][2]float64{{-121.925, 37.310}, {-121.917, 37.310}, {-121.910, 37.310}, {-121.903, 37.310}, {-121.895, 37.310}}
		case "overlap":
			pts = [][2]float64{{-121.898, 37.330}, {-121.890, 37.330}, {-121.880, 37.330}, {-121.870, 37.330}, {-121.860, 37.330}}
		case "static":
			pts = [][2]float64{{-121.880, 37.297}, {-121.880, 37.297}, {-121.880, 37.297}, {-121.880, 37.297}}
		case "jitter":
			pts = [][2]float64{{-121.8905, 37.297}, {-121.8895, 37.297}, {-121.8905, 37.297}, {-121.8850, 37.297}}
		case "teleport":
			pts = [][2]float64{{-121.940, 37.285}, {-121.880, 37.297}, {-120.500, 36.100}, {-121.860, 37.297}}
		case "invalid_gps":
			pts = [][2]float64{{-121.895, 37.297}, {-121.880, 95.500}, {-121.865, 37.297}}
		default:
			lonA := baseLon + rng.Float64()*0.02
			latA := baseLat + rng.Float64()*0.08
			pts = [][2]float64{{lonA, latA}, {lonA + 0.03, latA + 0.005}, {lonA + 0.06, latA + 0.01}, {lonA + 0.09, latA}}
		}
		line, _ := json.Marshal(map[string]any{
			"routeId":   fmt.Sprintf("route-%04d", r),
			"category":  tag,
			"waypoints": pts,
		})
		_, _ = fmt.Fprintln(routeFile, string(line))
	}

	// 4. Smoke fixture (outside -> inside -> inside -> outside on fence-004) + independent expected_transitions.json
	baseT := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	smokePts := [][2]float64{
		{-121.895, 37.297}, // seq 1: outside fence-004
		{-121.880, 37.297}, // seq 2: inside fence-004 -> ENTER
		{-121.878, 37.298}, // seq 3: still inside fence-004 -> NO transition
		{-121.865, 37.297}, // seq 4: outside fence-004 -> EXIT
	}
	smokeFile, _ := os.Create(filepath.Join(*outDir, "smoke_4events.jsonl"))
	for idx, pt := range smokePts {
		seq := int64(idx + 1)
		ts := baseT.Add(time.Duration(idx) * time.Second).Format(time.RFC3339Nano)
		ev := schema.LocationEventV1{
			SchemaVersion: 1,
			RunId:         "run_smoke_001",
			EventId:       schema.GenerateEventID("run_smoke_001", "courier-000001", seq, *seed),
			DeviceId:      "courier-000001",
			Sequence:      seq,
			RouteId:       "route-smoke-001",
			EventTime:     ts,
			ProducedAt:    ts,
			Latitude:      pt[1],
			Longitude:     pt[0],
			AccuracyM:     5.0,
		}
		b, _ := json.Marshal(ev)
		_, _ = fmt.Fprintln(smokeFile, string(b))
	}
	_ = smokeFile.Close()

	// Independent bbox ground-truth verification (without calling Engine.Evaluate) for smoke_4events:
	// fence-004 spans [-121.890, -121.870] x [37.290, 37.305]
	var expected []ExpectedTransition
	prevIn := false
	for idx, pt := range smokePts {
		seq := int64(idx + 1)
		inBox := pt[0] >= -121.890 && pt[0] <= -121.870 && pt[1] >= 37.290 && pt[1] <= 37.305
		if !prevIn && inBox {
			expected = append(expected, ExpectedTransition{DeviceID: "courier-000001", FenceID: "fence-004", Type: "ENTER", Seq: seq})
		} else if prevIn && !inBox {
			expected = append(expected, ExpectedTransition{DeviceID: "courier-000001", FenceID: "fence-004", Type: "EXIT", Seq: seq})
		}
		prevIn = inBox
	}
	expBytes, _ := json.MarshalIndent(expected, "", "  ")
	_ = os.WriteFile(filepath.Join(*outDir, "expected_transitions.json"), append(expBytes, '\n'), 0o644)

	// Quick structural check using ParseGeoJSON
	if _, err := geofence.ParseGeoJSON(gfBytes); err != nil {
		panic(err)
	}

	// 5. Fault profiles YAML
	faultYaml := `profiles:
  duplicate_delivery:
    description: Re-deliver exact same (eventId, sequence) after worker processing
  out_of_order_stale:
    description: Emit lower sequence number after higher sequence committed to Redis
  sequence_conflict:
    description: Reuse already accepted sequence with mutated coordinates -> pipeline.deadletter
  poison_payload:
    description: Latitude 95.5 or malformed JSON -> pipeline.deadletter
  step_5_to_6_crash:
    description: Crash worker after PG commit before Redis CAS update; replay verifies idempotent recovery
  sequence_gap:
    description: Skip sequence numbers to trigger partition_checkpoints gap + Prometheus alert
`
	_ = os.WriteFile(filepath.Join(*outDir, "fault_profiles.yaml"), []byte(faultYaml), 0o644)

	// 6. SHA-256 manifest
	targets := []string{"geofences.geojson", "city_grid.geojson", "routes.jsonl", "smoke_4events.jsonl", "expected_transitions.json", "fault_profiles.yaml"}
	sort.Strings(targets)
	manOut := ""
	for _, name := range targets {
		raw, err := os.ReadFile(filepath.Join(*outDir, name))
		if err != nil {
			panic(err)
		}
		sum := sha256.Sum256(raw)
		manOut += fmt.Sprintf("%s  %s\n", hex.EncodeToString(sum[:]), name)
	}
	_ = os.WriteFile(filepath.Join(*outDir, "manifest.sha256"), []byte(manOut), 0o644)
	fmt.Printf("Seeded fixtures in %s (seed=%d, 20 fences, 1000 routes, %d expected smoke transitions)\n", *outDir, *seed, len(expected))
}

