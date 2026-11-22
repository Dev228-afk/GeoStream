package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/example/geostream/internal/backpressure"
	"github.com/example/geostream/internal/geofence"
)

func main() {
	events := flag.Int("events", 2400, "Number of seeded events")
	out := flag.String("out", "evidence/backpressure/run-bp-001", "Output directory")
	flag.Parse()

	gfBytes, err := os.ReadFile("fixtures/geofences.geojson")
	if err != nil {
		panic(err)
	}
	fences, err := geofence.ParseGeoJSON(gfBytes)
	if err != nil {
		panic(err)
	}
	res, err := backpressure.RunMode("unbounded_baseline", fences, *events, *out)
	if err != nil {
		panic(err)
	}
	fmt.Printf("baseline unbounded: %.1f ev/s, maxQ=%d, p99=%.2fms, heap=%.2fMB, persisted=%d\n",
		res.ThroughputEvSec, res.MaxQueueDepth, res.P99Ms, res.PeakHeapMB, res.PersistedCount)
}

