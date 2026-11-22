package geofence

import (
	"testing"

	"github.com/example/geostream/internal/schema"
)

func TestDwellHysteresisSuppressesBriefJitter(t *testing.T) {
	rawGeo := []byte(`{
		"type":"FeatureCollection",
		"features":[{
			"type":"Feature","id":"zone-dwell",
			"properties":{"id":"zone-dwell","name":"Dwell Zone","version":1,"enabled":true},
			"geometry":{"type":"Polygon","coordinates":[[[-121.89,37.29],[-121.87,37.29],[-121.87,37.31],[-121.89,37.31],[-121.89,37.29]]]}
		}]
	}`)
	fences, _ := ParseGeoJSON(rawGeo)
	eng, err := NewEngine(fences, 3000) // 3000ms dwell required
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	// Single brief inside blip at t=0 -> suppressed
	ev1 := schema.LocationEventV1{RunId: "r-dw", DeviceId: "d1", Sequence: 1, EventTime: "2026-11-20T12:00:00Z", Longitude: -121.88, Latitude: 37.30}
	if ints, _ := eng.Evaluate(ev1, nil); len(ints) != 0 {
		t.Fatalf("expected 0 transitions on brief blip before dwellMs, got %+v", ints)
	}
	// Jumps back outside at t=1s -> resets pending candidate
	ev2 := schema.LocationEventV1{RunId: "r-dw", DeviceId: "d1", Sequence: 2, EventTime: "2026-11-20T12:00:01Z", Longitude: -121.95, Latitude: 37.30}
	if ints, _ := eng.Evaluate(ev2, nil); len(ints) != 0 {
		t.Fatalf("expected 0 transitions on outside return, got %+v", ints)
	}
	// Enters at t=10s -> pending; stays inside at t=13.5s -> triggers ENTER!
	ev3 := schema.LocationEventV1{RunId: "r-dw", DeviceId: "d1", Sequence: 3, EventTime: "2026-11-20T12:00:10Z", Longitude: -121.88, Latitude: 37.30}
	if ints, _ := eng.Evaluate(ev3, nil); len(ints) != 0 {
		t.Fatalf("expected dwell wait at t=10s, got %+v", ints)
	}
	ev4 := schema.LocationEventV1{RunId: "r-dw", DeviceId: "d1", Sequence: 4, EventTime: "2026-11-20T12:00:13.500Z", Longitude: -121.88, Latitude: 37.30}
	ints, _ := eng.Evaluate(ev4, nil)
	if len(ints) != 1 || ints[0].Type != "ENTER" {
		t.Fatalf("expected confirmed ENTER after >=3000ms dwell, got %+v", ints)
	}
}

