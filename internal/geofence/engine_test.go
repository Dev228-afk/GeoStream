package geofence

import (
	"testing"

	"github.com/example/geostream/internal/schema"
)

func TestGeofenceGeometryEdgeCases(t *testing.T) {
	rawGeo := []byte(`{
		"type":"FeatureCollection",
		"features":[
			{
				"type":"Feature","id":"hole-zone",
				"properties":{"id":"hole-zone","name":"Hollow","version":1,"enabled":true},
				"geometry":{"type":"Polygon","coordinates":[
					[[-122.0,37.0],[-121.0,37.0],[-121.0,38.0],[-122.0,38.0],[-122.0,37.0]],
					[[-121.7,37.3],[-121.3,37.3],[-121.3,37.7],[-121.7,37.7],[-121.7,37.3]]
				]}
			},
			{
				"type":"Feature","id":"overlap-zone",
				"properties":{"id":"overlap-zone","name":"Overlap","version":1,"enabled":true},
				"geometry":{"type":"Polygon","coordinates":[
					[[-121.9,37.1],[-121.4,37.1],[-121.4,37.4],[-121.9,37.4],[-121.9,37.1]]
				]}
			}
		]
	}`)
	fences, err := ParseGeoJSON(rawGeo)
	if err != nil {
		t.Fatalf("ParseGeoJSON failed: %v", err)
	}
	eng, err := NewEngine(fences, 0)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}

	// Boundary considered INSIDE
	evBoundary := schema.LocationEventV1{
		RunId: "r1", DeviceId: "d1", Sequence: 1, EventTime: "2026-11-20T12:00:01Z",
		Longitude: -122.0, Latitude: 37.5,
	}
	intents, err := eng.Evaluate(evBoundary, nil)
	if err != nil || len(intents) != 1 || intents[0].FenceID != "hole-zone" || intents[0].Type != "ENTER" {
		t.Fatalf("expected boundary ENTER hole-zone, got %+v err=%v", intents, err)
	}

	// Overlapping region enters BOTH overlap-zone (new) while already inside hole-zone -> 1 ENTER overlap-zone
	prev := []FenceMembership{{FenceID: "hole-zone", FenceVersion: 1, Inside: true}}
	evOverlap := schema.LocationEventV1{
		RunId: "r1", DeviceId: "d1", Sequence: 2, EventTime: "2026-11-20T12:00:02Z",
		Longitude: -121.8, Latitude: 37.2,
	}
	intents2, err := eng.Evaluate(evOverlap, prev)
	if err != nil || len(intents2) != 1 || intents2[0].FenceID != "overlap-zone" || intents2[0].Type != "ENTER" {
		t.Fatalf("expected ENTER overlap-zone cleanly, got %+v err=%v", intents2, err)
	}

	// Hole center is OUTSIDE hole-zone (and outside overlap-zone) -> 2 EXITs
	prevBoth := []FenceMembership{
		{FenceID: "hole-zone", FenceVersion: 1, Inside: true},
		{FenceID: "overlap-zone", FenceVersion: 1, Inside: true},
	}
	evHole := schema.LocationEventV1{
		RunId: "r1", DeviceId: "d1", Sequence: 3, EventTime: "2026-11-20T12:00:03Z",
		Longitude: -121.5, Latitude: 37.5,
	}
	intents3, err := eng.Evaluate(evHole, prevBoth)
	if err != nil || len(intents3) != 2 || intents3[0].Type != "EXIT" || intents3[1].Type != "EXIT" {
		t.Fatalf("expected 2 EXITs inside hole, got %+v err=%v", intents3, err)
	}
}

