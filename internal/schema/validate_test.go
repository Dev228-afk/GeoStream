package schema

import (
	"testing"
	"time"
)

func TestValidateLocationEventAndDeterministicIDs(t *testing.T) {
	id1 := GenerateEventID("run-1", "courier-1", 10, 2026)
	id2 := GenerateEventID("run-1", "courier-1", 10, 2026)
	if id1 != id2 {
		t.Fatalf("expected deterministic eventId, got %s vs %s", id1, id2)
	}

	tid1 := GenerateTransitionID("run-1", "courier-1", "fence-001", 1, 10, "ENTER")
	tid2 := GenerateTransitionID("run-1", "courier-1", "fence-001", 1, 10, "ENTER")
	if tid1 != tid2 || len(tid1) != 19 {
		t.Fatalf("unexpected transitionId: %s vs %s", tid1, tid2)
	}

	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	validJSON := []byte(`{"schemaVersion":1,"runId":"r1","eventId":"` + id1 + `","deviceId":"d1","sequence":1,"routeId":"rt1","eventTime":"2026-11-20T12:00:00.500Z","producedAt":"2026-11-20T12:00:00.503Z","latitude":37.33,"longitude":-121.89,"accuracyM":8.0}`)
	if _, err := ValidateLocationEvent(validJSON, 5*time.Minute, now); err != nil {
		t.Fatalf("expected valid event, got %v", err)
	}

	invalidCases := [][]byte{
		[]byte(`{"schemaVersion":2,"runId":"r1","eventId":"e1","deviceId":"d1","sequence":1,"routeId":"rt1","eventTime":"2026-11-20T12:00:00Z","producedAt":"2026-11-20T12:00:00Z","latitude":37.33,"longitude":-121.89,"accuracyM":8}`),
		[]byte(`{"schemaVersion":1,"runId":"r1","eventId":"e1","deviceId":"d1","sequence":1,"routeId":"rt1","eventTime":"2026-11-20T12:00:00Z","producedAt":"2026-11-20T12:00:00Z","latitude":95.0,"longitude":-121.89,"accuracyM":8}`),
		[]byte(`{"schemaVersion":1,"runId":"r1","eventId":"e1","deviceId":"d1","sequence":1,"routeId":"rt1","eventTime":"2026-11-20T12:10:00Z","producedAt":"2026-11-20T12:00:00Z","latitude":37.33,"longitude":-121.89,"accuracyM":8}`),
		[]byte(`{"schemaVersion":1,"runId":"r1","eventId":"e1","deviceId":"d1","sequence":0,"routeId":"rt1","eventTime":"2026-11-20T12:00:00Z","producedAt":"2026-11-20T12:00:00Z","latitude":37.33,"longitude":-121.89,"accuracyM":8}`),
		[]byte(`{"schemaVersion":1,"runId":"r1","eventId":"e1","deviceId":"d1","sequence":1,"routeId":"rt1","eventTime":"2026-11-20T12:00:00Z","producedAt":"2026-11-20T12:00:00Z","latitude":37.33,"longitude":-121.89,"accuracyM":250}`),
		[]byte(`{"schemaVersion":1,"runId":"r1","extraField":1}`),
	}
	for i, c := range invalidCases {
		if _, err := ValidateLocationEvent(c, 5*time.Minute, now); err == nil {
			t.Fatalf("case %d unexpectedly succeeded", i)
		}
	}
}

