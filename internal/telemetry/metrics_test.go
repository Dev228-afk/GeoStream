package telemetry

import (
	"testing"
)

func TestAll15CanonicalMetricsHaveBoundedCardinality(t *testing.T) {
	m := NewMetrics(nil)
	m.EventsReceived.Inc()
	m.EventsAccepted.Inc()
	m.EventsDuplicate.Inc()
	m.EventsInvalid.Inc()
	m.SequenceGaps.Inc()
	m.Transitions.WithLabelValues("ENTER").Inc()
	m.KafkaLag.WithLabelValues("0").Set(1)
	m.ProcessingLatency.Observe(0.002)
	m.OutputLatency.Observe(0.004)
	m.DBChannelDepth.Set(10)
	m.DBWorkerErrors.Inc()
	m.BackpressureSecs.Inc()
	m.RedisErrors.Inc()
	m.ReplayAttempts.Inc()
	m.DeadletterTotal.Inc()

	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics failed: %v", err)
	}
	if len(families) != 15 {
		t.Fatalf("expected 15 Prometheus metric families, got %d", len(families))
	}
	for _, fam := range families {
		for _, metric := range fam.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "deviceId" || lp.GetName() == "device_id" {
					t.Fatalf("unbounded deviceId label prohibited on metric %s", fam.GetName())
				}
			}
		}
	}
}

