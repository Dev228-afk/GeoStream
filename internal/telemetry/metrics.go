package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
)

var LatencyBuckets = []float64{0.0005, 0.001, 0.002, 0.004, 0.008, 0.012, 0.02, 0.05, 0.1, 0.5, 1.0}

type Metrics struct {
	Registry           *prometheus.Registry
	EventsReceived     prometheus.Counter
	EventsAccepted     prometheus.Counter
	EventsDuplicate    prometheus.Counter
	EventsInvalid      prometheus.Counter
	SequenceGaps       prometheus.Counter
	Transitions        *prometheus.CounterVec
	KafkaLag           *prometheus.GaugeVec
	ProcessingLatency  prometheus.Histogram // INGEST_INTERNAL boundary (Kafka consume -> Redis + geofence decision)
	OutputLatency      prometheus.Histogram // PIPELINE_TRANSITION boundary (source/ingest -> PG commit + outbox publish)
	DBChannelDepth     prometheus.Gauge
	DBWorkerErrors     prometheus.Counter
	BackpressureSecs   prometheus.Counter
	RedisErrors        prometheus.Counter
	ReplayAttempts     prometheus.Counter
	DeadletterTotal    prometheus.Counter
}

func NewMetrics(reg *prometheus.Registry) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m := &Metrics{
		Registry: reg,
		EventsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_events_received_total",
			Help: "Total location events consumed from Kafka before deduplication.",
		}),
		EventsAccepted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_events_accepted_total",
			Help: "Total unique location events accepted into state progression.",
		}),
		EventsDuplicate: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_events_duplicate_total",
			Help: "Total duplicate or stale location events suppressed.",
		}),
		EventsInvalid: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_events_invalid_total",
			Help: "Total invalid or conflicting events routed to DLQ.",
		}),
		SequenceGaps: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_sequence_gaps_total",
			Help: "Total detected monotonic device sequence gaps.",
		}),
		Transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "geostream_geofence_transitions_total",
			Help: "Total durable geofence transitions emitted, partitioned strictly by bounded type (ENTER|EXIT).",
		}, []string{"type"}),
		KafkaLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "geostream_kafka_lag",
			Help: "Estimated consumer offset lag by Kafka partition ID.",
		}, []string{"partition"}),
		ProcessingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "geostream_processing_latency_seconds",
			Help:    "INGEST_INTERNAL boundary latency in seconds (consume to geofence+Redis decision).",
			Buckets: LatencyBuckets,
		}),
		OutputLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "geostream_output_latency_seconds",
			Help:    "PIPELINE_TRANSITION boundary latency in seconds (ingest to durable PG+outbox persistence).",
			Buckets: LatencyBuckets,
		}),
		DBChannelDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "geostream_db_channel_depth",
			Help: "Current buffered items waiting on bounded persistence channel.",
		}),
		DBWorkerErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_db_worker_errors_total",
			Help: "Total transient or failed PostgreSQL persistence errors.",
		}),
		BackpressureSecs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_backpressure_seconds_total",
			Help: "Cumulative seconds Kafka partition consumption was paused due to full bounded channel.",
		}),
		RedisErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_redis_errors_total",
			Help: "Total Redis read/CAS errors encountered in pipeline.",
		}),
		ReplayAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_replay_attempts_total",
			Help: "Total DLQ or offset replay records processed.",
		}),
		DeadletterTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "geostream_deadletter_total",
			Help: "Total records acknowledged onto pipeline.deadletter.",
		}),
	}

	reg.MustRegister(
		m.EventsReceived,
		m.EventsAccepted,
		m.EventsDuplicate,
		m.EventsInvalid,
		m.SequenceGaps,
		m.Transitions,
		m.KafkaLag,
		m.ProcessingLatency,
		m.OutputLatency,
		m.DBChannelDepth,
		m.DBWorkerErrors,
		m.BackpressureSecs,
		m.RedisErrors,
		m.ReplayAttempts,
		m.DeadletterTotal,
	)
	return m
}

