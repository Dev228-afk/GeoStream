package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers       []string
	PostgresURL        string
	RedisAddr          string
	APIAddr            string
	MetricsAddr        string
	PrometheusURL      string
	AlertmanagerURL    string
	EvidenceDir        string
	FixturesDir        string
	WorkerPool         int
	ChannelCap         int
	DwellMS            int64
	AllowedFutureSkew  time.Duration
	RedisTTL           time.Duration
	ConsumerGroup      string
	TopicRaw           string
	TopicEnter         string
	TopicExit          string
	TopicDLQ           string
	ShutdownGrace      time.Duration
	RawPartitions      int32
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func intOr(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}

func Load() Config {
	brokersRaw := envOr("KAFKA_BROKERS", "127.0.0.1:19092")
	parts := strings.Split(brokersRaw, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			brokers = append(brokers, s)
		}
	}
	return Config{
		KafkaBrokers:      brokers,
		PostgresURL:       envOr("DATABASE_URL", "postgres://geostream:geostream_local_2026@127.0.0.1:5437/geostream?sslmode=disable"),
		RedisAddr:         envOr("REDIS_ADDR", "127.0.0.1:6383"),
		APIAddr:           envOr("API_ADDR", "127.0.0.1:8200"),
		MetricsAddr:       envOr("METRICS_ADDR", "127.0.0.1:8201"),
		PrometheusURL:     envOr("PROMETHEUS_URL", "http://127.0.0.1:9095"),
		AlertmanagerURL:   envOr("ALERTMANAGER_URL", "http://127.0.0.1:9096"),
		EvidenceDir:       envOr("GEOSTREAM_EVIDENCE_DIR", "./evidence"),
		FixturesDir:       envOr("GEOSTREAM_FIXTURES_DIR", "./fixtures"),
		WorkerPool:        intOr("GEOSTREAM_WORKER_POOL", 8),
		ChannelCap:        intOr("GEOSTREAM_CHANNEL_CAP", 4096),
		DwellMS:           int64(intOr("GEOSTREAM_DWELL_MS", 0)),
		AllowedFutureSkew: time.Duration(intOr("GEOSTREAM_ALLOWED_FUTURE_SKEW_MS", 300000)) * time.Millisecond,
		RedisTTL:          time.Duration(intOr("GEOSTREAM_REDIS_TTL_SECONDS", 86400)) * time.Second,
		ConsumerGroup:     envOr("KAFKA_CONSUMER_GROUP", "geostream-workers"),
		TopicRaw:          "courier.gps.raw",
		TopicEnter:        "geofence.enter",
		TopicExit:         "geofence.exit",
		TopicDLQ:          "pipeline.deadletter",
		ShutdownGrace:     5 * time.Second,
		RawPartitions:     int32(intOr("KAFKA_RAW_PARTITIONS", 16)),
	}
}

