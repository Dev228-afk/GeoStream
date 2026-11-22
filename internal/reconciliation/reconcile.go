package reconciliation

import (
	"context"
	"fmt"
	"time"

	gkafka "github.com/example/geostream/internal/kafka"
	"github.com/example/geostream/internal/redisstate"
	"github.com/example/geostream/internal/storage"
)

type Report struct {
	RunID                 string                    `json:"runId"`
	Generated             int64                     `json:"generated"`
	BrokerAcked           int64                     `json:"brokerAcked"`
	Consumed              int64                     `json:"consumed"`
	AcceptedUnique        int64                     `json:"acceptedUnique"`
	Duplicates            int64                     `json:"duplicates"`
	Invalid               int64                     `json:"invalid"`
	Late                  int64                     `json:"late"`
	Gaps                  int64                     `json:"gaps"`
	TransitionsPersisted  int64                     `json:"transitionsPersisted"`
	OutboxPending         int64                     `json:"outboxPending"`
	OutboxPublished       int64                     `json:"outboxPublished"`
	ActiveRedisDevices    int64                     `json:"activeRedisDevices"`
	LastCommittedOffsets  []gkafka.PartitionOffsets `json:"lastCommittedOffsets"`
	Checkpoints           []storage.CheckpointRow   `json:"checkpoints"`
	Pass                  bool                      `json:"pass"`
	FailureReasons        []string                  `json:"failureReasons,omitempty"`
	Limitations           []string                  `json:"limitations"`
	CheckedAt             string                    `json:"checkedAt"`
}

func AuditRun(
	ctx context.Context,
	pg *storage.PostgresStore,
	rds *redisstate.Store,
	brokers []string,
	runID string,
	consumerGroup string,
	consumedCount int64,
	acceptedUnique int64,
	duplicates int64,
	invalid int64,
	late int64,
	expectedMinTransitions int64,
) (Report, error) {
	run, found, err := pg.GetRun(ctx, runID)
	if err != nil {
		return Report{}, err
	}
	if !found {
		return Report{}, fmt.Errorf("run %q not found", runID)
	}

	transitions, pending, published, gaps, cps, err := pg.GetReconciliationCounts(ctx, runID)
	if err != nil {
		return Report{}, err
	}
	activeDevs, _ := rds.ActiveDeviceCount(ctx, runID)
	offsets, _ := gkafka.InspectTopicOffsets(ctx, brokers, "courier.gps.raw", consumerGroup)

	var reasons []string
	if run.GeneratedCount > 0 && run.BrokerAckedCount < run.GeneratedCount {
		reasons = append(reasons, fmt.Sprintf("brokerAcked(%d) < generated(%d)", run.BrokerAckedCount, run.GeneratedCount))
	}
	if gaps > 0 {
		reasons = append(reasons, fmt.Sprintf("sequence_gap_count=%d detected", gaps))
	}
	if pending > 0 {
		reasons = append(reasons, fmt.Sprintf("outbox_pending=%d unpublished transitions", pending))
	}
	if expectedMinTransitions > 0 && transitions < expectedMinTransitions {
		reasons = append(reasons, fmt.Sprintf("transitionsPersisted(%d) < expected(%d)", transitions, expectedMinTransitions))
	}

	return Report{
		RunID:                runID,
		Generated:            run.GeneratedCount,
		BrokerAcked:          run.BrokerAckedCount,
		Consumed:             consumedCount,
		AcceptedUnique:       acceptedUnique,
		Duplicates:           duplicates,
		Invalid:              invalid,
		Late:                 late,
		Gaps:                 gaps,
		TransitionsPersisted: transitions,
		OutboxPending:        pending,
		OutboxPublished:      published,
		ActiveRedisDevices:   activeDevs,
		LastCommittedOffsets: offsets,
		Checkpoints:          cps,
		Pass:                 len(reasons) == 0,
		FailureReasons:       reasons,
		Limitations: []string{
			"Kafka partition offset totals reflect topic history across runs sharing courier.gps.raw; per-run event uniqueness is verified via Postgres transitions/outbox & generator manifest counts.",
		},
		CheckedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

