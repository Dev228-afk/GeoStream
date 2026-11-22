package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type PartitionOffsets struct {
	Partition       int32 `json:"partition"`
	StartOffset     int64 `json:"startOffset"`
	EndOffset       int64 `json:"endOffset"`
	CommittedOffset int64 `json:"committedOffset"`
}

func EnsureTopics(ctx context.Context, brokers []string, rawPartitions int32) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	specs := []struct {
		name       string
		partitions int32
	}{
		{"courier.gps.raw", rawPartitions},
		{"geofence.enter", 4},
		{"geofence.exit", 4},
		{"pipeline.deadletter", 4},
	}

	for _, sp := range specs {
		resp, err := adm.CreateTopics(ctx, sp.partitions, 1, nil, sp.name)
		if err != nil {
			return err
		}
		for _, r := range resp {
			if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) && !strings.Contains(r.Err.Error(), "TOPIC_ALREADY_EXISTS") {
				return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
			}
		}
	}
	return nil
}

func NewProducer(brokers []string, batchMaxBytes int32) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
	}
	if batchMaxBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(batchMaxBytes))
	}
	return kgo.NewClient(opts...)
}

func NewConsumer(brokers []string, group string, topics []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.SessionTimeout(20*time.Second),
		kgo.HeartbeatInterval(3*time.Second),
	)
}

func SeekGroupToEnd(ctx context.Context, brokers []string, group, topic string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	var offs kadm.Offsets
	if tp, ok := ends[topic]; ok {
		for part, lo := range tp {
			offs.AddOffset(topic, part, lo.Offset, lo.LeaderEpoch)
		}
	}
	_, err = adm.CommitOffsets(ctx, group, offs)
	return err
}

func ProduceJSONSync(ctx context.Context, cl *kgo.Client, topic, key string, val any) (int32, int64, error) {
	payload, err := json.Marshal(val)
	if err != nil {
		return 0, 0, err
	}
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
	}
	res := cl.ProduceSync(ctx, rec)
	if err := res.FirstErr(); err != nil {
		return 0, 0, err
	}
	first := res[0].Record
	return first.Partition, first.Offset, nil
}

func InspectTopicOffsets(ctx context.Context, brokers []string, topic, group string) ([]PartitionOffsets, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	var committed kadm.OffsetResponses
	if group != "" {
		committed, err = adm.FetchOffsetsForTopics(ctx, group, topic)
		if err != nil {
			return nil, err
		}
	}

	var out []PartitionOffsets
	if tp, ok := ends[topic]; ok {
		for part, endOff := range tp {
			sOff := int64(0)
			if stp, ok := starts[topic]; ok {
				if so, ok := stp[part]; ok {
					sOff = so.Offset
				}
			}
			cOff := int64(-1)
			if committed != nil {
				if ctp, ok := committed[topic]; ok {
					if co, ok := ctp[part]; ok && co.Err == nil {
						cOff = co.At
					}
				}
			}
			out = append(out, PartitionOffsets{
				Partition:       part,
				StartOffset:     sOff,
				EndOffset:       endOff.Offset,
				CommittedOffset: cOff,
			})
		}
	}
	return out, nil
}
