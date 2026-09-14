package probe

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka reads log end offsets with one admin request per Observe.
type Kafka struct {
	cl  *kgo.Client
	adm *kadm.Client
}

// NewKafka takes ready-made client options; KafkaConfig.Opts builds them from configuration.
func NewKafka(opts ...kgo.Opt) (*Kafka, error) {
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Kafka{cl: cl, adm: kadm.NewClient(cl)}, nil
}

func (k *Kafka) Close() { k.cl.Close() }

func (k *Kafka) Observe(ctx context.Context, topics []string) (Offsets, error) {
	ends, err := k.adm.ListEndOffsets(ctx, topics...)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return Offsets{}, fmt.Errorf("end offsets for %s: %w", strings.Join(topics, ","), err)
	}
	// One entry per topic, end offsets joined in partition order: "topic" -> "8812,8790,9001".
	// Ten thousand partitions fit comfortably in a ConfigMap this way; per-partition keys would not.
	perTopic := map[string][]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		parts := perTopic[o.Topic]
		for int32(len(parts)) <= o.Partition {
			parts = append(parts, -1)
		}
		parts[o.Partition] = o.Offset
		perTopic[o.Topic] = parts
	})
	if len(perTopic) == 0 {
		return Offsets{}, fmt.Errorf("no partitions for %s", strings.Join(topics, ","))
	}
	out := make(map[string]string, len(perTopic))
	for topic, parts := range perTopic {
		var b strings.Builder
		for i, off := range parts {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.FormatInt(off, 10))
		}
		out[topic] = b.String()
	}
	return Offsets{Snapshot: out, Ends: perTopic}, nil
}

// Lag sums end offset minus the group's committed offset over every partition of the topics.
// Flink commits the next offset to read on each checkpoint, and the end offset is the next
// offset to write, so the difference is exactly the records not yet checkpointed as consumed.
func (k *Kafka) Lag(ctx context.Context, group string, topics []string) (int64, error) {
	committed, err := k.adm.FetchOffsets(ctx, group)
	if err == nil {
		err = committed.Error()
	}
	if err != nil {
		return 0, fmt.Errorf("committed offsets for group %s: %w", group, err)
	}
	ends, err := k.adm.ListEndOffsets(ctx, topics...)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return 0, fmt.Errorf("end offsets for %s: %w", strings.Join(topics, ","), err)
	}
	var pending int64
	var uncommitted error
	ends.Each(func(o kadm.ListedOffset) {
		c, ok := committed.Lookup(o.Topic, o.Partition)
		if !ok || c.Err != nil {
			// A partition this group never committed: we cannot say it is caught up.
			uncommitted = fmt.Errorf("group %s has no committed offset for %s partition %d", group, o.Topic, o.Partition)
			return
		}
		if d := o.Offset - c.At; d > 0 {
			pending += d
		}
	})
	return pending, uncommitted
}
