package probe

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	ctrl "sigs.k8s.io/controller-runtime"
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

func (k *Kafka) Observe(ctx context.Context, topics []string) (map[string]string, bool) {
	log := ctrl.LoggerFrom(ctx)
	ends, err := k.adm.ListEndOffsets(ctx, topics...)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		log.Info("end offsets unavailable", "topics", topics, "err", err.Error())
		return nil, false
	}
	out := make(map[string]string)
	ends.Each(func(o kadm.ListedOffset) {
		out[fmt.Sprintf("%s-%d", o.Topic, o.Partition)] = strconv.FormatInt(o.Offset, 10)
	})
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// Lag sums end offset minus the group's committed offset over every partition of the topics.
// Flink commits the next offset to read on each checkpoint, and the end offset is the next
// offset to write, so the difference is exactly the records not yet checkpointed as consumed.
func (k *Kafka) Lag(ctx context.Context, group string, topics []string) (int64, bool) {
	log := ctrl.LoggerFrom(ctx)
	committed, err := k.adm.FetchOffsets(ctx, group)
	if err == nil {
		err = committed.Error()
	}
	if err != nil {
		log.Info("committed offsets unavailable", "group", group, "err", err.Error())
		return 0, false
	}
	ends, err := k.adm.ListEndOffsets(ctx, topics...)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		log.Info("end offsets unavailable", "topics", topics, "err", err.Error())
		return 0, false
	}
	var pending int64
	known := true
	ends.Each(func(o kadm.ListedOffset) {
		c, ok := committed.Lookup(o.Topic, o.Partition)
		if !ok || c.Err != nil {
			known = false // a partition this group never committed: we cannot say it is caught up
			return
		}
		if d := o.Offset - c.At; d > 0 {
			pending += d
		}
	})
	return pending, known
}
