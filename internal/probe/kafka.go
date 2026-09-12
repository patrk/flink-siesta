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
