//go:build integration

package probe

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Run with: go test -tags integration ./internal/probe/
func TestKafkaEndOffsets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if _, err := testcontainers.NewDockerProvider(); err != nil {
		t.Skipf("docker not available: %v", err)
	}
	c, err := kafka.Run(ctx, "confluentinc/confluent-local:8.3.1", kafka.WithClusterID("siesta-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(context.Background()); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// ManualPartitioner makes franz-go honour Record.Partition; the default partitioner ignores it.
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, 2, 1, nil, "in"); err != nil {
		t.Fatal(err)
	}

	p, err := NewKafka(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	before, ok := p.Observe(ctx, []string{"in"})
	if !ok || before["in-0"] != "0" || before["in-1"] != "0" {
		t.Fatalf("want fresh topic at 0, got %v ok=%v", before, ok)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "in", Partition: 0, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	after, ok := p.Observe(ctx, []string{"in"})
	if !ok || after["in-0"] != "1" {
		t.Fatalf("want in-0 at 1 after one record, got %v", after)
	}
	if _, ok := p.Observe(ctx, []string{"nope"}); ok {
		t.Fatal("missing topic must be unknown, not zero")
	}

	// Lag: a group that never committed is unknown, not "caught up".
	if _, ok := p.Lag(ctx, "g", []string{"in"}); ok {
		t.Fatal("group without commits must be unknown")
	}
	adm := kadm.NewClient(cl)
	var committed kadm.Offsets
	committed.Add(kadm.Offset{Topic: "in", Partition: 0, At: 1})
	committed.Add(kadm.Offset{Topic: "in", Partition: 1, At: 0})
	if _, err := adm.CommitOffsets(ctx, "g", committed); err != nil {
		t.Fatal(err)
	}
	if pending, ok := p.Lag(ctx, "g", []string{"in"}); !ok || pending != 0 {
		t.Fatalf("want caught up (0, true), got (%d, %v)", pending, ok)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "in", Partition: 1, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if pending, ok := p.Lag(ctx, "g", []string{"in"}); !ok || pending != 1 {
		t.Fatalf("want one pending record, got (%d, %v)", pending, ok)
	}
}
