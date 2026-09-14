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

	before, err := p.Observe(ctx, []string{"in"})
	if err != nil || before["in"] != "0,0" {
		t.Fatalf("want fresh two-partition topic at 0,0, got %v err=%v", before, err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "in", Partition: 0, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	after, err := p.Observe(ctx, []string{"in"})
	if err != nil || after["in"] != "1,0" {
		t.Fatalf("want partition 0 at 1 after one record, got %v", after)
	}
	if _, err := p.Observe(ctx, []string{"nope"}); err == nil {
		t.Fatal("missing topic must be unknown, not zero")
	}

	// Lag: a group that never committed is unknown, not "caught up".
	if _, err := p.Lag(ctx, "g", []string{"in"}); err == nil {
		t.Fatal("group without commits must be unknown")
	}
	adm := kadm.NewClient(cl)
	var partial kadm.Offsets
	partial.Add(kadm.Offset{Topic: "in", Partition: 0, At: 1})
	if _, err := adm.CommitOffsets(ctx, "g", partial); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Lag(ctx, "g", []string{"in"}); err == nil {
		t.Fatal("a partition the group never committed must make lag unknown")
	}
	var committed kadm.Offsets
	committed.Add(kadm.Offset{Topic: "in", Partition: 0, At: 1})
	committed.Add(kadm.Offset{Topic: "in", Partition: 1, At: 0})
	if _, err := adm.CommitOffsets(ctx, "g", committed); err != nil {
		t.Fatal(err)
	}
	if pending, err := p.Lag(ctx, "g", []string{"in"}); err != nil || pending != 0 {
		t.Fatalf("want caught up (0, nil), got (%d, %v)", pending, err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "in", Partition: 1, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if pending, err := p.Lag(ctx, "g", []string{"in"}); err != nil || pending != 1 {
		t.Fatalf("want one pending record, got (%d, %v)", pending, err)
	}
}
