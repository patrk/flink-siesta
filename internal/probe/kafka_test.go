//go:build integration

package probe

import (
	"context"
	"errors"
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
	if err != nil || before.Snapshot["in"] != "0,0" {
		t.Fatalf("want fresh two-partition topic at 0,0, got %v err=%v", before, err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "in", Partition: 0, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	after, err := p.Observe(ctx, []string{"in"})
	if err != nil || after.Snapshot["in"] != "1,0" {
		t.Fatalf("want partition 0 at 1 after one record, got %v", after)
	}
	if _, err := p.Observe(ctx, []string{"nope"}); err == nil {
		t.Fatal("missing topic must be unknown, not zero")
	}

	lag := func() (int64, error) {
		o, err := p.Observe(ctx, []string{"in"})
		if err != nil {
			return 0, err
		}
		return p.Lag(ctx, "g", o.Ends)
	}
	// Lag: a group that never committed is unknown, not "caught up". A fresh broker answers
	// COORDINATOR_NOT_AVAILABLE for a few seconds first, which is unknown for another reason.
	_, err = lag()
	for deadline := time.Now().Add(time.Minute); err != nil && !errors.Is(err, ErrNoCommit) && time.Now().Before(deadline); {
		time.Sleep(time.Second)
		_, err = lag()
	}
	if !errors.Is(err, ErrNoCommit) {
		t.Fatalf("group without commits must be unknown, and say it has no commit: %v", err)
	}
	adm := kadm.NewClient(cl)
	var partial kadm.Offsets
	partial.Add(kadm.Offset{Topic: "in", Partition: 0, At: 1})
	if _, err := adm.CommitOffsets(ctx, "g", partial); err != nil {
		t.Fatal(err)
	}
	if _, err := lag(); !errors.Is(err, ErrNoCommit) {
		t.Fatalf("a partition the group never committed must make lag unknown, and say so: %v", err)
	}
	var committed kadm.Offsets
	committed.Add(kadm.Offset{Topic: "in", Partition: 0, At: 1})
	committed.Add(kadm.Offset{Topic: "in", Partition: 1, At: 0})
	if _, err := adm.CommitOffsets(ctx, "g", committed); err != nil {
		t.Fatal(err)
	}
	if pending, err := lag(); err != nil || pending != 0 {
		t.Fatalf("want caught up (0, nil), got (%d, %v)", pending, err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "in", Partition: 1, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if pending, err := lag(); err != nil || pending != 1 {
		t.Fatalf("want one pending record, got (%d, %v)", pending, err)
	}

	// Offsets the broker no longer holds, as after offsets.retention.minutes: no commit again.
	if _, err := adm.DeleteOffsets(ctx, "g", kadm.TopicsSet{"in": {0: {}, 1: {}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := lag(); !errors.Is(err, ErrNoCommit) {
		t.Fatalf("deleted offsets must read as no commit, got %v", err)
	}
}

// Transactions leave offsets no consumer is given: a marker after every commit or abort, and
// the records of an aborted transaction. Records tells those gaps from records pending.
func TestKafkaRecordsSeesThroughTransactionMarkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := testcontainers.NewDockerProvider(); err != nil {
		t.Skipf("docker not available: %v", err)
	}
	c, err := kafka.Run(ctx, "confluentinc/confluent-local:8.3.1", kafka.WithClusterID("siesta-txn"))
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
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(ctx, 1, 1, nil, "txn"); err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.TransactionalID("siesta-test"), kgo.DefaultProduceTopic("txn"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	transaction := func(n int, commit kgo.TransactionEndTry) {
		t.Helper()
		if err := producer.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for range n {
			if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("v")}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := producer.EndTransaction(ctx, commit); err != nil {
			t.Fatal(err)
		}
	}
	p, err := NewKafka(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	end := func() int64 {
		t.Helper()
		o, err := p.Observe(ctx, []string{"txn"})
		if err != nil {
			t.Fatal(err)
		}
		return o.Ends["txn"][0]
	}
	// The coordinator writes the markers after EndTransaction returns.
	endAt := func(want int64) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); end() != want; time.Sleep(200 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("end offset never reached %d, at %d", want, end())
			}
		}
	}
	records := func(from, to int64) bool {
		t.Helper()
		got, err := p.Records(ctx, "txn", 0, from, to)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	transaction(3, kgo.TryCommit) // records 0..2, commit marker at 3
	endAt(4)
	if records(3, 4) {
		t.Fatal("past the last record there is only the commit marker")
	}
	if !records(2, 4) {
		t.Fatal("record 2 is a record")
	}
	transaction(2, kgo.TryAbort) // aborted records 4..5, abort marker at 6
	endAt(7)
	if records(3, 7) {
		t.Fatal("an aborted transaction and its marker hold no record for a consumer")
	}
	transaction(1, kgo.TryCommit) // record 7, marker at 8
	endAt(9)
	if !records(3, 9) || records(8, 9) {
		t.Fatal("a committed record after an aborted transaction is a record; its marker is not")
	}

	// An open transaction's records may still commit: they count.
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if !records(8, end()) {
		t.Fatal("an open transaction below the end must count as records")
	}
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
}
