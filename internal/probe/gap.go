package probe

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Batch attribute bits of the record batch format, magic 2.
const (
	transactionalBatch = 0x10
	controlBatch       = 0x20
)

// gapFetches bounds how many fetches one question may take: a gap of aborted records larger
// than this many fetches stays unanswered, which the job gate reads as busy.
const gapFetches = 8

// gapKey is one question asked of a partition.
type gapKey struct {
	topic     string
	partition int32
	from, to  int64
}

// gaps remembers the ranges found to hold no committed record. A log only grows at its end, so
// a range below the last stable offset with no committed record in it never gets one.
type gaps struct {
	mu    sync.Mutex
	empty map[gapKey]bool
}

func (g *gaps) known(k gapKey) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.empty[k]
}

func (g *gaps) remember(k gapKey) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.empty == nil || len(g.empty) >= 10000 {
		g.empty = map[gapKey]bool{}
	}
	g.empty[k] = true
}

// Records says whether the partition holds a record a read_committed consumer would deliver
// at an offset in [from, to). Commit and abort markers take an offset each and are never
// delivered, nor are the records of an aborted transaction, and compaction leaves offsets with
// no record at all: a reader can be at the last record and still short of the end offset. A
// transaction still open below to counts as records, since its records may yet commit.
func (k *Kafka) Records(ctx context.Context, topic string, partition int32, from, to int64) (bool, error) {
	key := gapKey{topic, partition, from, to}
	if from >= to || k.gaps.known(key) {
		return false, nil
	}
	md, err := k.adm.Metadata(ctx, topic)
	if err != nil {
		return false, fmt.Errorf("metadata for %s: %w", topic, err)
	}
	detail, ok := md.Topics[topic]
	if !ok {
		return false, fmt.Errorf("metadata for %s: topic not listed", topic)
	}
	if detail.Err != nil {
		return false, fmt.Errorf("metadata for %s: %w", topic, detail.Err)
	}
	p, ok := detail.Partitions[partition]
	if !ok || p.Err != nil || p.Leader < 0 {
		return false, fmt.Errorf("no leader for %s partition %d", topic, partition)
	}
	aborting := map[int64]bool{} // producers whose aborted transaction has begun and not yet ended
	var aborted []kmsg.FetchResponseTopicPartitionAbortedTransaction
	next := from
	for range gapFetches {
		req := kmsg.NewPtrFetchRequest()
		req.ReplicaID, req.MaxWaitMillis, req.MinBytes, req.MaxBytes = -1, 0, 1, 1<<20
		req.IsolationLevel, req.SessionEpoch = 1, -1 // read_committed, no fetch session
		rt := kmsg.NewFetchRequestTopic()
		rt.Topic, rt.TopicID = topic, detail.ID
		rp := kmsg.NewFetchRequestTopicPartition()
		rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes = partition, next, 1<<20
		rp.CurrentLeaderEpoch, rp.LogStartOffset = -1, -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, k.cl.Broker(int(p.Leader)))
		if err != nil {
			return false, fmt.Errorf("fetch %s partition %d at %d: %w", topic, partition, next, err)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			return false, fmt.Errorf("fetch %s partition %d: unexpected response shape", topic, partition)
		}
		rpart := resp.Topics[0].Partitions[0]
		if err := kerr.ErrorForCode(rpart.ErrorCode); err != nil {
			return false, fmt.Errorf("fetch %s partition %d at %d: %w", topic, partition, next, err)
		}
		if rpart.LastStableOffset >= 0 && rpart.LastStableOffset < to {
			return true, nil // a transaction is open below to
		}
		aborted = append(aborted, rpart.AbortedTransactions...)
		slices.SortFunc(aborted, func(a, b kmsg.FetchResponseTopicPartitionAbortedTransaction) int {
			return int(a.FirstOffset - b.FirstOffset)
		})
		raw, progressed := rpart.RecordBatches, false
		for len(raw) >= 12 {
			size := 12 + int(int32(binary.BigEndian.Uint32(raw[8:12])))
			if size < 12 || size > len(raw) {
				break // the last batch can be cut off at the byte limit; fetched again next round
			}
			var b kmsg.RecordBatch
			if err := b.ReadFrom(raw[:size]); err != nil {
				return false, fmt.Errorf("decode batch of %s partition %d: %w", topic, partition, err)
			}
			raw = raw[size:]
			first, last := b.FirstOffset, b.FirstOffset+int64(b.LastOffsetDelta)
			if last < next {
				continue
			}
			if first >= to {
				k.gaps.remember(key)
				return false, nil
			}
			next, progressed = last+1, true
			for len(aborted) > 0 && aborted[0].FirstOffset <= first {
				aborting[aborted[0].ProducerID] = true
				aborted = aborted[1:]
			}
			switch {
			case b.Attributes&controlBatch != 0:
				delete(aborting, b.ProducerID) // the marker ends the transaction, aborted or not
			case b.Attributes&transactionalBatch != 0 && aborting[b.ProducerID]:
			default:
				return true, nil
			}
			if next >= to {
				k.gaps.remember(key)
				return false, nil
			}
		}
		if !progressed {
			return false, fmt.Errorf("fetch %s partition %d at %d returned no batch below %d", topic, partition, next, to)
		}
	}
	return false, fmt.Errorf("%s partition %d: [%d, %d) not read within %d fetches", topic, partition, from, to, gapFetches)
}
