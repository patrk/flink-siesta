package flink

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// jm is a fake JobManager: one source vertex whose metric names are set per test, the way a
// Flink 2.2 Kafka source publishes them (recorded from a live job), and one plain vertex.
type jm struct {
	names  []string
	values map[string]float64
	calls  int
}

func newJM(t *testing.T, names []string, values map[string]float64) (*REST, *jm) {
	t.Helper()
	f := &jm{names: names, values: values}
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/abc", func(w http.ResponseWriter, _ *http.Request) {
		f.calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"vertices": []map[string]string{{"id": "src"}, {"id": "map"}}})
	})
	mux.HandleFunc("/jobs/abc/vertices/src/subtasks/metrics", func(w http.ResponseWriter, r *http.Request) {
		f.calls++
		if get := r.URL.Query().Get("get"); get != "" {
			ids := strings.Split(get, ",")
			out := make([]map[string]any, 0, len(ids))
			for _, id := range ids {
				v := f.values[id]
				out = append(out, map[string]any{"id": id, "min": v, "max": v, "avg": v, "sum": v})
			}
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		out := make([]map[string]string, 0, len(f.names))
		for _, n := range f.names {
			out = append(out, map[string]string{"id": n})
		}
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/jobs/abc/vertices/map/subtasks/metrics", func(w http.ResponseWriter, _ *http.Request) {
		f.calls++
		_ = json.NewEncoder(w).Encode([]map[string]string{{"id": "numRecordsIn"}, {"id": "numRecordsOut"}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	r := NewREST(8081)
	r.URL = func(string, string) string { return srv.URL }
	return r, f
}

var (
	fetched = []string{
		"Source__Kafka_Source.KafkaSourceReader.commitsSucceeded",
		"Source__Kafka_Source.KafkaSourceReader.KafkaConsumer.records-lag-max",
		"Source__Kafka_Source.KafkaSourceReader.topic.private.orders.v2.partition.0.currentOffset",
		"Source__Kafka_Source.KafkaSourceReader.topic.private.orders.v2.partition.0.committedOffset",
		"Source__Kafka_Source.KafkaSourceReader.topic.private.orders.v2.partition.1.currentOffset",
		"Source__Kafka_Source.KafkaSourceReader.topic.audit.partition.0.currentOffset",
		"Source__Kafka_Source.split.audit-0.watermark.currentWatermark",
		"Source__Kafka_Source.sourceIdleTime",
		"Source__Kafka_Source.numRecordsIn",
		"numRecordsOut",
	}
	// Right after RUNNING: the reader exists, no splits yet.
	starting = []string{"Source__Kafka_Source.KafkaSourceReader.commitsSucceeded", "numRecordsOut"}
)

func TestSourcesFindsTopicsWithDotsAndTheSourceVertex(t *testing.T) {
	r, f := newJM(t, fetched, nil)
	src, err := r.Sources(context.Background(), "ns", "job", "abc")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(src.Topics, ","); got != "audit,private.orders.v2" {
		t.Fatalf("topics = %q", got)
	}
	if len(src.Vertices) != 1 || src.Vertices[0] != "src" {
		t.Fatalf("source vertices = %v", src.Vertices)
	}
	if f.calls != 3 {
		t.Fatalf("expected 1 job + 2 vertex calls, got %d", f.calls)
	}
}

func TestSourcesBeforeSplitsAreAssignedHasTheVertexButNoTopics(t *testing.T) {
	r, _ := newJM(t, starting, nil)
	src, err := r.Sources(context.Background(), "ns", "job", "abc")
	if err != nil {
		t.Fatal(err)
	}
	if len(src.Topics) != 0 || len(src.Vertices) != 1 {
		t.Fatalf("src = %+v", src)
	}
}

func TestReadReportsLastEmittedOffsetsAndTheBusiestIdleTime(t *testing.T) {
	r, f := newJM(t, fetched, map[string]float64{
		"Source__Kafka_Source.KafkaSourceReader.topic.private.orders.v2.partition.0.currentOffset": 249,
		"Source__Kafka_Source.KafkaSourceReader.topic.private.orders.v2.partition.1.currentOffset": 12,
		"Source__Kafka_Source.KafkaSourceReader.topic.audit.partition.0.currentOffset":             -1,
		"Source__Kafka_Source.sourceIdleTime":                                                      4200,
	})
	src := Sources{Vertices: []string{"src"}}
	before := f.calls
	got, err := r.Read(context.Background(), "ns", "job", "abc", src)
	if err != nil {
		t.Fatal(err)
	}
	if got.Offsets["private.orders.v2"][0] != 249 || got.Offsets["private.orders.v2"][1] != 12 || got.Offsets["audit"][0] != InitialOffset {
		t.Fatalf("offsets = %v", got.Offsets)
	}
	if got.IdleFor != 4200*time.Millisecond {
		t.Fatalf("idle = %s", got.IdleFor)
	}
	if f.calls-before != 2 {
		t.Fatalf("Read must cost a listing and a read per source vertex, cost %d", f.calls-before)
	}
}

func TestReadIsUnknownWithoutGauges(t *testing.T) {
	src := Sources{Vertices: []string{"src"}}
	r, _ := newJM(t, starting, nil)
	if _, err := r.Read(context.Background(), "ns", "job", "abc", src); err == nil {
		t.Fatal("no partition metrics must be unknown")
	}
	r, _ = newJM(t, fetched, nil)
	if _, err := r.Read(context.Background(), "ns", "job", "abc", Sources{}); err == nil {
		t.Fatal("no source vertices must be unknown")
	}
}

func TestSourcesUnreachableIsAnError(t *testing.T) {
	r := NewREST(8081)
	r.URL = func(string, string) string { return "http://127.0.0.1:1" }
	if _, err := r.Sources(context.Background(), "ns", "job", "abc"); err == nil {
		t.Fatal("expected a connection error")
	}
}

func TestDefaultURLIsTheOperatorsRestService(t *testing.T) {
	if got := NewREST(8081).URL("flink", "orders"); got != "http://orders-rest.flink.svc:8081" {
		t.Fatalf("url = %s", got)
	}
}
