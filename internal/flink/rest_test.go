package flink

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeJobManager serves the two endpoints the client uses, with metric names as a Flink 2.2
// Kafka source publishes them. Topic names with dots are the case worth guarding.
func fakeJobManager(t *testing.T, pending map[string]float64) (client *REST, calls *int) {
	t.Helper()
	calls = new(int)
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/abc", func(w http.ResponseWriter, _ *http.Request) {
		*calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"vertices": []map[string]string{{"id": "src"}, {"id": "map"}}})
	})
	mux.HandleFunc("/jobs/abc/vertices/src/subtasks/metrics", func(w http.ResponseWriter, r *http.Request) {
		*calls++
		if get := r.URL.Query().Get("get"); get != "" {
			ids := strings.Split(get, ",")
			out := make([]map[string]any, 0, len(ids))
			for _, id := range ids {
				out = append(out, map[string]any{"id": id, "sum": pending[id]})
			}
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		names := []string{
			"Source__Kafka_Source.KafkaSourceReader.topic.private.orders.v2.partition.0.currentOffset",
			"Source__Kafka_Source.KafkaSourceReader.topic.private.orders.v2.partition.0.committedOffset",
			"Source__Kafka_Source.KafkaSourceReader.topic.private.orders.v2.partition.1.currentOffset",
			"Source__Kafka_Source.KafkaSourceReader.topic.audit.partition.0.currentOffset",
			"Source__Kafka_Source.pendingRecords",
			"Source__Kafka_Source.numRecordsIn",
			"numRecordsOut",
		}
		var out []map[string]string
		for _, n := range names {
			out = append(out, map[string]string{"id": n})
		}
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/jobs/abc/vertices/map/subtasks/metrics", func(w http.ResponseWriter, _ *http.Request) {
		*calls++
		_ = json.NewEncoder(w).Encode([]map[string]string{{"id": "numRecordsIn"}, {"id": "numRecordsOut"}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client = NewREST(8081)
	client.URL = func(string, string) string { return srv.URL }
	return client, calls
}

func TestSourcesFindsTopicsWithDotsAndPendingGauge(t *testing.T) {
	r, calls := fakeJobManager(t, nil)
	src, err := r.Sources(context.Background(), "ns", "job", "abc")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(src.Topics, ","); got != "audit,private.orders.v2" {
		t.Fatalf("topics = %q", got)
	}
	if got := src.Pending["src"]; len(got) != 1 || got[0] != "Source__Kafka_Source.pendingRecords" {
		t.Fatalf("pending metric ids = %v", got)
	}
	if _, has := src.Pending["map"]; has {
		t.Fatal("a vertex without a source must not be asked for pendingRecords")
	}
	if *calls != 3 {
		t.Fatalf("expected 1 job + 2 vertex calls, got %d", *calls)
	}
}

func TestPendingSumsTheSourceVertices(t *testing.T) {
	r, calls := fakeJobManager(t, map[string]float64{"Source__Kafka_Source.pendingRecords": 42})
	src, err := r.Sources(context.Background(), "ns", "job", "abc")
	if err != nil {
		t.Fatal(err)
	}
	before := *calls
	n, err := r.Pending(context.Background(), "ns", "job", "abc", src)
	if err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Fatalf("pending = %d", n)
	}
	if *calls-before != 1 {
		t.Fatalf("Pending must cost one call per source vertex, cost %d", *calls-before)
	}
}

func TestPendingWithoutGaugeIsAnError(t *testing.T) {
	r, _ := fakeJobManager(t, nil)
	if _, err := r.Pending(context.Background(), "ns", "job", "abc", Sources{}); err == nil {
		t.Fatal("no pendingRecords metric must be an error, never zero")
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
