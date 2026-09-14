package flink

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// REST reads what only the running job knows: which Kafka topics its sources consume, the
// last offset each reader emitted and how long the sources have been idle. The operator exposes every JobManager as a Service
// named <deployment>-rest on the rest port, so no discovery is needed.
type REST struct {
	HTTP *http.Client
	// URL returns the base URL for a deployment. Tests point it at a local server.
	URL func(namespace, name string) string
}

func NewREST(port int) *REST {
	return &REST{
		HTTP: &http.Client{Timeout: 10 * time.Second},
		URL: func(ns, name string) string {
			return fmt.Sprintf("http://%s-rest.%s.svc:%d", name, ns, port)
		},
	}
}

// Sources is what the job graph says about its Kafka input.
type Sources struct {
	Topics   []string // sorted and unique; empty when no vertex exposes Kafka source metrics yet
	Vertices []string // ids of the vertices that carry a Kafka source reader
}

// The Kafka source registers, under its operator scope, one group per topic and partition,
// KafkaSourceReader.topic.<topic>.partition.<n>.<currentOffset|committedOffset>, as soon as the
// reader gets its splits, and the standard source gauge sourceIdleTime from the start. Topic
// names may contain dots, so the partition part anchors the match.
var (
	readerMetric = regexp.MustCompile(`(^|\.)KafkaSourceReader\.`)
	topicMetric  = regexp.MustCompile(`KafkaSourceReader\.topic\.(.+)\.partition\.\d+\.(currentOffset|committedOffset)$`)
	offsetMetric = regexp.MustCompile(`KafkaSourceReader\.topic\.(.+)\.partition\.(\d+)\.currentOffset$`)
	idleMetric   = regexp.MustCompile(`(^|\.)sourceIdleTime$`)
)

// Sources lists the job's vertices and reads their metric names. It costs one call for the job
// plus one per vertex, so callers cache the result per job id once topics have shown up.
func (r *REST) Sources(ctx context.Context, ns, name, jobID string) (Sources, error) {
	base := r.URL(ns, name)
	var job struct {
		Vertices []struct {
			ID string `json:"id"`
		} `json:"vertices"`
	}
	if err := r.get(ctx, base+"/jobs/"+jobID, &job); err != nil {
		return Sources{}, err
	}
	out := Sources{}
	topics := map[string]bool{}
	for _, v := range job.Vertices {
		ids, err := r.metricNames(ctx, base, jobID, v.ID)
		if err != nil {
			return Sources{}, err
		}
		reader := false
		for _, id := range ids {
			if match := topicMetric.FindStringSubmatch(id); match != nil {
				topics[match[1]] = true
			}
			reader = reader || readerMetric.MatchString(id)
		}
		if reader {
			out.Vertices = append(out.Vertices, v.ID)
		}
	}
	for t := range topics {
		out.Topics = append(out.Topics, t)
	}
	sort.Strings(out.Topics)
	return out, nil
}

// Reading is the running job's own account of its Kafka input this tick.
type Reading struct {
	// Offsets maps topic -> partition -> the last offset the reader emitted, InitialOffset before
	// the first record. That is the position a savepoint would record, so end offset minus
	// (offset+1) is exactly what the job has not processed.
	Offsets map[string]map[int]int64
	// IdleFor is the smallest sourceIdleTime over the source vertices: 0 while any source emits,
	// counting since the last emitted record otherwise, including for a source that never had one.
	IdleFor time.Duration
}

// InitialOffset is the currentOffset gauge's value before the reader has emitted a record.
const InitialOffset = -1

// Read fetches the current offsets and the idle time of the source vertices found by Sources.
// Two calls per source vertex. A job without source vertices, or without the gauges yet, is
// an error: unknown never reads as caught up.
func (r *REST) Read(ctx context.Context, ns, name, jobID string, src Sources) (Reading, error) {
	if len(src.Vertices) == 0 {
		return Reading{}, fmt.Errorf("job %s exposes no Kafka source reader metrics", jobID)
	}
	base := r.URL(ns, name)
	out := Reading{Offsets: map[string]map[int]int64{}, IdleFor: -1}
	for _, vertex := range src.Vertices {
		ids, err := r.metricNames(ctx, base, jobID, vertex)
		if err != nil {
			return Reading{}, err
		}
		var wanted []string
		for _, id := range ids {
			if offsetMetric.MatchString(id) || idleMetric.MatchString(id) {
				wanted = append(wanted, id)
			}
		}
		if len(wanted) == 0 {
			return Reading{}, fmt.Errorf("vertex %s has no partition metrics yet", vertex)
		}
		agg, err := r.aggregated(ctx, base, jobID, vertex, wanted)
		if err != nil {
			return Reading{}, err
		}
		idleSeen := false
		for _, a := range agg {
			if m := offsetMetric.FindStringSubmatch(a.ID); m != nil {
				p, _ := strconv.Atoi(m[2])
				if out.Offsets[m[1]] == nil {
					out.Offsets[m[1]] = map[int]int64{}
				}
				out.Offsets[m[1]][p] = int64(a.Max) // one subtask owns the partition; the others report nothing
				continue
			}
			if idleMetric.MatchString(a.ID) {
				idleSeen = true
				idle := time.Duration(a.Min) * time.Millisecond // the busiest subtask decides
				if out.IdleFor < 0 || idle < out.IdleFor {
					out.IdleFor = idle
				}
			}
		}
		if !idleSeen {
			return Reading{}, fmt.Errorf("vertex %s has no sourceIdleTime metric", vertex)
		}
	}
	return out, nil
}

type aggregate struct {
	ID  string  `json:"id"`
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

func (r *REST) metricNames(ctx context.Context, base, jobID, vertex string) ([]string, error) {
	var names []struct {
		ID string `json:"id"`
	}
	if err := r.get(ctx, base+"/jobs/"+jobID+"/vertices/"+vertex+"/subtasks/metrics", &names); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(names))
	for _, n := range names {
		ids = append(ids, n.ID)
	}
	return ids, nil
}

func (r *REST) aggregated(ctx context.Context, base, jobID, vertex string, ids []string) ([]aggregate, error) {
	q := url.Values{"get": {strings.Join(ids, ",")}}
	var agg []aggregate
	if err := r.get(ctx, base+"/jobs/"+jobID+"/vertices/"+vertex+"/subtasks/metrics?"+q.Encode(), &agg); err != nil {
		return nil, err
	}
	if len(agg) == 0 {
		return nil, fmt.Errorf("vertex %s returned no values for %s", vertex, strings.Join(ids, ","))
	}
	return agg, nil
}

func (r *REST) get(ctx context.Context, u string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", u, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(into)
}
