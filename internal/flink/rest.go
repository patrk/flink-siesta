package flink

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// REST reads what only the running job knows: which Kafka topics its sources consume and how
// many records they have not fetched yet. The operator exposes every JobManager as a Service
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
	Topics []string // sorted and unique; empty when the job exposes no Kafka source metrics
	// Pending maps a source vertex id to the metric ids that carry its pendingRecords gauge.
	// Kept so that Pending can ask for exactly those without listing metrics again.
	Pending map[string][]string
}

// The Kafka source registers, under its operator scope, one group per topic and partition,
// KafkaSourceReader.topic.<topic>.partition.<n>.<currentOffset|committedOffset>, and the
// standard source gauge pendingRecords. Topic names may contain dots, so the partition part
// anchors the match.
var (
	topicMetric   = regexp.MustCompile(`KafkaSourceReader\.topic\.(.+)\.partition\.\d+\.(currentOffset|committedOffset)$`)
	pendingMetric = regexp.MustCompile(`(^|\.)pendingRecords$`)
)

// Sources lists the job's vertices and reads their metric names once. It costs one call for
// the job plus one per vertex, so callers cache the result per job id.
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
	out := Sources{Pending: map[string][]string{}}
	topics := map[string]bool{}
	for _, v := range job.Vertices {
		var names []struct {
			ID string `json:"id"`
		}
		if err := r.get(ctx, base+"/jobs/"+jobID+"/vertices/"+v.ID+"/subtasks/metrics", &names); err != nil {
			return Sources{}, err
		}
		for _, m := range names {
			if match := topicMetric.FindStringSubmatch(m.ID); match != nil {
				topics[match[1]] = true
			}
			if pendingMetric.MatchString(m.ID) {
				out.Pending[v.ID] = append(out.Pending[v.ID], m.ID)
			}
		}
	}
	for t := range topics {
		out.Topics = append(out.Topics, t)
	}
	sort.Strings(out.Topics)
	return out, nil
}

// Pending sums pendingRecords over the source vertices found by Sources. Zero vertices means
// the job cannot tell, which is an error here, not a zero: unknown never reads as caught up.
func (r *REST) Pending(ctx context.Context, ns, name, jobID string, src Sources) (int64, error) {
	if len(src.Pending) == 0 {
		return 0, fmt.Errorf("job %s exposes no pendingRecords metric", jobID)
	}
	base := r.URL(ns, name)
	var total int64
	for vertex, ids := range src.Pending {
		q := url.Values{"get": {strings.Join(ids, ",")}, "agg": {"sum"}}
		var agg []struct {
			ID  string  `json:"id"`
			Sum float64 `json:"sum"`
		}
		if err := r.get(ctx, base+"/jobs/"+jobID+"/vertices/"+vertex+"/subtasks/metrics?"+q.Encode(), &agg); err != nil {
			return 0, err
		}
		if len(agg) == 0 {
			return 0, fmt.Errorf("vertex %s returned no pendingRecords", vertex)
		}
		for _, a := range agg {
			total += int64(a.Sum)
		}
	}
	return total, nil
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
