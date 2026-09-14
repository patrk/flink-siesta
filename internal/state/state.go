// Package state is everything the controller remembers, persisted in a ConfigMap (ADR 10).
package state

import (
	"encoding/json"
	"strconv"
	"time"
)

type Phase string

const (
	Active        Phase = "active"
	Suspended     Phase = "suspended"
	Unrecoverable Phase = "unrecoverable"
)

type State struct {
	Phase          Phase
	Snapshot       map[string]string
	LastActivityAt time.Time
	SuspendedAt    time.Time
	AwakeSince     time.Time
	Restarts       RestartBudget
	Reason         string
	Generation     int64     // metadata.generation last seen; a newer one clears unrecoverable
	ResumedAt      time.Time // set when we resume; cleared, and the latency reported, once the job is RUNNING
	Pending        int64     // records the consumer group has not consumed, last time we could tell
	Outage         Outage    // the source could not be asked last tick, and since when
	Reported       Reported  // what has already been said about the current situation
}

// Outage is the source's reachability as of the last tick. It drives one event per edge.
type Outage struct {
	Down  bool
	Since time.Time // when the current outage was first seen; names it in the events
}

// Reported remembers which once-per-situation events have been raised, so a tick never
// repeats them. Each field is cleared when its situation ends.
type Reported struct {
	SuspendChecked bool   // the operator completed our suspend and we checked for a savepoint
	ResumeStalled  bool   // ResumeStalled was raised for the current resume
	SuspendStalled bool   // SuspendStalled was raised for the current suspend
	SourcesChecked string // job id whose graph the sources annotation was last checked against
}

func Initial(now time.Time) State {
	return State{Phase: Active, Snapshot: map[string]string{}, LastActivityAt: now, AwakeSince: now, Reason: "first observation"}
}

// Read returns ok=false when there is no state yet, or it is unreadable (treated as never seen).
func Read(prefix string, ann map[string]string) (State, bool) {
	phase, has := ann[prefix+"/state"]
	if !has {
		return State{}, false
	}
	s := State{Phase: Phase(phase), Snapshot: map[string]string{}, Reason: ann[prefix+"/reason"]}
	if v := ann[prefix+"/offsets"]; v != "" {
		if err := json.Unmarshal([]byte(v), &s.Snapshot); err != nil {
			return State{}, false
		}
	}
	if v := ann[prefix+"/restarts"]; v != "" {
		if err := json.Unmarshal([]byte(v), &s.Restarts); err != nil {
			return State{}, false
		}
	}
	s.LastActivityAt, _ = parseTime(ann[prefix+"/last-activity-at"])
	s.SuspendedAt, _ = parseTime(ann[prefix+"/suspended-at"])
	s.AwakeSince, _ = parseTime(ann[prefix+"/awake-since"])
	s.Generation, _ = strconv.ParseInt(ann[prefix+"/generation"], 10, 64)
	s.ResumedAt, _ = parseTime(ann[prefix+"/resumed-at"])
	return s, true
}

// Annotations is what goes on the FlinkDeployment itself: only what a human wants to see in
// kubectl describe. Everything else lives in the ConfigMap, see Data. Keys that earlier
// versions wrote on the object are set to "" so a merge patch removes them.
func (s State) Annotations(prefix string) map[string]string {
	return map[string]string{
		prefix + "/state":   string(s.Phase), // "" for the zero State: the key is removed
		prefix + "/reason":  s.Reason,
		prefix + "/offsets": "", prefix + "/restarts": "", prefix + "/last-activity-at": "",
		prefix + "/suspended-at": "", prefix + "/awake-since": "", prefix + "/generation": "", prefix + "/resumed-at": "",
	}
}

// Data is the ConfigMap form: one readable key per field, so kubectl get cm -o yaml needs no jq.
func (s State) Data() map[string]string {
	snap, _ := json.Marshal(s.Snapshot)
	restarts, _ := json.Marshal(s.Restarts)
	return map[string]string{
		"state":             string(s.Phase),
		"reason":            s.Reason,
		"offsets":           string(snap),
		"restarts":          string(restarts),
		"last-activity-at":  formatTime(s.LastActivityAt),
		"suspended-at":      formatTime(s.SuspendedAt),
		"awake-since":       formatTime(s.AwakeSince),
		"resumed-at":        formatTime(s.ResumedAt),
		"generation":        strconv.FormatInt(s.Generation, 10),
		"pending":           strconv.FormatInt(s.Pending, 10),
		"source-down":       strconv.FormatBool(s.Outage.Down),
		"source-down-since": formatTime(s.Outage.Since),
		"suspend-checked":   strconv.FormatBool(s.Reported.SuspendChecked),
		"resume-stalled":    strconv.FormatBool(s.Reported.ResumeStalled),
		"suspend-stalled":   strconv.FormatBool(s.Reported.SuspendStalled),
		"sources-checked":   s.Reported.SourcesChecked,
	}
}

// FromData is the inverse of Data.
func FromData(d map[string]string) (State, bool) {
	phase, has := d["state"]
	if !has {
		return State{}, false
	}
	s := State{Phase: Phase(phase), Snapshot: map[string]string{}, Reason: d["reason"]}
	if v := d["offsets"]; v != "" {
		if err := json.Unmarshal([]byte(v), &s.Snapshot); err != nil {
			return State{}, false
		}
	}
	if v := d["restarts"]; v != "" {
		if err := json.Unmarshal([]byte(v), &s.Restarts); err != nil {
			return State{}, false
		}
	}
	// A timestamp that does not parse is memory we cannot trust. Zero time would read as "idle
	// for ages" and suspend on the spot; unknown never acts applies to our own memory too.
	var ok bool
	for _, f := range []struct {
		key string
		dst *time.Time
	}{{"last-activity-at", &s.LastActivityAt}, {"suspended-at", &s.SuspendedAt}, {"awake-since", &s.AwakeSince},
		{"resumed-at", &s.ResumedAt}, {"source-down-since", &s.Outage.Since}} {
		if *f.dst, ok = parseTime(d[f.key]); !ok {
			return State{}, false
		}
	}
	s.Generation, _ = strconv.ParseInt(d["generation"], 10, 64)
	s.Pending, _ = strconv.ParseInt(d["pending"], 10, 64)
	s.Outage.Down, _ = strconv.ParseBool(d["source-down"])
	s.Reported.SuspendChecked, _ = strconv.ParseBool(d["suspend-checked"])
	s.Reported.ResumeStalled, _ = strconv.ParseBool(d["resume-stalled"])
	s.Reported.SuspendStalled, _ = strconv.ParseBool(d["suspend-stalled"])
	s.Reported.SourcesChecked = d["sources-checked"]
	return s, true
}

// parseTime reads an RFC 3339 time; the empty string is the zero time and is fine.
func parseTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, true
	}
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
