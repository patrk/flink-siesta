// Package state is everything the controller remembers, persisted as annotations.
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
	SourceDown     bool      // the source could not be asked last tick; drives one event per edge
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
	s.LastActivityAt = parseTime(ann[prefix+"/last-activity-at"])
	s.SuspendedAt = parseTime(ann[prefix+"/suspended-at"])
	s.AwakeSince = parseTime(ann[prefix+"/awake-since"])
	s.Generation, _ = strconv.ParseInt(ann[prefix+"/generation"], 10, 64)
	s.ResumedAt = parseTime(ann[prefix+"/resumed-at"])
	return s, true
}

// Annotations is what goes on the FlinkDeployment itself: only what a human wants to see in
// kubectl describe. Everything else lives in the ConfigMap, see Data. Keys that earlier
// versions wrote on the object are set to "" so a merge patch removes them.
func (s State) Annotations(prefix string) map[string]string {
	return map[string]string{
		prefix + "/state":   string(s.Phase),
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
		"state":            string(s.Phase),
		"reason":           s.Reason,
		"offsets":          string(snap),
		"restarts":         string(restarts),
		"last-activity-at": formatTime(s.LastActivityAt),
		"suspended-at":     formatTime(s.SuspendedAt),
		"awake-since":      formatTime(s.AwakeSince),
		"resumed-at":       formatTime(s.ResumedAt),
		"generation":       strconv.FormatInt(s.Generation, 10),
		"pending":          strconv.FormatInt(s.Pending, 10),
		"source-down":      strconv.FormatBool(s.SourceDown),
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
	s.LastActivityAt = parseTime(d["last-activity-at"])
	s.SuspendedAt = parseTime(d["suspended-at"])
	s.AwakeSince = parseTime(d["awake-since"])
	s.ResumedAt = parseTime(d["resumed-at"])
	s.Generation, _ = strconv.ParseInt(d["generation"], 10, 64)
	s.Pending, _ = strconv.ParseInt(d["pending"], 10, 64)
	s.SourceDown, _ = strconv.ParseBool(d["source-down"])
	return s, true
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
