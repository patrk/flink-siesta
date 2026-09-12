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

func (s State) Annotations(prefix string) map[string]string {
	snap, _ := json.Marshal(s.Snapshot)
	restarts, _ := json.Marshal(s.Restarts)
	return map[string]string{
		prefix + "/state":            string(s.Phase),
		prefix + "/offsets":          string(snap),
		prefix + "/restarts":         string(restarts),
		prefix + "/last-activity-at": formatTime(s.LastActivityAt),
		prefix + "/suspended-at":     formatTime(s.SuspendedAt),
		prefix + "/awake-since":      formatTime(s.AwakeSince),
		prefix + "/reason":           s.Reason,
		prefix + "/generation":       strconv.FormatInt(s.Generation, 10),
		prefix + "/resumed-at":       formatTime(s.ResumedAt),
	}
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
