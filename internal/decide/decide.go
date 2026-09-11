package decide

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/patrk/flink-siesta/internal/policy"
	"github.com/patrk/flink-siesta/internal/state"
)

type Live struct {
	SpecJobState   string // "running" | "suspended"
	JobState       string // Flink JobStatus: RUNNING, FAILED, RESTARTING, FINISHED, ...
	LifecycleState string // operator: CREATED, SUSPENDED, UPGRADING, DEPLOYED, STABLE, ROLLING_BACK, ROLLED_BACK, FAILED
	ReconcileError string // status.reconciliationStatus.error
}

func (l Live) operatorBusy() bool {
	return l.LifecycleState == "UPGRADING" || l.LifecycleState == "ROLLING_BACK"
}

type Action int

const (
	None Action = iota
	Suspend
	Resume
	Restart
	MarkUnrecoverable
)

func (a Action) String() string {
	return [...]string{"none", "suspend", "resume", "restart", "mark-unrecoverable"}[a]
}

type Decision struct {
	Action Action
	Next   state.State
	Reason string
}

type RestartPolicy struct {
	MaxRestarts   int
	Window        time.Duration
	BaseBackoff   time.Duration
	Multiplier    float64
	FailingAfter  time.Duration
	Unrecoverable []string // substrings of the reconciliation error
}

type Decider struct{ restart RestartPolicy }

func New(r RestartPolicy) *Decider { return &Decider{restart: r} }

func (d *Decider) Decide(p policy.Policy, prev state.State, live Live, snapshot map[string]string, known bool, now time.Time) Decision {
	if live.operatorBusy() {
		return Decision{None, prev, "operator busy: " + live.LifecycleState}
	}
	if p.Mode == policy.ModeOff && prev.Phase == state.Suspended {
		return Decision{Resume, wake(prev, now, "suspension disabled"), "suspension disabled"}
	}
	if !known {
		return Decision{None, prev, "offsets unavailable"} // unknown never acts
	}

	moved := !maps.Equal(snapshot, prev.Snapshot)
	cur := prev
	cur.Snapshot = snapshot
	if moved {
		cur.LastActivityAt = now
	}

	switch prev.Phase {
	case state.Suspended:
		if moved {
			return Decision{Resume, wake(cur, now, "input observed"), "input observed"}
		}
		return Decision{None, cur, "idle"}

	case state.Unrecoverable:
		return Decision{None, cur, prev.Reason}

	default: // Active
		if r := d.unrecoverable(live); r != "" {
			cur.Phase, cur.Reason = state.Unrecoverable, r
			return Decision{MarkUnrecoverable, cur, r}
		}
		if p.Restart && d.failing(live, cur, now) {
			if cur.Restarts.Allows(now, d.restart.MaxRestarts, d.restart.Window) {
				cur.Restarts = cur.Restarts.Consume(now, d.restart.Window, d.restart.BaseBackoff, d.restart.Multiplier)
				cur.AwakeSince = now
				cur.Reason = fmt.Sprintf("restart %d", cur.Restarts.Count)
				return Decision{Restart, cur, fmt.Sprintf("job %s, restart %d", live.JobState, cur.Restarts.Count)}
			}
			cur.Phase, cur.Reason = state.Unrecoverable, "restart budget exhausted"
			return Decision{MarkUnrecoverable, cur, cur.Reason}
		}
		if p.Mode == policy.ModeAuto &&
			live.JobState == "RUNNING" && live.SpecJobState == "running" &&
			now.After(cur.LastActivityAt.Add(p.IdleAfter)) &&
			now.After(cur.AwakeSince.Add(p.MinAwake)) {
			cur.Phase, cur.SuspendedAt = state.Suspended, now
			cur.Reason = "no input for " + p.IdleAfter.String()
			return Decision{Suspend, cur, cur.Reason}
		}
		return Decision{None, cur, "active"}
	}
}

func wake(s state.State, now time.Time, reason string) state.State {
	s.Phase, s.SuspendedAt, s.AwakeSince, s.LastActivityAt, s.Reason = state.Active, time.Time{}, now, now, reason
	return s
}

func (d *Decider) failing(live Live, s state.State, now time.Time) bool {
	if live.JobState == "FAILED" || live.LifecycleState == "FAILED" {
		return true
	}
	return live.JobState == "RESTARTING" && now.After(s.AwakeSince.Add(d.restart.FailingAfter))
}

func (d *Decider) unrecoverable(live Live) string {
	if live.ReconcileError == "" {
		return ""
	}
	for _, sub := range d.restart.Unrecoverable {
		if sub = strings.TrimSpace(sub); sub != "" && strings.Contains(live.ReconcileError, sub) {
			return "unrecoverable: " + sub
		}
	}
	return ""
}
