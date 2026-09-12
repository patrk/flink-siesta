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
	UpgradeMode    string // "savepoint" | "last-state" | "stateless"; the controller never changes it
	Generation     int64  // metadata.generation; a change means someone edited the spec
	JobState       string // Flink JobStatus: RUNNING, FAILED, RESTARTING, FINISHED, ...
	LifecycleState string // operator: CREATED, SUSPENDED, UPGRADING, DEPLOYED, STABLE, ROLLING_BACK, ROLLED_BACK, FAILED
	ReconcileError string // status.reconciliationStatus.error
}

func (l Live) operatorBusy() bool {
	return l.LifecycleState == "UPGRADING" || l.LifecycleState == "ROLLING_BACK"
}

// stable: the operator has settled the deployment and the job runs. Only then is a suspend safe.
func (l Live) stable() bool { return l.LifecycleState == "STABLE" && l.JobState == "RUNNING" }

// suspended: the operator completed the suspend (savepoint taken, pods gone). Only then is a resume safe.
func (l Live) suspended() bool { return l.LifecycleState == "SUSPENDED" }

// keepsPosition: suspend and resume keep the job's position only with these upgrade modes.
// "stateless" would resume from scratch, which for a Kafka source means replaying the topic.
func (l Live) keepsPosition() bool {
	return l.UpgradeMode == "savepoint" || l.UpgradeMode == "last-state"
}

type Action int

const (
	None Action = iota
	Suspend
	Resume
	Restart
	MarkUnrecoverable
	Refuse // nothing patched, but the reason deserves a Warning event
)

func (a Action) String() string {
	return [...]string{"none", "suspend", "resume", "restart", "mark-unrecoverable", "refuse"}[a]
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

// Observation is what the probes reported this tick. Known=false and LagKnown=false both mean
// "could not ask", which is never treated as "nothing there".
type Observation struct {
	Snapshot map[string]string
	Known    bool
	Pending  int64 // records the consumer group has not consumed yet; meaningful only if LagKnown
	LagKnown bool
}

type Decider struct{ restart RestartPolicy }

func New(r RestartPolicy) *Decider { return &Decider{restart: r} }

func (d *Decider) Decide(p policy.Policy, prev state.State, live Live, obs Observation, now time.Time) Decision {
	if live.operatorBusy() {
		return Decision{None, prev, "operator busy: " + live.LifecycleState}
	}
	if p.Mode == policy.ModeOff && prev.Phase == state.Suspended {
		if !live.suspended() {
			return Decision{None, prev, "waiting for operator to finish suspending"}
		}
		return Decision{Resume, wake(prev, now, "suspension disabled"), "suspension disabled"}
	}
	// A spec edit (new generation) is a human saying "try again": it clears unrecoverable and
	// restarts the clocks, so the deployment gets a full idle window like after a resume.
	if prev.Phase == state.Unrecoverable && live.Generation > prev.Generation {
		prev = wake(prev, now, "spec changed")
		prev.Restarts = state.RestartBudget{}
	}
	prev.Generation = live.Generation

	if !obs.Known {
		return Decision{None, prev, "offsets unavailable"} // unknown never acts
	}

	moved := !maps.Equal(obs.Snapshot, prev.Snapshot)
	cur := prev
	cur.Snapshot = obs.Snapshot
	if moved {
		cur.LastActivityAt = now
	}

	switch prev.Phase {
	case state.Suspended:
		if !live.suspended() {
			// We patched suspended, the operator has not reported SUSPENDED yet: a savepoint may be
			// in flight. Writing "running" now would race it. Record activity, act next time.
			return Decision{None, cur, "waiting for operator to finish suspending"}
		}
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
			live.stable() && live.SpecJobState == "running" &&
			now.After(cur.LastActivityAt.Add(p.IdleAfter)) &&
			now.After(cur.AwakeSince.Add(p.MinAwake)) {
			if !live.keepsPosition() {
				cur.Reason = "suspend refused: upgradeMode " + live.UpgradeMode + " would lose the job's position"
				return Decision{Refuse, cur, cur.Reason}
			}
			if p.ConsumerGroup != "" {
				// Idle also means caught up: nothing pending for the job's consumer group.
				if !obs.LagKnown {
					return Decision{None, cur, "lag unknown for group " + p.ConsumerGroup}
				}
				if obs.Pending > 0 {
					return Decision{None, cur, fmt.Sprintf("%d records pending for group %s", obs.Pending, p.ConsumerGroup)}
				}
			}
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
