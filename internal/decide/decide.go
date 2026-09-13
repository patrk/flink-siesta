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
	SavepointPath  string // status.jobStatus.upgradeSavepointPath; empty after a suspend means no savepoint was taken
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
	// ResumedAfter is non-zero on the first tick a resumed job reports RUNNING again: the time
	// from the input that woke it to the job running. Observed, never acted on.
	ResumedAfter time.Duration
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
		return Decision{Action: None, Next: prev, Reason: "operator busy: " + live.LifecycleState}
	}
	if p.Mode == policy.ModeOff && prev.Phase == state.Suspended {
		if !live.suspended() {
			return Decision{Action: None, Next: prev, Reason: "waiting for operator to finish suspending"}
		}
		return Decision{Action: Resume, Next: wake(prev, now, "suspension disabled"), Reason: "suspension disabled"}
	}
	// A spec edit (new generation) is a human saying "try again": it clears unrecoverable and
	// restarts the clocks, so the deployment gets a full idle window like after a resume.
	if prev.Phase == state.Unrecoverable && live.Generation > prev.Generation {
		prev = wake(prev, now, "spec changed")
		prev.Restarts = state.RestartBudget{}
	}
	prev.Generation = live.Generation

	if !obs.Known {
		return Decision{Action: None, Next: prev, Reason: "offsets unavailable"} // unknown never acts
	}

	moved := !maps.Equal(obs.Snapshot, prev.Snapshot)
	cur := prev
	cur.Snapshot = obs.Snapshot
	if moved {
		cur.LastActivityAt = now
	}

	switch prev.Phase {
	case state.Suspended:
		if live.SpecJobState == "running" {
			// Someone else set it running. Respect that: it is awake, with a full idle window.
			return Decision{Action: None, Next: wake(cur, now, "resumed outside siesta"), Reason: "resumed outside siesta"}
		}
		if !live.suspended() {
			// We patched suspended, the operator has not reported SUSPENDED yet: a savepoint may be
			// in flight. Writing "running" now would race it. Record activity, act next time.
			return Decision{Action: None, Next: cur, Reason: "waiting for operator to finish suspending"}
		}
		if moved {
			next := wake(cur, now, "input observed")
			next.ResumedAt = now
			return Decision{Action: Resume, Next: next, Reason: "input observed"}
		}
		return Decision{Action: None, Next: cur, Reason: "idle"}

	case state.Unrecoverable:
		reason := prev.Reason
		if reason == "" {
			reason = "unrecoverable; edit the spec to retry"
		}
		cur.Reason = reason
		return Decision{Action: None, Next: cur, Reason: reason}

	default: // Active
		if live.SpecJobState == "suspended" {
			// Suspended by someone else. Not ours to resume; say so and stay out of the way.
			cur.Reason = "suspended outside siesta"
			return Decision{Action: None, Next: cur, Reason: cur.Reason}
		}
		var resumedAfter time.Duration
		if !cur.ResumedAt.IsZero() && live.stable() {
			resumedAfter = now.Sub(cur.ResumedAt)
			cur.ResumedAt = time.Time{}
		}
		if r := d.unrecoverable(live); r != "" {
			cur.Phase, cur.Reason = state.Unrecoverable, r
			return Decision{Action: MarkUnrecoverable, Next: cur, Reason: r}
		}
		if p.Restart && d.failing(live, cur, now) {
			switch {
			case cur.Restarts.InBackoff(now, d.restart.Window):
				// The last restart is still taking effect, or its backoff has not elapsed. Checked
				// first: even a budget at its maximum gets to see whether its last restart worked.
				return Decision{Action: None, Next: cur, Reason: "restart backoff until " + cur.Restarts.NextAfter.UTC().Format(time.RFC3339)}
			case cur.Restarts.Exhausted(now, d.restart.MaxRestarts, d.restart.Window):
				cur.Phase, cur.Reason = state.Unrecoverable, "restart budget exhausted"
				return Decision{Action: MarkUnrecoverable, Next: cur, Reason: cur.Reason}
			default:
				cur.Restarts = cur.Restarts.Consume(now, d.restart.Window, d.restart.BaseBackoff, d.restart.Multiplier)
				cur.AwakeSince = now
				cur.Reason = fmt.Sprintf("restart %d", cur.Restarts.Count)
				return Decision{Action: Restart, Next: cur, Reason: fmt.Sprintf("job %s, restart %d", live.JobState, cur.Restarts.Count)}
			}
		}
		if p.Mode == policy.ModeAuto &&
			live.stable() && live.SpecJobState == "running" &&
			now.After(cur.LastActivityAt.Add(p.IdleAfter)) &&
			now.After(cur.AwakeSince.Add(p.MinAwake)) {
			if !live.keepsPosition() {
				cur.Reason = "suspend refused: upgradeMode " + live.UpgradeMode + " would lose the job's position"
				return Decision{Action: Refuse, Next: cur, Reason: cur.Reason}
			}
			if p.ConsumerGroup != "" {
				// Idle also means caught up: nothing pending for the job's consumer group.
				if !obs.LagKnown {
					return Decision{Action: None, Next: cur, Reason: "lag unknown for group " + p.ConsumerGroup}
				}
				if obs.Pending > 0 {
					// Coarse reason on the object; the number goes to the store.
					cur.Pending = obs.Pending
					return Decision{Action: None, Next: cur, Reason: "records pending for group " + p.ConsumerGroup}
				}
				cur.Pending = 0
			}
			cur.Phase, cur.SuspendedAt = state.Suspended, now
			cur.Reason = "no input for " + p.IdleAfter.String()
			return Decision{Action: Suspend, Next: cur, Reason: cur.Reason}
		}
		return Decision{Action: None, Next: cur, Reason: "active", ResumedAfter: resumedAfter}
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
