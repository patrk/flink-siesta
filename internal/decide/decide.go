package decide

import (
	"maps"
	"strings"
	"time"

	"github.com/patrk/flink-siesta/internal/policy"
	"github.com/patrk/flink-siesta/internal/state"
)

type Live struct {
	SpecJobState  string // "running" | "suspended"
	UpgradeMode   string // "savepoint" | "last-state" | "stateless"; the controller never changes it
	SavepointPath string // status.jobStatus.upgradeSavepointPath; empty after a suspend means no savepoint was taken
	// OperatorRestarts: the deployment enables the operator's own health-check restart. Two
	// restart mechanisms would fight, so ours steps back and says so.
	OperatorRestarts bool
	Generation       int64  // metadata.generation; a change means someone edited the spec
	RestartNonce     int64  // spec.restartNonce; newer than memory means a restart memory did not record
	JobID            string // status.jobStatus.jobId; changes on every start, so it keys "once per job instance"
	JobState         string // Flink JobStatus: RUNNING, FAILED, RESTARTING, FINISHED, ...
	LifecycleState   string // operator: CREATED, SUSPENDED, UPGRADING, DEPLOYED, STABLE, ROLLING_BACK, ROLLED_BACK, FAILED
	ReconcileError   string // status.reconciliationStatus.error
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
	// Held names the gate that keeps an idle job awake this tick, empty when none does:
	// min-awake, lag-unknown, lag-pending, job-unknown, job-busy. Observed, never acted on.
	Held string
	// Gate and GateNote are the job gate's answer when it was consulted, for the log line.
	Gate     JobGate
	GateNote string
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
	// Ends holds the broker's end offset per partition for each source, the typed side of the
	// snapshot, for the job gate. A missing partition is -1.
	Ends map[string][]int64
	// Reading is the running job's own view, used only with idle: job (ADR 13). ReadingKnown is
	// false when the job could not be asked, and ReadingNote says why.
	Reading      JobReading
	ReadingKnown bool
	ReadingNote  string
	// Why says why the snapshot is unknown when the observer knows better than "unavailable",
	// for example sources: auto before the job has run once.
	Why string
}

// JobReading is what the running job's REST API reports about its Kafka sources.
type JobReading struct {
	// Offsets maps topic -> partition -> the last offset the reader emitted, InitialOffset before
	// the first record. That is the position a savepoint would record.
	Offsets map[string]map[int]int64
	// IdleFor is the smallest sourceIdleTime over the source vertices: 0 while any source emits,
	// counting since the last emitted record otherwise, including for a source that never had one.
	IdleFor time.Duration
}

// InitialOffset is the currentOffset gauge's value before the reader has emitted a record.
const InitialOffset = -1

// JobGate is the job's answer to "may this job sleep": ADR 13.
type JobGate int

const (
	JobUnknown JobGate = iota // not asked, not reachable, or no answer for a declared partition
	JobBusy                   // records still to emit, or a record emitted within the last poll interval
	JobIdle                   // every declared partition emitted up to the end, and quiet for a poll interval
)

func (g JobGate) String() string {
	switch g {
	case JobBusy:
		return "busy"
	case JobIdle:
		return "idle"
	default:
		return "unknown"
	}
}

type Decider struct {
	restart RestartPolicy
	// Quiet is how long a source must have emitted nothing before the job gate calls it idle:
	// one poll interval, so that a job working through a fetched backlog still objects.
	Quiet time.Duration
}

func New(r RestartPolicy) *Decider { return &Decider{restart: r} }

func (d *Decider) quiet() time.Duration {
	if d.Quiet > 0 {
		return d.Quiet
	}
	return time.Minute
}

func (d *Decider) Decide(p policy.Policy, prev state.State, live Live, obs Observation, now time.Time) Decision {
	if live.operatorBusy() {
		return Decision{Action: None, Next: prev, Reason: reasonOperatorBusy(live.LifecycleState)}
	}
	// Mode off resumes what we suspended, once the operator has finished suspending it. A spec
	// that already says running was resumed by someone else and is handled as such below.
	if p.Mode == policy.ModeOff && prev.Phase == state.Suspended && live.SpecJobState == "suspended" {
		if !live.suspended() {
			return Decision{Action: None, Next: prev, Reason: ReasonOperatorSuspends}
		}
		return Decision{Action: Resume, Next: wake(prev, now, ReasonSuspensionOff), Reason: ReasonSuspensionOff}
	}
	// A spec edit (new generation) is a human saying "try again": it clears unrecoverable and
	// restarts the clocks, so the deployment gets a full idle window like after a resume.
	if prev.Phase == state.Unrecoverable && live.Generation > prev.Generation {
		prev = wake(prev, now, "spec changed")
		prev.Restarts = state.RestartBudget{}
	}
	prev.Generation = live.Generation

	if !obs.Known {
		if obs.Why != "" {
			// A named reason is a lasting situation, not a transient failure: it goes on the object.
			next := prev
			next.Reason = obs.Why
			return Decision{Action: None, Next: next, Reason: obs.Why}
		}
		return Decision{Action: None, Next: prev, Reason: ReasonOffsetsUnknown} // unknown never acts
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
			return Decision{Action: None, Next: wake(cur, now, ReasonResumedOutside), Reason: ReasonResumedOutside}
		}
		if !live.suspended() {
			// We patched suspended, the operator has not reported SUSPENDED yet: a savepoint may be
			// in flight. Writing "running" now would race it. Record activity, act next time.
			return Decision{Action: None, Next: cur, Reason: ReasonOperatorSuspends}
		}
		if moved {
			next := wake(cur, now, "input observed")
			next.ResumedAt = now
			return Decision{Action: Resume, Next: next, Reason: ReasonInputObserved}
		}
		return Decision{Action: None, Next: cur, Reason: ReasonIdle}

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
			cur.Reason = ReasonSuspendedOutside
			return Decision{Action: None, Next: cur, Reason: cur.Reason}
		}
		var resumedAfter time.Duration
		if !cur.ResumedAt.IsZero() && live.stable() {
			resumedAfter = now.Sub(cur.ResumedAt)
			cur.ResumedAt = time.Time{}
		}
		if live.RestartNonce > cur.Restarts.LastNonce {
			// A restart memory never recorded: ours, lost between the patch and the save, or a
			// human's. Either way it consumed a slot, and the budget is only a limit if it counts.
			cur.Restarts = cur.Restarts.Consume(now, d.restart.Window, d.restart.BaseBackoff, d.restart.Multiplier)
			cur.Restarts.LastNonce = live.RestartNonce
			cur.AwakeSince = now
		}
		if r := d.unrecoverable(live); r != "" {
			cur.Phase, cur.Reason = state.Unrecoverable, r
			return Decision{Action: MarkUnrecoverable, Next: cur, Reason: r}
		}
		if p.Restart && live.OperatorRestarts && d.failing(live, cur, now) {
			return Decision{Action: None, Next: cur, Reason: ReasonOperatorRestarts}
		}
		if p.Restart && d.failing(live, cur, now) {
			switch {
			case cur.Restarts.InBackoff(now, d.restart.Window):
				// The last restart is still taking effect, or its backoff has not elapsed. Checked
				// first: even a budget at its maximum gets to see whether its last restart worked.
				return Decision{Action: None, Next: cur, Reason: reasonBackoffUntil(cur.Restarts.NextAfter)}
			case cur.Restarts.Exhausted(now, d.restart.MaxRestarts, d.restart.Window):
				cur.Phase, cur.Reason = state.Unrecoverable, ReasonBudgetExhausted
				return Decision{Action: MarkUnrecoverable, Next: cur, Reason: cur.Reason}
			default:
				cur.Restarts = cur.Restarts.Consume(now, d.restart.Window, d.restart.BaseBackoff, d.restart.Multiplier)
				cur.Restarts.LastNonce = now.UnixMilli() // the nonce the patch will carry
				cur.AwakeSince = now
				cur.Reason = reasonRestart(cur.Restarts.Count)
				return Decision{Action: Restart, Next: cur, Reason: reasonRestarting(live.JobState, cur.Restarts.Count)}
			}
		}
		if p.Mode == policy.ModeAuto &&
			live.stable() && live.SpecJobState == "running" &&
			now.After(cur.LastActivityAt.Add(p.IdleAfter)) {
			if !now.After(cur.AwakeSince.Add(p.MinAwake)) {
				return Decision{Action: None, Next: cur, Reason: ReasonActive, Held: "min-awake", ResumedAfter: resumedAfter}
			}
			if !live.keepsPosition() {
				cur.Reason = reasonRefused(live.UpgradeMode)
				return Decision{Action: Refuse, Next: cur, Reason: cur.Reason}
			}
			if p.ConsumerGroup != "" {
				// Idle also means caught up: nothing pending for the job's consumer group.
				// These reasons are written to the object, unlike the other waits: an idle job
				// that stays awake is a question someone will ask, and this is the answer.
				if !obs.LagKnown {
					cur.Reason = reasonLagUnknown(p.ConsumerGroup)
					return Decision{Action: None, Next: cur, Reason: cur.Reason, Held: "lag-unknown"}
				}
				if obs.Pending > 0 {
					// Coarse reason on the object; the number goes to the store.
					cur.Pending, cur.Reason = obs.Pending, reasonRecordsPending(p.ConsumerGroup)
					return Decision{Action: None, Next: cur, Reason: cur.Reason, Held: "lag-pending"}
				}
				cur.Pending = 0
			}
			if p.IdleFromJob {
				// The job's own view is the last gate (ADR 13): it may only object.
				gate, note := JobUnknown, obs.ReadingNote
				if obs.ReadingKnown {
					gate, note = jobGate(p.Sources, obs.Ends, obs.Reading, d.quiet())
				}
				switch gate {
				case JobIdle:
				case JobBusy:
					cur.Reason = reasonJobBusy(note)
					return Decision{Action: None, Next: cur, Reason: cur.Reason, Held: "job-busy", Gate: gate, GateNote: note}
				default:
					cur.Reason = reasonJobUnknown(note)
					return Decision{Action: None, Next: cur, Reason: cur.Reason, Held: "job-unknown", Gate: gate, GateNote: note}
				}
			}
			cur.Phase, cur.SuspendedAt = state.Suspended, now
			cur.Reason = reasonNoInput(p.IdleAfter)
			return Decision{Action: Suspend, Next: cur, Reason: cur.Reason}
		}
		return Decision{Action: None, Next: cur, Reason: ReasonActive, ResumedAfter: resumedAfter}
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
