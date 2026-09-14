package decide

import (
	"testing"
	"time"

	"github.com/patrk/flink-siesta/internal/policy"
	"github.com/patrk/flink-siesta/internal/state"
)

// The operator's state space is small and finite. Rather than adding a guard per incident, this
// test walks every combination and checks the invariants that must hold everywhere, and the
// positive rows of the ADR 11 table. If a new operator version adds a lifecycle state, add it
// here and the decider must answer for it.
var (
	lifecycles   = []string{"", "CREATED", "DEPLOYED", "STABLE", "SUSPENDED", "UPGRADING", "ROLLING_BACK", "ROLLED_BACK", "FAILED"}
	jobStates    = []string{"", "CREATED", "RECONCILING", "RUNNING", "RESTARTING", "FAILED", "FINISHED", "CANCELED"}
	specStates   = []string{"running", "suspended"}
	phases       = []state.Phase{state.Active, state.Suspended, state.Unrecoverable}
	upgradeModes = []string{"savepoint", "last-state", "stateless"} // ADR 6
	gates        = []JobGate{JobUnknown, JobBusy, JobIdle}          // ADR 13
)

type point struct {
	lc, js, sp string
	ph         state.Phase
	obs        string
	um         string
	fromJob    bool
	gate       JobGate
}

func (p point) String() string {
	s := "lifecycle=" + p.lc + " job=" + p.js + " spec=" + p.sp + " phase=" + string(p.ph) + " obs=" + p.obs + " mode=" + p.um
	if p.fromJob {
		s += " idle:job gate=" + p.gate.String()
	}
	return s
}

func TestEveryOperatorStateHasADefinedAnswer(t *testing.T) {
	d := New(rp)
	now := t0.Add(30 * 24 * time.Hour) // long past every window, so time never blocks an action
	observations := map[string]Observation{
		"unknown":   unknown,
		"unchanged": seen(snap("100")),
		"moved":     seen(snap("101")),
	}
	checked := 0
	for _, lc := range lifecycles {
		for _, js := range jobStates {
			for _, sp := range specStates {
				for _, ph := range phases {
					for name, obs := range observations {
						for _, um := range upgradeModes {
							for _, fromJob := range []bool{false, true} {
								for _, gate := range gates {
									if !fromJob && gate != JobUnknown {
										continue // the gate is read only with the policy
									}
									p := point{lc, js, sp, ph, name, um, fromJob, gate}
									pol := auto
									pol.IdleFromJob = fromJob
									o := obs
									o.Ends = map[string][]int64{"t": {100}}
									o.ReadingNote = "note"
									switch gate {
									case JobIdle:
										o.ReadingKnown, o.Reading = true, JobReading{Offsets: map[string]map[int]int64{"t": {0: 99}}, IdleFor: time.Hour}
									case JobBusy:
										o.ReadingKnown, o.Reading = true, JobReading{Offsets: map[string]map[int]int64{"t": {0: 50}}}
									}
									prev := state.State{Phase: ph, Snapshot: snap("100"), LastActivityAt: t0, AwakeSince: t0, Generation: 1}
									if ph == state.Suspended {
										prev.SuspendedAt = t0
									}
									live := Live{SpecJobState: sp, UpgradeMode: um, JobState: js, LifecycleState: lc, Generation: 1}
									got := decideWithoutPanic(t, d, pol, prev, live, o, now, p)
									checked++
									check(t, p, got)
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("%d combinations checked", checked)
}

func decideWithoutPanic(t *testing.T, d *Decider, pol policy.Policy, prev state.State, live Live, obs Observation, now time.Time, p point) (got Decision) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic at %s: %v", p, r)
		}
	}()
	return d.Decide(pol, prev, live, obs, now)
}

func check(t *testing.T, p point, got Decision) {
	t.Helper()
	if got.Reason == "" {
		t.Fatalf("%s: every decision carries a reason", p)
	}
	busy := p.lc == "UPGRADING" || p.lc == "ROLLING_BACK"
	if busy && got.Action != None {
		t.Fatalf("%s: never act while the operator is mid-change, got %s", p, got.Action)
	}
	maySuspend := p.lc == "STABLE" && p.js == "RUNNING" && p.sp == "running" && p.ph == state.Active && p.obs != "unknown"
	mayResume := p.lc == "SUSPENDED" && p.sp == "suspended" && p.ph == state.Suspended && p.obs == "moved"
	mayRestart := p.js == "FAILED" || p.lc == "FAILED" || p.js == "RESTARTING"
	if got.Action == Suspend && !maySuspend {
		t.Fatalf("%s: suspend is only allowed from STABLE+RUNNING, active, observed", p)
	}
	if got.Action == Resume && !mayResume {
		t.Fatalf("%s: resume is only allowed from SUSPENDED after input", p)
	}
	if got.Action == Restart && !mayRestart {
		t.Fatalf("%s: restart is only allowed on a failing job", p)
	}
	if p.obs == "unknown" && got.Action != None {
		t.Fatalf("%s: unknown never acts, got %s", p, got.Action)
	}
	if p.ph == state.Unrecoverable && got.Action != None {
		t.Fatalf("%s: unrecoverable waits for a human, got %s", p, got.Action)
	}
	if got.Action == Suspend && p.um == "stateless" {
		t.Fatalf("%s: a stateless job is never suspended", p)
	}
	if got.Action == Suspend && p.fromJob && p.gate != JobIdle {
		t.Fatalf("%s: the job gate may object and was ignored", p)
	}

	// The positive rows: what must happen, not only what must not.
	keeps := p.um != "stateless"
	gateOK := !p.fromJob || p.gate == JobIdle
	idleRow := maySuspend && p.obs == "unchanged"
	switch {
	case idleRow && keeps && gateOK && got.Action != Suspend:
		t.Fatalf("%s: idle past the window with every gate open must suspend, got %s (%s)", p, got.Action, got.Reason)
	case idleRow && !keeps && got.Action != Refuse:
		t.Fatalf("%s: idle on %s must be refused, got %s", p, p.um, got.Action)
	case idleRow && keeps && !gateOK && (got.Action != None || (got.Held != "job-busy" && got.Held != "job-unknown")):
		t.Fatalf("%s: the job gate must hold with a named reason, got %s held=%q", p, got.Action, got.Held)
	case mayResume && got.Action != Resume:
		t.Fatalf("%s: input while suspended by us must resume, got %s", p, got.Action)
	case p.sp == "suspended" && p.ph == state.Active && !busy && p.obs != "unknown" && got.Reason != ReasonSuspendedOutside:
		t.Fatalf("%s: a suspend we did not make is reported as such, got %q", p, got.Reason)
	}
}

// mode off must never suspend, and must resume anything we suspended once the operator has
// finished suspending it, regardless of traffic.
func TestModeOffAcrossTheStateSpace(t *testing.T) {
	d := New(rp)
	off := withMode(auto, policy.ModeOff)
	now := t0.Add(30 * 24 * time.Hour)
	for _, lc := range lifecycles {
		for _, js := range jobStates {
			for _, sp := range specStates {
				prev := state.State{Phase: state.Active, Snapshot: snap("100"), LastActivityAt: t0, AwakeSince: t0}
				live := Live{SpecJobState: sp, UpgradeMode: "savepoint", JobState: js, LifecycleState: lc}
				if got := d.Decide(off, prev, live, seen(snap("100")), now); got.Action == Suspend {
					t.Fatalf("mode off must never suspend (lifecycle=%s job=%s spec=%s)", lc, js, sp)
				}
				got := d.Decide(off, suspendedState(), live, seen(snap("100")), now)
				wantResume := lc == "SUSPENDED" && sp == "suspended"
				if wantResume && got.Action != Resume {
					t.Fatalf("mode off must resume what we suspended (lifecycle=%s job=%s spec=%s), got %s", lc, js, sp, got.Action)
				}
				if !wantResume && got.Action != None {
					t.Fatalf("mode off may only resume a completed suspend (lifecycle=%s job=%s spec=%s), got %s", lc, js, sp, got.Action)
				}
			}
		}
	}
}
